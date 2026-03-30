//! # Runtime Module
//!
//! This module provides the core WebAssembly (Wasm) runtime capabilities for the operator.
//! It manages the Wasmtime engine and orchestrates the execution of individual Wasm components,
//! ensuring they can interact with the Kubernetes API and other host functionalities.

use crate::runtime::watcher::watcher;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use kube::runtime::watcher::{self, Event};
use kube::Resource;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use wasmtime::{Engine, Store};

use crate::config::metadata::{EnvironmentVariable, WasmComponentMetadata};
use crate::host::api::bindings;
use crate::host::state::State;
use crate::kubernetes::crd::WasmOperator;
use crate::kubernetes::KubernetesService;

use self::instance::WasmInstance;

pub mod instance;

// A unique identifier for each operator, e.g., from its Custom Resource.
type OperatorId = String;

enum OperatorState {
    Loaded {
        operator: bindings::KubeOperator,
        store: Mutex<Store<State>>,
        last_active: Instant,
        metadata: WasmComponentMetadata,
    },
    Unloaded {
        // Path to the serialized memory file.
        state_path: PathBuf,
        // Path to the original .wasm component file.
        metadata: WasmComponentMetadata,
    },
}

/// A service that manages the wasmtime engine and the execution of Wasm components.
pub struct WasmRuntime {
    engine: Engine,
    kubernetes_service: Arc<KubernetesService>,
    operators: DashMap<OperatorId, OperatorState>,
    watchers: DashMap<OperatorId, Vec<CancellationToken>>,
}

// TODO: change back to 5 minutes in production, set to 5 seconds for testing purposes
const IDLE_THRESHOLD: Duration = Duration::from_secs(5); // 5 minutes

impl WasmRuntime {
    /// Creates a new `WasmRuntime`.
    pub fn new(kubernetes_service: Arc<KubernetesService>) -> Result<Self> {
        let mut config = wasmtime::Config::new();
        config.async_support(true);
        config.cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize);
        let engine = Engine::new(&config)?;

        Ok(Self {
            engine,
            kubernetes_service,
            operators: DashMap::new(),
            watchers: DashMap::new(),
        })
    }

    /// Start the main runtime to pull WasmOperator CRs and start/stop them accordingly.
    pub async fn start(self: Arc<Self>) -> Result<()> {
        let k8s_service = self.kubernetes_service.clone();

        // Get the API resource for WasmOperator CRD to be able to watch it for changes.
        let kind = WasmOperator::kind(&());
        let api_resource = match k8s_service.find_api_resource(&kind) {
            Ok((ar, _)) => ar,
            Err(e) => {
                // TODO: handle this error better
                error!("Failed to find API resource for kind '{}': {}", kind, e);
                return Err(anyhow::anyhow!(
                    "Failed to find API resource for kind '{}': {}",
                    kind,
                    e
                ));
            }
        };

        // Start a background task to periodically check for idle operators and unload them.
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.idle_check_loop().await;
        });

        // Get the K8S watcher stream for the WasmOperator CRD.
        let namespace = std::env::var("WASMOP_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let mut wasmop_watcher = watcher(
            k8s_service.dynamic_api(api_resource, &namespace),
            Default::default(),
        )
        .boxed();

        // Watch for changes to WasmOperator CRs
        loop {
            match wasmop_watcher.next().await {
                Some(Ok(event)) => {
                    match event {
                        Event::Applied(obj) => {
                            // TODO: handle the result if loading is unsuccessful
                            if self.is_operator_outdated(&obj)
                                || !self.is_operator_initialized(&obj)
                            {
                                self.clone().initiate_operator(&obj).await?;
                            }
                        }
                        Event::Deleted(obj) => {
                            self.remove_operator(&obj).await?;
                        }
                        Event::Restarted(objects) => {
                            // TODO: check if not already loaded to avoid reloading all operators on every restart
                            // TODO: handle the result if loading is unsuccessful
                            for obj in objects {
                                if self.is_operator_outdated(&obj)
                                    || !self.is_operator_initialized(&obj)
                                {
                                    self.clone().initiate_operator(&obj).await?;
                                }
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    warn!(
                        "Watcher for '{}' in namespace '{}' encountered an error: {}",
                        &kind, namespace, e
                    );
                }
                None => {
                    // Stream ended, might want to restart the watch.
                    info!(
                        "Watcher for '{}' in namespace '{}' stream ended.",
                        &kind, namespace,
                    );
                    break;
                }
            }
        }
        Ok(())
    }

    fn is_operator_outdated(&self, k8s_object: &kube::api::DynamicObject) -> bool {
        //let name = k8s_object.metadata.name.clone().unwrap_or_default();
        let generation = k8s_object.metadata.generation.unwrap_or_default();
        let observed_generation = k8s_object
            .data
            .pointer("/status/observedGeneration")
            .and_then(|v| v.as_i64())
            .unwrap_or_default();
        generation > observed_generation
    }

    fn is_operator_initialized(&self, k8s_object: &kube::api::DynamicObject) -> bool {
        self.operators
            .contains_key(&k8s_object.metadata.name.clone().unwrap_or_default())
    }

    /// Initiates a Wasm operator from a Kubernetes object, instantiates it, and starts watching for its requested resources.
    async fn initiate_operator(
        self: Arc<Self>,
        k8s_object: &kube::api::DynamicObject,
    ) -> Result<()> {
        // Extract metadata from the CRD instance to create WasmComponentMetadata
        let op_name: String = k8s_object.metadata.name.clone().unwrap_or_default();

        let wasm_str: &str = k8s_object
            .data
            .pointer("/spec/wasm")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        let args: Vec<String> = k8s_object
            .data
            .pointer("/spec/args")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let env: Vec<EnvironmentVariable> = k8s_object
            .data
            .pointer("/spec/env")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|env_entry| {
                        let name = env_entry.get("name")?.as_str()?;
                        let value = env_entry.get("value")?.as_str()?;
                        Some(EnvironmentVariable {
                            name: name.to_string(),
                            value: value.to_string(),
                        })
                    })
                    .collect::<Vec<EnvironmentVariable>>()
            })
            .unwrap_or_default();

        let operator_meta = WasmComponentMetadata {
            name: op_name.clone(),
            wasm: PathBuf::from(wasm_str),
            env: env,
            args: args,
        };

        // Load the operator from the Wasm file and instantiate it, save it to the operator map
        self.load_operator(operator_meta).await?;

        // Update the observed generation in the status to match the current generation after successful load
        let generation = k8s_object.metadata.generation.unwrap_or_default();
        self.kubernetes_service
            .patch_operator_observed_generation(&op_name, generation)
            .await?;

        // Get the watch requests for the operator
        let watch_requests = self
            .with_operator(&op_name, |operator, store| {
                Box::pin(async move { operator.call_get_watch_requests(store).await })
            })
            .await?;

        // Create a watcher for each requested watch and do this in a async task
        let mut existing_watchers = self
            .watchers
            .remove(&op_name)
            .map(|pair| pair.1)
            .unwrap_or_default();
        for request in watch_requests {
            info!(
                "Operator '{}' requested watch for kind '{}' in namespace '{}'",
                op_name.clone(),
                request.kind,
                request.namespace
            );
            let op_id = op_name.clone();
            let runtime = self.clone();
            let cancel_token = CancellationToken::new();
            let task_token = cancel_token.clone();
            tokio::task::spawn_local(async move {
                runtime
                    .watch_and_reconcile(op_id, request, task_token)
                    .await;
            });
            existing_watchers.push(cancel_token);
        }
        self.watchers.insert(op_name.clone(), existing_watchers);

        Ok(())
    }

    async fn load_operator(&self, op_metadata: WasmComponentMetadata) -> Result<()> {
        let instance = WasmInstance::new(
            self.engine.clone(),
            self.kubernetes_service.clone(),
            op_metadata.clone(),
        );
        let op_id = op_metadata.name.clone();

        let (op, store) = instance.load().await?;

        // Update the Kubernetes resource to mark the operator as loaded
        self.kubernetes_service
            .patch_operator_status(&op_id, true)
            .await?;

        let state = OperatorState::Loaded {
            operator: op,
            store: Mutex::new(store),
            last_active: Instant::now(),
            metadata: op_metadata,
        };

        self.operators.insert(op_id.clone(), state);

        Ok(())
    }

    async fn reload_operator(&self, id: &str) -> Result<()> {
        let operator = self.operators.remove_if(id, |_, state| {
            matches!(state, OperatorState::Unloaded { .. })
        });

        let (_, mut op_state) = match operator {
            Some(pair) => pair,
            None => {
                // Operator is either already loaded, skipping.
                return Ok(());
            }
        };

        info!("Reloading operator {} from disk...", id);
        let (state_path, metadata) = match &op_state {
            OperatorState::Unloaded {
                state_path,
                metadata,
            } => (state_path.clone(), metadata.clone()),
            _ => {
                // This case should not be reached with the current enum definition.
                // We add a panic to make the compiler happy that the variables are always initialized.
                panic!("Unkown operator state for operator {}", id);
            }
        };

        // 1. Load the original component and instantiate it.
        let wasm_instance = WasmInstance::new(
            self.engine.clone(),
            self.kubernetes_service.clone(),
            metadata.clone(),
        );
        let (operator, mut store) = wasm_instance.load().await?;

        // 2. Read the saved state from disk asynchronously.
        let saved_state = tokio::fs::read(&state_path).await?;

        // 3. Ask the new component instance to deserialize the state.
        operator.call_deserialize(&mut store, &saved_state).await?;
        info!("Successfully restored memory state for operator {}", id);

        // 4. Update the state to Loaded.
        op_state = OperatorState::Loaded {
            operator,
            store: Mutex::new(store),
            last_active: Instant::now(),
            metadata,
        };

        // Update the Kubernetes resource to mark the operator as loaded
        self.kubernetes_service
            .patch_operator_status(&id, true)
            .await?;

        self.operators.insert(id.to_string(), op_state);
        Ok(())
    }

    async fn unload_operator(&self, id: &OperatorId) -> Result<()> {
        // Use remove-modify-insert pattern to avoid holding DashMap lock across .await
        if let Some((_id, mut op_state)) = self.operators.remove(id) {
            if let OperatorState::Loaded {
                operator,
                store,
                metadata,
                ..
            } = &mut op_state
            {
                let mut store_guard = store.lock().await;

                // 1. Ask the component to serialize its own state.
                let memory_data = operator.call_serialize(&mut *store_guard).await?;
                info!(
                    "Serializing {} bytes of memory for operator {}",
                    memory_data.len(),
                    id
                );

                // 3. Write memory to a file asynchronously.
                let state_path = PathBuf::from(format!("/tmp/wasm-state/{}.mem", id));
                if let Some(parent) = state_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(&state_path, &memory_data).await?;

                // 4. Create the new Unloaded state.
                let unloaded_state = OperatorState::Unloaded {
                    state_path: state_path.clone(),
                    metadata: metadata.clone(),
                };
                // 5. Insert the new state back into the map.
                self.operators.insert(id.clone(), unloaded_state);
                info!(
                    "Successfully unloaded operator {} to disk at {:?}",
                    id, &state_path
                );

                // Update the Kubernetes resource to mark the operator as loaded
                self.kubernetes_service
                    .patch_operator_status(&id, false)
                    .await?;
            } else {
                // It was already unloaded or in another state, just put it back.
                self.operators.insert(id.clone(), op_state);
            }
        }
        Ok(())
    }

    async fn remove_operator(&self, k8s_object: &kube::api::DynamicObject) -> Result<()> {
        let name = k8s_object.metadata.name.clone().unwrap_or_default();
        let watchers = self
            .watchers
            .remove(&name)
            .map(|pair| pair.1)
            .unwrap_or_default();
        for cancel_token in watchers {
            // TODO: add wait for the task to finish after cancellation
            cancel_token.cancel();
        }
        self.operators.remove(&name);
        info!("Operator {} removed from runtime.", name);
        Ok(())
    }

    async fn watch_and_reconcile(
        self: Arc<Self>,
        operator_id: String,
        request: bindings::local::operator::types::WatchRequest,
        cancel_token: CancellationToken,
    ) {
        let client = self.kubernetes_service.clone();
        let (ar, _) = match client.find_api_resource(&request.kind) {
            Ok(ar) => ar,
            Err(e) => {
                error!(
                    "Failed to find API resource for kind '{}': {}",
                    request.kind, e
                );
                return;
            }
        };

        let mut watcher = watcher(
            client.dynamic_api(ar, &request.namespace),
            Default::default(),
        )
        .boxed();

        info!(
            "Watcher started for kind '{}' in namespace '{}'",
            request.kind, request.namespace
        );

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!("Watcher for kind '{}' in namespace '{}' received cancellation signal. Stopping watcher.", request.kind, request.namespace);
                    break;
                }

                event_option = watcher.next() => {
                    match event_option {
                        Some(Ok(event)) => match event {
                            Event::Applied(obj) => {
                                self.dispatch_reconcile(
                                    &operator_id,
                                    bindings::local::operator::types::EventType::Added,
                                    &obj,
                                )
                                .await;
                            }
                            Event::Deleted(obj) => {
                                self.dispatch_reconcile(
                                    &operator_id,
                                    bindings::local::operator::types::EventType::Deleted,
                                    &obj,
                                )
                                .await;
                            }
                            Event::Restarted(objects) => {
                                for obj in objects {
                                    self.dispatch_reconcile(
                                        &operator_id,
                                        bindings::local::operator::types::EventType::Added,
                                        &obj,
                                    )
                                    .await;
                                }
                            }
                        },
                        Some(Err(e)) => {
                            warn!(
                                "Watcher for kind '{}' in namespace '{}' encountered an error: {}",
                                request.kind, request.namespace, e
                            );
                        }
                        None => {
                            // Stream ended, might want to restart the watch.
                            info!(
                                "Watcher for kind '{}' in namespace '{}' stream ended.",
                                request.kind, request.namespace
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn dispatch_reconcile(
        &self,
        operator_id: &str,
        event_type: bindings::local::operator::types::EventType,
        object: &kube::api::DynamicObject,
    ) {
        let name = object.metadata.name.clone().unwrap_or_default();
        let namespace = object.metadata.namespace.clone().unwrap_or_default();
        let resource_json = match serde_json::to_string(object) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize resource to JSON: {}", e);
                return;
            }
        };

        let reconcile_request = bindings::local::operator::types::ReconcileRequest {
            event_type,
            name,
            namespace,
            resource_json,
        };

        if let Err(e) = self
            .with_operator(operator_id, |operator, store| {
                Box::pin(async move { operator.call_reconcile(store, &reconcile_request).await })
            })
            .await
        {
            error!(
                "Reconciliation for operator '{}' failed: {}",
                operator_id, e
            );
        }
    }

    async fn idle_check_loop(&self) {
        loop {
            tokio::time::sleep(IDLE_THRESHOLD / 2).await;

            // Collect IDs of idle operators to avoid holding the map lock while unloading.
            let idle_ids: Vec<OperatorId> = self
                .operators
                .iter()
                .filter_map(|entry| {
                    if let OperatorState::Loaded { last_active, .. } = entry.value() {
                        if last_active.elapsed() > IDLE_THRESHOLD {
                            Some(entry.key().clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            for id in idle_ids {
                info!("Operator {} is idle. Unloading...", &id);
                if let Err(e) = self.unload_operator(&id).await {
                    tracing::error!("Failed to unload component {}: {}", id, e);
                }
            }
        }
    }

    async fn with_operator<F, T>(&self, id: &str, f: F) -> Result<T>
    where
        for<'a> F: FnOnce(
            &'a bindings::KubeOperator,
            &'a mut Store<State>,
        ) -> Pin<Box<dyn Future<Output = Result<T>> + 'a>>,
    {
        // Reload the operator if it's currently unloaded.
        self.reload_operator(id).await?;

        // Use remove-modify-insert pattern to avoid holding DashMap lock across .await
        let mut op_state: OperatorState = self.operators.remove(id).unwrap().1;

        let result = match &mut op_state {
            OperatorState::Loaded {
                operator,
                store,
                last_active,
                ..
            } => {
                *last_active = Instant::now();
                let mut store_guard = store.lock().await;

                // Call the provided function.
                f(operator, &mut store_guard).await
            }
            _ => Err(anyhow::anyhow!("Operator in invalid state")),
        };

        // Insert the (potentially updated) state back into the map.
        self.operators.insert(id.to_string(), op_state);

        result
    }
}
