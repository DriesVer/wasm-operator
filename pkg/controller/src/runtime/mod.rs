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
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use wasmtime::{Engine, Store};

use crate::config::metadata::WasmComponentMetadata;
use crate::host::api::bindings;
use crate::host::state::State;
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
}

// TODO: change back to 5 minutes in production, set to 3 seconds for testing purposes
const IDLE_THRESHOLD: Duration = Duration::from_secs(3); // 5 minutes

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
        })
    }

    /// Runs all the Wasm components specified in the metadata.
    pub async fn run_components(
        self: Arc<Self>,
        components_metadata: Vec<WasmComponentMetadata>,
    ) -> Result<()> {
        // Stagger the initialization of each component to avoid a thundering herd of requests
        // to the Kubernetes API server.
        let stagger_delay = Duration::from_millis(125);

        for metadata in components_metadata {
            tokio::time::sleep(stagger_delay).await;

            let operator_id = metadata.name.clone();

            let instance = WasmInstance::new(
                self.engine.clone(),
                self.kubernetes_service.clone(),
                metadata.clone(),
            );

            let (operator, store) = instance.load().await?;
            let op_state = OperatorState::Loaded {
                operator,
                store: Mutex::new(store),
                last_active: Instant::now(),
                metadata,
            };
            self.operators.insert(operator_id.clone(), op_state);

            // Get the watch requests from the component
            let watch_requests = self
                .with_operator(&operator_id, |operator, store| {
                    Box::pin(async move { operator.call_get_watch_requests(store).await })
                })
                .await?;

            for request in watch_requests {
                info!(
                    "Operator '{}' requested watch for kind '{}' in namespace '{}'",
                    operator_id, request.kind, request.namespace
                );

                let self_clone = self.clone();
                let operator_id_clone = operator_id.clone();
                tokio::task::spawn_local(async move {
                    self_clone
                        .watch_and_reconcile(operator_id_clone, request)
                        .await;
                });
            }
        }

        let runtime = Arc::clone(&self);
        tokio::spawn(async move {
            runtime.idle_check_loop().await;
        });

        // The main event loop to keep the operator alive.
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    async fn watch_and_reconcile(
        self: Arc<Self>,
        operator_id: String,
        request: bindings::local::operator::types::WatchRequest,
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

        info!("Watcher started for kind '{}' in namespace '{}'", request.kind, request.namespace);

        loop {
            match watcher.next().await {
                Some(Ok(event)) => {
                    match event {
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
                    }
                }
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
                if let Err(e) = self.unload_component(&id).await {
                    tracing::error!("Failed to unload component {}: {}", id, e);
                }
            }
        }
    }

    async fn unload_component(&self, id: &OperatorId) -> Result<()> {
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
            } else {
                // It was already unloaded or in another state, just put it back.
                self.operators.insert(id.clone(), op_state);
            }
        }
        Ok(())
    }

    async fn with_operator<F, T>(&self, id: &str, f: F) -> Result<T>
    where
        for<'a> F: FnOnce(
            &'a bindings::KubeOperator,
            &'a mut Store<State>,
        ) -> Pin<Box<dyn Future<Output = Result<T>> + 'a>>,
    {
        // Use remove-modify-insert pattern to avoid holding DashMap lock across .await
        let mut op_state = self.operators.remove(id).unwrap().1;

        let result: Result<T>;

        if let OperatorState::Unloaded {
            state_path,
            metadata,
        } = op_state
        {
            info!("Reloading operator {} from disk...", id);

            // 1. Load the original component and instantiate it.
            let wasm_instance = WasmInstance::new(
                self.engine.clone(),
                self.kubernetes_service.clone(),
                metadata.clone(),
            );
            let (operator, mut store) = wasm_instance.load().await?;

            // 2. Read the saved state from disk asynchronously.
            info!("Reading saved state from {:?}", &state_path);
            let saved_state = tokio::fs::read(&state_path).await?;
            info!(
                "Read {} bytes of saved state for operator {}",
                saved_state.len(),
                id
            );

            // 3. Ask the new component instance to deserialize the state.
            operator.call_deserialize(&mut store, &saved_state).await?;
            info!("Successfully restored memory state for operator {}", id);

            // 5. Call the closure with the new operator and store.
            result = f(&operator, &mut store).await;

            // 6. Update the state to Loaded.
            op_state = OperatorState::Loaded {
                operator,
                store: Mutex::new(store),
                last_active: Instant::now(),
                metadata,
            };
        } else if let OperatorState::Loaded {
            operator,
            store,
            last_active,
            ..
        } = &mut op_state
        {
            *last_active = Instant::now();
            let mut store_guard = store.lock().await;
            result = f(operator, &mut store_guard).await;
        } else {
            // This case should not be reached with the current enum definition.
            // We add a panic to make the compiler happy that `result` is always initialized.
            panic!("Unexpected operator state");
        }

        // Insert the (potentially updated) state back into the map.
        self.operators.insert(id.to_string(), op_state);

        result
    }
}
