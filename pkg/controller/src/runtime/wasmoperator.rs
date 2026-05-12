use crate::runtime::watcher::watcher;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use futures::StreamExt;
use kube::runtime::watcher::Event;
use kube::{Resource, ResourceExt};
use serde_json;
use target_lexicon::Triple;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::Store;
use wasmtime_wasi::p2::{add_to_linker_async, WasiCtxBuilder};

use crate::host::api::bindings;
use crate::host::api::bindings::local::operator::types as wit_types;
use crate::host::state::State;
use crate::kubernetes::crd::{
    EnvironmentVariable, WasmOperator as WasmOperatorCRD, WasmOperatorState, WasmOperatorStatus,
    WasmSource,
};
use crate::kubernetes::KubernetesService;
use crate::runtime::CONTROLLER_UUID;
use crate::runtime::{WasmEngineSingleton, WASMOP_CACHE_DIR};

use super::stats::WasmOperatorStatisticsRecorder;

const STATUS_PATCH_THROTTLE_DURATION: Duration = Duration::from_secs(1); // TODO: increase this and  make another process to update status periodically

// This struct mirrors the WatchRequest from WIT bindgen but enables us to use it as a key in a DashMap for managing watchers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WatchRequestKey {
    kind: String,
    namespace: String,
}
impl From<&wit_types::WatchRequest> for WatchRequestKey {
    fn from(request: &wit_types::WatchRequest) -> Self {
        Self {
            kind: request.kind.clone(),
            namespace: request.namespace.clone(),
        }
    }
}

pub type OperatorUid = String;

// This struct is a reduced version of the WasmOperator CRD that only contains the fields relevant for the runtime, this way we can reduce the memory usage of one WasmOperatorRuntime instance by not storing the entire CRD spec in memory.
pub struct WasmOperatorReduced {
    pub name: String,
    pub generation: Option<i64>,
    pub uid: OperatorUid,

    pub wasm: WasmSource,
    pub env: Vec<EnvironmentVariable>,
    pub args: Vec<String>,
}
impl From<&WasmOperatorCRD> for WasmOperatorReduced {
    fn from(crd: &WasmOperatorCRD) -> Self {
        Self {
            name: crd.name_any(),
            generation: crd.metadata.generation,
            uid: crd.uid().unwrap(),
            wasm: crd.spec.wasm.clone(),
            env: crd.spec.env.clone(),
            args: crd.spec.args.clone(),
        }
    }
}

struct LoadedState {
    operator: bindings::KubeOperator,
    store: Mutex<Store<State>>,
    last_active: Mutex<Instant>,
}

struct UnloadedState {
    // Path to the serialized memory file.
    state_path: PathBuf,
}

// Use boxed variants to reduce enum size
enum OperatorState {
    Loaded(Arc<LoadedState>),
    Unloaded(Arc<UnloadedState>),
}

pub struct WasmOperatorRuntime {
    pub cr: WasmOperatorReduced,
    state: RwLock<OperatorState>,
    watchers: DashMap<WatchRequestKey, CancellationToken>,
    task_tracker: Mutex<TaskTracker>,
    shutdown_tx: mpsc::Sender<String>,
    stats: WasmOperatorStatisticsRecorder,
    last_patch_time: Mutex<Instant>,
}

impl WasmOperatorRuntime {
    pub fn new(wasmop_cr: WasmOperatorReduced, shutdown_tx: mpsc::Sender<String>) -> Arc<Self> {
        Arc::new(Self {
            cr: wasmop_cr,
            state: RwLock::new(OperatorState::Unloaded(Arc::new(UnloadedState {
                state_path: PathBuf::new(),
            }))),
            watchers: DashMap::new(),
            task_tracker: Mutex::new(TaskTracker::new()),
            shutdown_tx,
            stats: WasmOperatorStatisticsRecorder::new(),
            last_patch_time: Mutex::new(Instant::now() - STATUS_PATCH_THROTTLE_DURATION * 2),
        })
    }

    async fn patch_k8s_status(&self, state: WasmOperatorState) -> Result<()> {
        let k8s_service: Arc<KubernetesService> = KubernetesService::global()
            .await
            .expect("Failed to initialize K8s service");

        let status = WasmOperatorStatus {
            state,
            last_updated: Utc::now().to_rfc3339(),
            observed_generation: self.cr.generation,
            owner: CONTROLLER_UUID.get().cloned(),
            statistics: Some(self.stats.get_statistics().await),
        };

        let patch = serde_json::json!({
            "status": status
        });

        // Update the Kubernetes resource status
        let kind = WasmOperatorCRD::kind(&());
        let namespace = std::env::var("WASMOP_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        k8s_service
            .patch_status(&kind, &self.cr.name, &namespace, &patch.to_string())
            .await?;

        Ok(())
    }

    async fn patch_k8s_status_throttled(
        &self,
        state: WasmOperatorState,
        force: bool,
    ) -> Result<()> {
        let mut last_patch_time_guard = self.last_patch_time.lock().await;
        if force || last_patch_time_guard.elapsed() > STATUS_PATCH_THROTTLE_DURATION {
            *last_patch_time_guard = Instant::now();
            self.patch_k8s_status(state).await?;
        }
        Ok(())
    }

    async fn throw_fatal_error<T>(&self, message: &str) -> Result<T> {
        self.stats.record_error(message).await;
        self.patch_k8s_status_throttled(WasmOperatorState::Error, true)
            .await
            .ok();
        self.shutdown_tx
            .send(self.cr.uid.clone())
            .await
            .context("Failed to send shutdown signal for operator")?;
        Err(anyhow::anyhow!(message.to_string()))
    }

    pub async fn unload(&self) -> Result<()> {
        // Acquire write lock and extract the loaded state
        let mut state_guard = self.state.write().await;
        let loaded_state = if let OperatorState::Loaded(ref state) = *state_guard {
            state.clone()
        } else {
            return Ok(());
        };

        let mut store_guard = loaded_state.store.lock().await;

        // Ask the component to serialize its own state
        let memory_data = match loaded_state
            .operator
            .call_serialize(&mut *store_guard)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                let error = format!("Failed to serialize state: {}", e);
                return self.throw_fatal_error(&error).await;
            }
        };

        self.stats.record_memory_usage(memory_data.len() as u32);

        info!(
            "Serializing {} bytes of memory for operator {}",
            memory_data.len(),
            self.cr.name
        );

        // Write state to memory to a file
        let state_path = PathBuf::from(format!("{}/{}/state.mem", WASMOP_CACHE_DIR, self.cr.uid));
        if let Some(parent) = state_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&state_path, &memory_data).await?;

        self.patch_k8s_status_throttled(WasmOperatorState::Idle, true)
            .await?;

        // Update the state to Unloaded
        *state_guard = OperatorState::Unloaded(Arc::new(UnloadedState { state_path }));

        self.stats.record_unloading_operation();

        Ok(())
    }

    async fn load(&self) -> Result<()> {
        info!("Loading operator {}...", self.cr.name);

        self.stats.record_loading_operation();
        let start_load = Instant::now();

        // This locks the state for the entire duration of the load process
        let mut state_guard = self.state.write().await;
        let unloaded_state = if let OperatorState::Unloaded(ref state) = *state_guard {
            state.clone()
        } else {
            return Ok(());
        };

        let (operator, mut store) = self.load_wasm_instance().await?;

        // If no state was saved this means it is the first time loading the component, otherwise we try to restore the previous state
        if unloaded_state.state_path.exists() {
            let saved_state = tokio::fs::read(&unloaded_state.state_path).await?;

            // Ask the new component instance to deserialize the state
            //operator.call_deserialize(&mut store, &saved_state).await?;
            if let Err(e) = operator.call_deserialize(&mut store, &saved_state).await {
                let error = format!("Failed to deserialize state: {}", e);
                return self.throw_fatal_error(&error).await;
            }

            info!(
                "Successfully restored memory state for operator {}",
                self.cr.name
            );

            tokio::fs::remove_file(&unloaded_state.state_path).await?;
        }

        self.patch_k8s_status_throttled(WasmOperatorState::Running, true)
            .await?;

        // Update the state to Loaded
        *state_guard = OperatorState::Loaded(Arc::new(LoadedState {
            operator,
            store: Mutex::new(store),
            last_active: Mutex::new(Instant::now()),
        }));

        let duration = start_load.elapsed().as_millis();
        self.stats.record_load_duration(duration as u32);

        Ok(())
    }

    async fn load_wasm_instance(&self) -> Result<(bindings::KubeOperator, Store<State>)> {
        let wasmtime_engine = wasmtime::Engine::global().await?;

        let target_arch = Triple::host();
        let generation = self.cr.generation.unwrap_or(0);
        let cache_path = format!(
            "{}/{}_{}/{}.cwasm",
            WASMOP_CACHE_DIR, self.cr.uid, generation, target_arch
        );
        let cache_path = PathBuf::from(cache_path);

        let component = if cache_path.exists() {
            let component_bytes = std::fs::read(&cache_path)?;
            (unsafe {
                Component::deserialize(&wasmtime_engine, &component_bytes).map_err(|e| {
                    std::fs::remove_file(&cache_path).ok();
                    anyhow::anyhow!(
                        "Failed to deserialize cached component '{:?}': {}",
                        cache_path,
                        e
                    )
                })
            })?
        } else {
            let wasm_bytes = match self.load_wasm_file() {
                Ok(bytes) => bytes,
                Err(e) => {
                    return self.throw_fatal_error(&e.to_string()).await;
                }
            };
            let component = match Component::new(&wasmtime_engine, &wasm_bytes) {
                Ok(c) => c,
                Err(e) => {
                    let error = format!(
                        "Failed to compile wasm for operator '{}': {}",
                        self.cr.name, e
                    );
                    return self.throw_fatal_error(&error).await;
                }
            };
            let component_bytes: Vec<u8> = component.serialize()?;

            if let Some(parent) = cache_path.parent() {
                std::fs::create_dir_all(parent).context(format!(
                    "Failed to create cache directory structure for {}",
                    self.cr.name
                ))?;
            }
            std::fs::write(cache_path, component_bytes).context(format!(
                "Failed to write compiled cwasm for component {}",
                self.cr.name
            ))?;
            component
        };

        let wasi_ctx = WasiCtxBuilder::new()
            .inherit_stdio()
            .args(&self.cr.args)
            .envs(
                &self
                    .cr
                    .env
                    .iter()
                    .map(|e| (e.name.as_str(), e.value.as_str()))
                    .collect::<Vec<_>>(),
            )
            .build();

        let k8s_service = KubernetesService::global().await?;

        let state = State {
            wasi_ctx,
            kubernetes_service: k8s_service,
            resources: Default::default(),
        };
        let mut store = Store::new(wasmtime_engine, state);

        let mut linker = Linker::new(wasmtime_engine);
        add_to_linker_async(&mut linker)?;

        bindings::KubeOperator::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx: &mut State| ctx)?;

        let operator =
            bindings::KubeOperator::instantiate_async(&mut store, &component, &linker).await?;

        Ok((operator, store))
    }

    fn load_wasm_file(&self) -> Result<Vec<u8>> {
        match self.cr.wasm.clone() {
            WasmSource::Pvc { path, file } => {
                debug!(
                    "Loading WASM from PVC path '{}' and file '{}'...",
                    path, file
                );
                let path = std::path::Path::new(&path).join(&file);
                std::fs::read(&path).context(format!(
                    "Failed to read WASM file '{}' from PVC path '{:?}'",
                    &file, &path
                ))
            }
        }
    }

    pub async fn is_idle(&self, threshold: Duration) -> bool {
        if let OperatorState::Loaded(state) = &*self.state.read().await {
            let last_active_guard = state.last_active.lock().await;
            last_active_guard.elapsed() > threshold
        } else {
            false
        }
    }

    pub async fn start_watching(self: Arc<Self>) -> Result<()> {
        // Get the watch requests for the operator
        let watch_requests = self
            .execute_via_wit(|operator, store| {
                Box::pin(async move { operator.call_get_watch_requests(store).await })
            })
            .await?;

        if !self.watchers.is_empty() {
            self.stop_watching().await;
        }

        // If task tracker is closed, reopen it to allow spawning new tasks
        {
            let mut task_tracker = self.task_tracker.lock().await;
            if task_tracker.is_closed() {
                *task_tracker = TaskTracker::new();
            }
        }

        // Create a watcher for each requested watch
        for request in &watch_requests {
            self.clone().start_watcher(request.clone()).await;
        }
        Ok(())
    }

    async fn start_watcher(self: Arc<Self>, request: wit_types::WatchRequest) {
        // Get the API resource for the requested k8s kind
        let client = KubernetesService::global().await.unwrap();
        let ar = client.find_api_resource(&request.kind).await.unwrap();

        // Create a watcher stream for the requested resource and namespace
        let mut k8s_watcher = watcher(
            client.dynamic_api(ar, &request.namespace),
            Default::default(),
        )
        .boxed();

        info!(
            "Watcher started for kind '{}' in namespace '{}'",
            request.kind, request.namespace
        );

        // Watch for events and dispatch reconcile calls in a seperate async thread
        let cancel_token = CancellationToken::new();
        self.watchers
            .insert(WatchRequestKey::from(&request), cancel_token.clone());

        let operator = self.clone();
        let tracker = self.task_tracker.lock().await;
        tracker.spawn_local(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Stoppin watcher for kind '{}' in namespace '{}' for operator '{}'.", request.kind, request.namespace, operator.cr.name.clone());
                        break;
                    }

                    event_option = k8s_watcher.next() => {
                        match event_option {
                            Some(Ok(event)) => match event {
                                Event::Apply(obj) => {
                                    operator.reconcile(
                                        bindings::local::operator::types::EventType::Added,
                                        &obj,
                                    )
                                    .await;
                                }
                                Event::Delete(obj) => {
                                    operator.reconcile(
                                        bindings::local::operator::types::EventType::Deleted,
                                        &obj,
                                    )
                                    .await;
                                }
                                Event::InitApply(obj) => {
                                    operator.reconcile(
                                        bindings::local::operator::types::EventType::Added,
                                        &obj,
                                    )
                                    .await;
                                }
                                Event::Init | Event::InitDone => {
                                    // These event indicate start/stop of a restart of a watcher stream, we can ignore these. 
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
            }
        });
    }

    pub async fn stop_watching(&self) {
        // Cancel all active watchers
        for watcher in self.watchers.iter() {
            watcher.value().cancel();
        }

        // Wait for all watcher tasks to finish
        let task_tracker = {
            let tracker_guard = self.task_tracker.lock().await;
            tracker_guard.close();
            tracker_guard.clone()
        };

        task_tracker.wait().await;

        self.watchers.clear();
    }

    async fn reconcile(
        &self,
        event_type: wit_types::EventType,
        k8s_object: &kube::api::DynamicObject,
    ) {
        let start_reconcile = Instant::now();

        let name = k8s_object.metadata.name.clone().unwrap_or_default();
        let namespace = k8s_object.metadata.namespace.clone().unwrap_or_default();
        let resource_json = match serde_json::to_string(k8s_object) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize resource to JSON: {}", e);
                return;
            }
        };

        info!(
            "Dispatching reconcile for event '{:?}' on resource '{}/{}' in namespace '{}'",
            event_type,
            k8s_object
                .types
                .as_ref()
                .map(|t| t.kind.as_str())
                .unwrap_or("Unknown kind"),
            name,
            namespace
        );

        let reconcile_request = wit_types::ReconcileRequest {
            event_type,
            name,
            namespace,
            resource_json,
        };

        if let Err(e) = self
            .execute_via_wit(|operator, store| {
                Box::pin(async move { operator.call_reconcile(store, &reconcile_request).await })
            })
            .await
        {
            error!(
                "Reconciliation for operator '{}' failed: {}",
                self.cr.name, e
            );
        }

        let duration = start_reconcile.elapsed().as_millis();
        self.stats.record_reconcile(duration as u32);

        self.patch_k8s_status_throttled(WasmOperatorState::Running, false)
            .await
            .ok();
    }

    pub async fn execute_via_wit<F, T>(&self, f: F) -> Result<T>
    where
        for<'a> F: FnOnce(
            &'a bindings::KubeOperator,
            &'a mut Store<State>,
        ) -> Pin<Box<dyn Future<Output = Result<T>> + 'a>>,
    {
        let mut state_guard = self.state.write().await;

        // Check if the operator is loaded, else load it before executing
        if let OperatorState::Unloaded(_) = *state_guard {
            drop(state_guard);
            self.load().await?;
            state_guard = self.state.write().await;
        }

        let loaded_state = if let OperatorState::Loaded(ref mut state) = *state_guard {
            state.clone()
        } else {
            return Err(anyhow::anyhow!("Operator is not in a loaded state"));
        };

        // Update the last active time
        let mut last_active_guard = loaded_state.last_active.lock().await;
        *last_active_guard = Instant::now();

        // Execute the function
        let mut store_guard = loaded_state.store.lock().await;
        f(&loaded_state.operator, &mut store_guard).await
    }
}
