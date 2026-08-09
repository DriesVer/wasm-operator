use crate::runtime::watcher::watcher;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::{stream, Stream};
use kube::api::DynamicObject;
use kube::runtime::watcher::{Error as WatcherError, Event};
use kube::{Resource, ResourceExt};
use serde_json;
use target_lexicon::Triple;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::Store;
use wasmtime_wasi::WasiCtxBuilder;

use crate::host::api::bindings;
use crate::host::state::State;
use crate::kubernetes::crd::{
    EnvironmentVariable, WasmOperator as WasmOperatorCR, WasmOperatorState, WasmOperatorStatus,
    WasmSource,
};
use crate::kubernetes::KubernetesService;
use crate::runtime::stats::WasmOperatorStatisticsRecorder;
use crate::runtime::CONTROLLER_UUID;
use crate::runtime::{WasmEngineSingleton, WASMOP_CACHE_DIR};
use wasmtime_wasi::p2::bindings::Command;

const STATUS_PATCH_THROTTLE_DURATION: Duration = Duration::from_secs(5);

pub type OperatorUid = String;

// Type wrappers to make the watcher stream more human readable
type WatcherResult = Result<Event<DynamicObject>, WatcherError>;
type BoxedWatcherStream = Pin<Box<dyn Stream<Item = WatcherResult> + Send>>;

// This struct is a reduced version of the WasmOperator CRD that only contains the fields relevant for the runtime, this way we can reduce the memory usage of one WasmOperatorRuntime instance by not storing the entire CRD spec in memory.
pub struct WasmOperatorReduced {
    pub name: String,
    pub generation: Option<i64>,
    pub uid: OperatorUid,

    pub wasm: WasmSource,
    pub env: Vec<EnvironmentVariable>,
    pub args: Vec<String>,
}
impl From<&WasmOperatorCR> for WasmOperatorReduced {
    fn from(cr: &WasmOperatorCR) -> Self {
        Self {
            name: cr.name_any(),
            generation: cr.metadata.generation,
            uid: cr.uid().unwrap(),
            wasm: cr.spec.wasm.clone(),
            env: cr.spec.env.clone(),
            args: cr.spec.args.clone(),
        }
    }
}

pub enum WORCommand {
    StartOperator,
    LoadAt(DateTime<Utc>),
    Unload,
    Pause,
    Shutdown,
    ProcessWatchEvent(
        bindings::local::kube::api::WatchId,
        bindings::local::kube::api::WatchEvent,
    ),
}

// TODO: remove pub from LoadedState, UnloadedState, and OperatorState, debugging only
pub struct LoadedState {
    operator: bindings::Wasmoperator,
    store: Mutex<Store<State>>,
    last_active: Mutex<Instant>,
}

pub struct UnloadedState {
    // Path to the serialized memory file.
    state_path: PathBuf,
}

// Use boxed variants to reduce enum size
pub enum OperatorState {
    Loaded(Arc<LoadedState>),
    Unloaded(Arc<UnloadedState>),
}

pub struct WasmOperatorRuntime {
    pub cr: WasmOperatorReduced,
    pub cmd_tx: mpsc::UnboundedSender<WORCommand>, // Channel for sending commands to the operator's command handler

    // TODO: remove pub from state, debugging only
    pub state: RwLock<OperatorState>,
    task_tracker: TaskTracker,
    shutdown_token: CancellationToken,
    shutdown_tx: mpsc::Sender<OperatorUid>, // Channel to signal the operator is shutting down
    stats: WasmOperatorStatisticsRecorder,
    last_patch_time: Mutex<Instant>,
}

impl WasmOperatorRuntime {
    pub fn new(
        wasmop_cr: WasmOperatorReduced,
        shutdown_tx: mpsc::Sender<OperatorUid>,
    ) -> Arc<Self> {
        let (wasm_op_tx, wasm_op_rx) = mpsc::unbounded_channel();

        let self_ = Arc::new(Self {
            cr: wasmop_cr,
            cmd_tx: wasm_op_tx,
            state: RwLock::new(OperatorState::Unloaded(Arc::new(UnloadedState {
                state_path: PathBuf::new(),
            }))),
            task_tracker: TaskTracker::new(),
            shutdown_token: CancellationToken::new(),
            shutdown_tx,
            stats: WasmOperatorStatisticsRecorder::new(),
            last_patch_time: Mutex::new(Instant::now() - STATUS_PATCH_THROTTLE_DURATION * 2),
        });

        let self_clone = self_.clone();
        self_.task_tracker.spawn(async move {
            self_clone.handle_commands_loop(wasm_op_rx).await;
        });

        self_
    }

    async fn handle_commands_loop(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<WORCommand>) {
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_token.cancelled() => {
                    info!("Command handler for operator '{}' received shutdown signal, stopping command handling.", self.cr.name);
                    return;
                }

                command = rx.recv() => {
                    if let Some(command) = command {
                        match command {
                            WORCommand::StartOperator => {
                                if let Err(e) = self.clone().start().await {
                                    let error = format!("Failed to start operator: {}", e);
                                    let _ = self.throw_fatal_error::<()>(&error);
                                    error!("Operator '{}' failed to start operator: {}", self.cr.name, e);
                                    return;
                                }
                            },
                            WORCommand::LoadAt(timestamp) => {
                                self.clone().load_at(timestamp).await;
                            },
                            WORCommand::Unload => {
                                if let Err(e) = self.unload().await {
                                    let error = format!("Failed to unload: {}", e);
                                    let _ = self.throw_fatal_error::<()>(&error);
                                    error!("Operator '{}' failed to unload: {}", self.cr.name, e);
                                    return;
                                }
                            },
                            WORCommand::Pause => {
                                if let Err(e) = self.pause().await {
                                    let error = format!("Failed to pause: {}", e);
                                    let _ = self.throw_fatal_error::<()>(&error);
                                    error!("Operator '{}' failed to pause: {}", self.cr.name, e);
                                    return;
                                }
                            },
                            WORCommand::Shutdown => {
                                if let Err(e) = self.shutdown().await {
                                    error!("Operator '{}' failed to shutdown properly: {}", self.cr.name, e);
                                    return;
                                }
                            },
                            WORCommand::ProcessWatchEvent(id, watch_event) => {
                                let result = self.clone().execute_via_wit(
                                    |operator, store| {
                                        // Call the WIT function to process the watch event (wakes up the operator if it was waiting for events)
                                        let res = operator.call_receive_watch_event(&mut *store, id, &watch_event).map_err(anyhow::Error::from);
                                        res
                                    }
                                ).await;

                                if let Err(e) = result {
                                    warn!("Failed to send watch event for stream {}, removing stream: {:?}", id, e);
                                }
                            },
                        }
                    } else {
                        info!("Command channel for operator '{}' was closed, shutting down command handler.", self.cr.name);
                    }
                }
            }
        }
    }

    async fn patch_k8s_status(&self, state: WasmOperatorState) -> Result<()> {
        let k8s_service: Arc<KubernetesService> = KubernetesService::global()
            .await
            .expect("Failed to initialize K8s service");

        let status = WasmOperatorStatus {
            state,
            last_updated: Utc::now(),
            observed_generation: self.cr.generation,
            owner: CONTROLLER_UUID.get().cloned(),
            statistics: Some(self.stats.get_statistics().await),
        };

        let patch = serde_json::json!({
            "status": status
        });

        // Update the Kubernetes resource status
        let kind = WasmOperatorCR::kind(&());
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

    fn get_cache_path(&self) -> PathBuf {
        PathBuf::from(format!(
            "{}/{}_{}",
            WASMOP_CACHE_DIR,
            self.cr.uid,
            self.cr.generation.unwrap_or(0)
        ))
    }

    async fn unload(&self) -> Result<()> {
        // Acquire write lock and extract the loaded state
        let mut state_guard = self.state.write().await;
        let loaded_state = if let OperatorState::Loaded(ref state) = *state_guard {
            state.clone()
        } else {
            return Ok(());
        };

        let mut store_guard = loaded_state.store.lock().await;

        // Serialize the linear memory of the WASM component
        let memory_data = store_guard.get_linear_memory()?;

        self.stats.record_memory_usage(memory_data.len() as u32);

        info!(
            "Serializing {} bytes of memory for operator {}",
            memory_data.len(),
            self.cr.name
        );

        // Write state to memory to a file
        let state_path = self.get_cache_path().join("state.mem");
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

    async fn load(self: Arc<Self>) -> Result<()> {
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

        let (operator, mut store) = self.clone().load_wasm_instance().await?;

        // Restore the memory of the operator if a save file exists
        if unloaded_state.state_path.exists() {
            let saved_state = tokio::fs::read(&unloaded_state.state_path).await?;
            store.set_linear_memory(&saved_state)?;

            info!(
                "Successfully restored memory state for operator {}",
                self.cr.name
            );

            tokio::fs::remove_file(&unloaded_state.state_path).await?;
        }

        self.patch_k8s_status_throttled(WasmOperatorState::Running, true)
            .await?;

        // Starting the operator's async runtime and running until first yield point
        // When called again, the operator will continue from the last yield point and not restart from the beginning
        let _ = tokio::task::block_in_place(|| operator.wasi_cli_run().call_run(&mut store))?;

        // Update the state to Loaded
        *state_guard = OperatorState::Loaded(Arc::new(LoadedState {
            operator,
            store: Mutex::new(store),
            last_active: Mutex::new(Instant::now()),
        }));

        let duration = start_load.elapsed().as_millis();
        self.stats.record_load_duration(duration as u32);

        self.run_until_unloaded();

        Ok(())
    }

    async fn load_at(self: Arc<Self>, timestamp: DateTime<Utc>) {
        let self_clone = self.clone();

        self.task_tracker.spawn(async move {
            let now = Utc::now();
            if timestamp > now {
                if let Ok(std_duration) = (timestamp - now).to_std() {
                    tokio::select! {
                        biased;
                        _ = self_clone.shutdown_token.cancelled() => {
                            return;
                        }
                        _ = tokio::time::sleep(std_duration) => {},
                    }
                }
                info!(
                    "Loading operator '{}' at scheduled time {}...",
                    self_clone.cr.name, timestamp
                );
                if let Err(e) = self_clone.clone().load().await {
                    error!(
                        "Failed to load operator '{}' at scheduled time: {}",
                        self_clone.cr.name, e
                    );
                }
            }
        });
    }

    async fn load_wasm_instance(self: Arc<Self>) -> Result<(bindings::Wasmoperator, Store<State>)> {
        let wasmtime_engine = wasmtime::Engine::global().await?;

        let target_arch = Triple::host();
        let cache_path = self.get_cache_path().join(format!("{}.cwasm", target_arch));
        let component = if cache_path.exists() {
            info!(
                "Found cached component for operator '{}', loading from cache...",
                self.cr.name
            );
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
            info!(
                "No cached component found for operator '{}', compiling from wasm...",
                self.cr.name
            );
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
            .inherit_network()
            .allow_blocking_current_thread(true)
            .allow_ip_name_lookup(true)
            .build();

        let state = State {
            operator: self.clone(),
            wasi_ctx,
            resources: Default::default(),
        };
        let mut store = Store::new(wasmtime_engine, state);

        let mut linker = Linker::new(wasmtime_engine);

        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        bindings::Wasmoperator::add_to_linker::<_, HasSelf<_>>(&mut linker, |state| state)?;

        let operator = bindings::Wasmoperator::instantiate(&mut store, &component, &linker)?;

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

    async fn start_watching(self: Arc<Self>) -> Result<()> {
        // If task tracker is closed, we cannot start new tasks, the operator is dead.
        if self.task_tracker.is_closed() {
            return Err(anyhow::anyhow!(
                "Cannot start watchers for operator '{}' because task tracker is closed.",
                self.cr.name
            ));
        }

        let self_clone = self.clone();
        self.task_tracker.spawn(async move {
            if let Err(err) = self_clone.run_operator_loop().await {
                error!("Error running operator loop: {:?}", err);
            }
        });

        Ok(())
    }

    fn run_until_unloaded(self: &Arc<Self>) {
        let self_clone = self.clone();
        self.task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = self_clone.shutdown_token.cancelled() => {
                        info!("Operator '{}' received shutdown signal, stopping operator loop.", self_clone.cr.name);
                        return;
                    }
                    _ = tokio::task::yield_now() => {
                        let state_guard = self_clone.state.read().await;
                        if let OperatorState::Unloaded(_) = *state_guard {
                            info!("Operator '{}' has been unloaded, stopping operator loop.", self_clone.cr.name);
                            return;
                        }
                        if let OperatorState::Loaded(ref loaded_state) = *state_guard {
                            let operator = &loaded_state.operator;
                            let mut store = loaded_state.store.lock().await;
                            let result = tokio::task::block_in_place(|| {
                                let now = Instant::now();
                                let result = operator.wasi_cli_run().call_run(&mut *store);
                                let duration = now.elapsed().as_millis();
                                if duration > 10 {
                                    warn!("Operator '{}' run loop executed in {} ms", self_clone.cr.name, duration);
                                }
                                result
                            });

                            if let Err(e) = result {
                                error!("Error running operator loop for '{}': {:?}", self_clone.cr.name, e);
                            }
                        }
                    }
                }
            }
        });
    }

    async fn start(self: Arc<Self>) -> Result<()> {
        // This will load the operator and start the runtime until yield
        self.clone().load().await?;

        // Start an operator loop to progress the operator's async runtime and handle events
        self.run_until_unloaded();

        // let mut state_guard = self.state.write().await;
        // if let OperatorState::Unloaded(_) = *state_guard {
        //     drop(state_guard);
        //     self.clone().load().await?;
        //     state_guard = self.state.write().await;
        // }

        // let loaded_state = if let OperatorState::Loaded(ref mut state) = *state_guard {
        //     state.clone()
        // } else {
        //     return Err(anyhow::anyhow!("Operator is not in a loaded state"));
        // };
        // drop(state_guard);

        // let mut store_guard = loaded_state.store.lock().await;
        // loaded_state
        //     .operator
        //     .wasi_cli_run()
        //     .call_run(*store_guard)?;

        Ok(())
    }

    async fn run_operator_loop(self: Arc<Self>) -> Result<()> {
        let mut state_guard = self.state.write().await;
        if let OperatorState::Unloaded(_) = *state_guard {
            drop(state_guard);
            self.clone().load().await?;
            state_guard = self.state.write().await;
        }

        let loaded_state = if let OperatorState::Loaded(ref mut state) = *state_guard {
            state.clone()
        } else {
            return Err(anyhow::anyhow!("Operator is not in a loaded state"));
        };
        drop(state_guard);

        let mut store_guard = loaded_state.store.lock().await;

        // let program_result = loaded_state
        //     .operator
        //     .wasi_cli_run()
        //     .call_run(&mut *store_guard)
        //     .await?;

        todo!(
            "run the operator loop, which should include starting the watcher and handling events"
        );

        Ok(())
    }

    /*
    async fn watcher_loop(
        self: Arc<Self>,
        mut watcher_stream: stream::SelectAll<BoxedWatcherStream>,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_token.cancelled() => {
                    info!("Watcher loop for operator '{}' received shutdown signal, stopping watcher.", self.cr.name);
                    return;
                }

                watcher_event = watcher_stream.next() => {
                    // Extract the event itself
                    let event = match watcher_event {
                        Some(Ok(e)) => e,
                        Some(Err(e)) => {
                            self.throw_fatal_error::<()>(&format!("Watcher stream error: {}", e)).await.ok();
                            error!("Operator '{}' had watch stream error: {}", self.cr.name, e);
                            return;
                        },
                        None => {
                            warn!("Watcher stream for operator '{}' ended unexpectedly.", self.cr.name);
                            // TODO: we might want to restart the watcher here instead of exiting the loop, but we need to be careful to not end in a restart loop if the watcher keeps failing immediately
                            return;
                        }
                    };

                    // Map the event on WIT event types and extract the K8S object
                    let (event_type, k8s_obj) = match event {
                        Event::Apply(obj) | Event::InitApply(obj) => (wit_types::EventType::Applied, obj),
                        Event::Delete(obj) => (wit_types::EventType::Deleted, obj),
                        Event::Init => continue,
                        Event::InitDone => {
                            // Clear the reconcile history to not polute the prediction models
                            self.stats.clear_recent_reconcile_history().await;
                            continue
                        },
                    };

                    // Call reconcile for the event
                    if let Err(e) = self.clone().reconcile(event_type, &k8s_obj).await {
                        error!("Failed to reconcile '{:?}' event for operator '{}': {}", event_type, self.cr.name, e);
                    }
                }
            }
        }
    }
    */

    async fn stop_execution(&self) {
        // Cancel all active loops including watchers
        self.shutdown_token.cancel();

        // Wait for all spawned tasks to finish
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down operator '{}'...", self.cr.name);
        self.stop_execution().await;
        let cache_path = self.get_cache_path();
        if cache_path.exists() {
            std::fs::remove_dir_all(&cache_path)?;
        }
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        info!("Pausing operator '{}'...", self.cr.name);
        self.stop_execution().await;
        self.patch_k8s_status_throttled(WasmOperatorState::Paused, true)
            .await?;
        Ok(())
    }

    /*
    async fn reconcile(
        self: Arc<Self>,
        event_type: wit_types::EventType,
        k8s_object: &kube::api::DynamicObject,
    ) -> Result<()> {
        let start_reconcile = Instant::now();

        let name = k8s_object.name_any();
        let namespace = k8s_object.namespace().unwrap_or_default();
        let kind = k8s_object
            .types
            .as_ref()
            .map(|t| t.kind.as_str())
            .unwrap_or("Unknown kind");
        let resource_json = serde_json::to_string(k8s_object)?;

        info!(
            "Dispatching reconcile for event '{:?}' on resource '{}/{}' in namespace '{}'",
            event_type, kind, &name, &namespace
        );

        let reconcile_request = wit_types::ReconcileRequest {
            event_type,
            name: name.clone(),
            namespace: namespace.clone(),
            resource_json,
        };

        let reconcile_result = match self
            .execute_via_wit(|operator, store| {
                Box::pin(async move {
                    operator
                        .call_reconcile(store, &reconcile_request)
                        .await
                        .map_err(anyhow::Error::from)
                })
            })
            .await
        {
            Ok(result) => result,
            Err(e) => {
                let error = format!("Reconciliation crashed: {}", e);
                return self.throw_fatal_error(&error).await;
            }
        };

        match reconcile_result {
            wit_types::ReconcileResult::Ok => {}
            wit_types::ReconcileResult::Error(e) => {
                let error = format!(
                     "Reconcile error for resource '{}/{}' in namespace '{}': \n {}",
                    kind, &name, &namespace, e
                );
                self.stats.record_error(&error).await;
                error!("Operator '{}' threw error: {}", self.cr.name, error);
            }
            wit_types::ReconcileResult::Requeue(milis) => {
                let self_clone = self.clone();
                let event_type_clone = event_type.clone();
                let k8s_object_clone = k8s_object.clone();

                self.task_tracker.spawn(async move {
                    tokio::select! {
                        biased;
                        _ = self_clone.shutdown_token.cancelled() => {
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(milis as u64)) => {},
                    }
                    if let Err(e) = self_clone
                        .clone()
                        .reconcile(event_type_clone, &k8s_object_clone)
                        .await
                    {
                        error!(
                            "Failed to reconcile '{:?}' requeued event for operator '{}': {}",
                            event_type, self_clone.cr.name, e
                        );
                    }
                });
            }
        }

        let duration = start_reconcile.elapsed().as_millis();
        self.stats.record_reconcile(duration as u32).await;

        self.patch_k8s_status_throttled(WasmOperatorState::Running, false)
            .await
            .ok();
        Ok(())
    }
    */

    pub async fn execute_via_wit<F, T>(self: Arc<Self>, f: F) -> Result<T>
    where
        for<'a> F: FnOnce(&'a bindings::Wasmoperator, &'a mut Store<State>) -> Result<T>,
    {
        let mut state_guard = self.state.write().await;

        // Check if the operator is loaded, else load it before executing
        if let OperatorState::Unloaded(_) = *state_guard {
            debug!(
                "Operator '{}' is unloaded, loading before executing the WIT call...",
                self.cr.name
            );
            drop(state_guard);
            self.clone().load().await?;
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
        tokio::task::block_in_place(|| f(&loaded_state.operator, &mut store_guard))
    }

    pub async fn is_idle(&self, threshold: Duration) -> bool {
        if let OperatorState::Loaded(state) = &*self.state.read().await {
            let last_active_guard = state.last_active.lock().await;
            last_active_guard.elapsed() > threshold
        } else {
            false
        }
    }

    pub async fn get_reconcile_history(&self) -> Vec<DateTime<Utc>> {
        self.stats.get_recent_reconcile_history().await
    }
}
