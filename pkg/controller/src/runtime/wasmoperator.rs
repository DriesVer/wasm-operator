use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use kube::{Resource, ResourceExt};
use serde_json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use target_lexicon::Triple;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};
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

const STATUS_PATCH_THROTTLE_DURATION: Duration = Duration::from_secs(5);

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

type RunnerStopAck = oneshot::Sender<()>;

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

    runner_tx: mpsc::Sender<RunnerStopAck>,
    runner_rx: Mutex<mpsc::Receiver<RunnerStopAck>>,

    stats: WasmOperatorStatisticsRecorder,
    last_patch_time: Mutex<Instant>,
}

impl WasmOperatorRuntime {
    pub fn new(
        wasmop_cr: WasmOperatorReduced,
        shutdown_tx: mpsc::Sender<OperatorUid>,
    ) -> Arc<Self> {
        let (wasm_op_tx, wasm_op_rx) = mpsc::unbounded_channel();

        let (runner_tx, runner_rx) = mpsc::channel(1);

        let self_ = Arc::new(Self {
            cr: wasmop_cr,
            cmd_tx: wasm_op_tx,
            state: RwLock::new(OperatorState::Unloaded(Arc::new(UnloadedState {
                state_path: PathBuf::new(),
            }))),
            task_tracker: TaskTracker::new(),
            shutdown_token: CancellationToken::new(),
            shutdown_tx,
            runner_tx,
            runner_rx: Mutex::new(runner_rx),
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
                                if let Err(e) = self.clone().load().await {
                                    let error = format!("Failed to start operator: {}", e);
                                    let _ = self.throw_fatal_error::<()>(&error).await;
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
                                    let _ = self.throw_fatal_error::<()>(&error).await;
                                    error!("Operator '{}' failed to unload: {}", self.cr.name, e);
                                    return;
                                }
                            },
                            WORCommand::Pause => {
                                if let Err(e) = self.pause().await {
                                    let error = format!("Failed to pause: {}", e);
                                    let _ = self.throw_fatal_error::<()>(&error).await;
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
                                    let error = format!("Operator '{}' crashed during watch event processing: {:?}", self.cr.name, e);
                                    let _ = self.throw_fatal_error::<()>(&error).await;
                                    error!("{}", error);
                                    return;
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

    pub async fn is_loaded(&self) -> bool {
        let state_guard = self.state.read().await;
        matches!(*state_guard, OperatorState::Loaded(_))
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

        // Stop the operator's async runtime and wait for it to finish
        let (ack_tx, ack_rx) = oneshot::channel();
        let runner_tx = self.runner_tx.clone();
        runner_tx
            .send(ack_tx)
            .await
            .context("Failed to send stop signal to operator loop")?;
        ack_rx
            .await
            .context("Failed to receive acknowledgment from operator loop")?;

        let mut store_guard = loaded_state.store.lock().await;

        // Serialize the linear memory of the WASM component
        let memory_data = store_guard.get_snapshot()?;

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

        debug!("Unload completed for operator {}", self.cr.name);

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
            store.set_snapshot(&saved_state)?;

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

        // TODO maybe remove allow blocking current thread if possible (were debug things), do this as part of block_in_place conversion
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
            .allow_blocking_current_thread(true)
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

    fn run_until_unloaded(self: &Arc<Self>) {
        let self_clone = self.clone();
        self.task_tracker.spawn(async move {
            let mut runner_rx_guard = match self_clone.runner_rx.try_lock() {
                Ok(guard) => guard,
                // There was already a runner loop running, we don't need to start another one
                Err(e) => {
                    error!("Operator '{}' already has a runner loop running, not starting another one: {}", self_clone.cr.name, e);
                    return;
                },
            };
            loop {
                tokio::select! {
                    biased;
                    _ = self_clone.shutdown_token.cancelled() => {
                        info!("Operator '{}' received shutdown signal, stopping operator loop.", self_clone.cr.name);
                        return;
                    }

                    Some(ack_tx) = runner_rx_guard.recv() => {
                        debug!("Operator '{}' received stop signal for operator loop to unload", self_clone.cr.name);
                        let _ = ack_tx.send(());
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
                                let result = operator.wasi_cli_run().call_run(&mut *store);
                                result
                            });

                            if let Err(e) = result {
                                let error = format!("Operator '{}' crashed during run loop: {}", self_clone.cr.name, e);
                                let _ = self_clone.throw_fatal_error::<()>(&error).await;
                                error!("{}", error);
                                return;
                            }
                        }
                    }
                }
            }
        });
    }

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
