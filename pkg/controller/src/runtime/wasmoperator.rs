use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use kube::{Resource, ResourceExt};
use serde_json;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use target_lexicon::Triple;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::Store;
use wasmtime_wasi::WasiCtxBuilder;

use crate::host::state::State;
use crate::host::wit::bindings;
use crate::kubernetes::crd::{
    EnvironmentVariable, WasmOperator as WasmOperatorCR, WasmOperatorState, WasmOperatorStatus,
    WasmSource,
};
use crate::kubernetes::KubernetesService;
use crate::runtime::stats::WasmOperatorStatisticsRecorder;
use crate::runtime::{
    GlobalMonotonicClock, WasmEngineSingleton, CONTROLLER_UUID, WASMOP_CACHE_DIR,
};

// TODO: make this configurable via env var
const STATUS_PATCH_THROTTLE_DURATION: Duration = Duration::from_secs(5);

pub type OperatorUid = String;

// TODO: add wasm_hash to wasmoperator CRD

// This struct is a reduced version of the WasmOperator CRD that only contains the fields relevant for the runtime, this way we can reduce the memory usage of one WasmOperatorRuntime instance by not storing the entire CRD spec in memory.
pub struct WasmOperatorReduced {
    pub name: String,
    pub generation: Option<i64>,
    pub uid: OperatorUid,

    pub wasm: WasmSource,
    pub wasm_hash: Mutex<Option<String>>,
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
            wasm_hash: Mutex::new(None),
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
    CheckIdle(Duration, Duration, tokio::sync::oneshot::Sender<bool>),
}

struct LoadedState {
    operator: bindings::Wasmoperator,
    store: Mutex<Store<State>>,
}

struct UnloadedState {
    // TODO: state path is deterministic, we can remove it
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
    pub cmd_tx: mpsc::UnboundedSender<WORCommand>, // Channel for sending commands to the operator's command handler

    state: Arc<RwLock<OperatorState>>,
    task_tracker: TaskTracker,
    shutdown_token: CancellationToken,
    shutdown_tx: mpsc::Sender<OperatorUid>, // Channel to signal the operator is shutting down
    last_active: AtomicI64,
    executed_idle: AtomicI64,

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
            state: Arc::new(RwLock::new(OperatorState::Unloaded(Arc::new(
                UnloadedState {
                    state_path: PathBuf::new(),
                },
            )))),
            task_tracker: TaskTracker::new(),
            shutdown_token: CancellationToken::new(),
            shutdown_tx,
            last_active: AtomicI64::new(0),
            executed_idle: AtomicI64::new(0),
            stats: WasmOperatorStatisticsRecorder::new(),
            last_patch_time: Mutex::new(Instant::now() - STATUS_PATCH_THROTTLE_DURATION * 2),
        });

        let self_clone = self_.clone();
        tokio::spawn(async move {
            self_clone.handle_commands_loop(wasm_op_rx).await;
        });

        self_
    }

    async fn handle_commands_loop(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<WORCommand>) {
        let mut watch_events: VecDeque<(
            bindings::local::kube::api::WatchId,
            bindings::local::kube::api::WatchEvent,
        )> = VecDeque::new();
        let mut latest_bookmark: Option<(
            bindings::local::kube::api::WatchId,
            bindings::local::kube::api::WatchEvent,
        )> = None;

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
                                // Prevent operator from unloading if there are pending watch events to process
                                if !watch_events.is_empty() {
                                    self.update_last_active();
                                    continue;
                                }
                                if let Err(e) = self.unload().await {
                                    let error = format!("Failed to unload: {}", e);
                                    let _ = self.throw_fatal_error::<()>(&error).await;
                                    error!("Operator '{}' failed to unload: {}", self.cr.name, e);
                                    return;
                                }
                            },
                            WORCommand::Pause => {
                                // Prevent operator from pasuing if there are pending watch events to process
                                // TODO: change this to that the operator will take the unfinished watch events with it
                                if !watch_events.is_empty() {
                                    self.update_last_active();
                                    continue;
                                }
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
                                if let bindings::local::kube::api::WatchEvent::Bookmark(_) = &watch_event {
                                    latest_bookmark = Some((id, watch_event));
                                } else {
                                    watch_events.push_back((id, watch_event));
                                    latest_bookmark = None; // Bookmark is not relevant if there is a newer normal event
                                    self.stats.record_reconcile().await;
                                }
                            },
                            WORCommand::CheckIdle(idle_threshold, execute_threshold, reply_tx) => {
                                if !watch_events.is_empty() {
                                    // Operator still needs to process watch events
                                    let _ = reply_tx.send(false);
                                    continue;
                                }
                                if !self.is_loaded() {
                                    // Operator is not loaded so cannot be idle
                                    let _ = reply_tx.send(false);
                                    continue;
                                }
                                let idle_threshold = idle_threshold.as_millis() as i64;

                                // Check if operator has been long enough in loaded state
                                let last_active = self.last_active.load(Ordering::SeqCst);
                                let now = Utc::now().timestamp_millis();
                                let diff = now - last_active;
                                let is_active_long = diff > idle_threshold;

                                // Check if operator has been executing for too long without yielding
                                let executed_idle = self.executed_idle.load(Ordering::SeqCst);
                                let execute_threshold = execute_threshold.as_millis() as i64;
                                let is_executing_idle = executed_idle > execute_threshold;

                                let is_idle = is_active_long && is_executing_idle;

                                let _ = reply_tx.send(is_idle);
                            },
                        }
                    } else {
                        info!("Command channel for operator '{}' was closed, shutting down command handler.", self.cr.name);
                        return;
                    }
                }

                _ = tokio::task::yield_now(), if self.is_loaded() || !watch_events.is_empty() => {
                    if let Some((id, watch_event)) = watch_events.pop_front() {
                        let result = self.clone().execute_via_wit(
                            |operator, store| {
                                operator.call_receive_watch_event(&mut *store, id, &watch_event).map_err(anyhow::Error::from)
                            }, true
                        ).await;

                        if let Err(e) = result {
                            let error = format!("Operator '{}' crashed during watch event processing: {:?}", self.cr.name, e);
                            let _ = self.throw_fatal_error::<()>(&error).await;
                            error!("{}", error);
                            return;
                        }
                    } else if self.is_loaded() && latest_bookmark.is_some() {
                        let (id, watch_event) = latest_bookmark.take().unwrap();
                        let result = self.clone().execute_via_wit(
                            |operator, store| {
                                operator.call_receive_watch_event(&mut *store, id, &watch_event).map_err(anyhow::Error::from)
                            }, false
                        ).await;

                        if let Err(e) = result {
                            let error = format!("Operator '{}' crashed during watch event processing: {:?}", self.cr.name, e);
                            let _ = self.throw_fatal_error::<()>(&error).await;
                            error!("{}", error);
                            return;
                        }
                    } else {
                        // If no commands are received, yield to allow other tasks to run
                        if let Err(e) = self.clone().run_until_stalled().await {
                            let error = format!("Operator '{}' crashed during run loop: {}", self.cr.name, e);
                            let _ = self.throw_fatal_error::<()>(&error).await;
                            error!("{}", error);
                        }
                    }
                }
            }
        }
    }

    fn is_loaded(&self) -> bool {
        if let Ok(state_guard) = self.state.try_read() {
            matches!(*state_guard, OperatorState::Loaded(_))
        } else {
            false
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

    fn get_swap_path(&self) -> PathBuf {
        PathBuf::from(format!(
            "{}/memory/{}_{}.mem",
            WASMOP_CACHE_DIR,
            self.cr.uid,
            self.cr.generation.unwrap_or(0)
        ))
    }

    async fn get_cache_path(&self) -> PathBuf {
        let wasm_hash = self.cr.wasm_hash.lock().await.clone().unwrap_or_default();
        let target_arch = Triple::host();
        PathBuf::from(format!(
            "{}/binaries/{}_{}.cwasm",
            WASMOP_CACHE_DIR, wasm_hash, target_arch
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

        let state_path = self.get_swap_path();

        // TODO: move this to a Lazy static or similar to avoid trying to create the directory every time
        if let Some(parent) = state_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = std::fs::File::create(&state_path)?;
        store_guard.snapshot_to_writer(&mut file)?;

        let file_metadata = tokio::fs::metadata(&state_path).await?;
        let mem_size = file_metadata.len() as u32;

        self.stats.record_memory_usage(mem_size);

        self.patch_k8s_status_throttled(WasmOperatorState::Idle, true)
            .await?;

        // Update the state to Unloaded
        *state_guard = OperatorState::Unloaded(Arc::new(UnloadedState { state_path }));

        self.stats.record_unloading_operation();

        info!(
            "Serialized {} bytes of memory for operator {}",
            mem_size, self.cr.name
        );

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
            let file = std::fs::File::open(&unloaded_state.state_path)?;
            let mmap = unsafe { memmap2::Mmap::map(&file)? };

            store.set_snapshot(&mmap)?;

            info!(
                "Successfully restored memory state for operator {}",
                self.cr.name
            );

            drop(mmap);
            drop(file);

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
        }));
        self.update_last_active();

        let duration = start_load.elapsed().as_millis();
        self.stats.record_load_duration(duration as u32);

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

    async fn load_cached_cwasm_file(&self, cache_path: &PathBuf) -> Result<Component> {
        let wasmtime_engine = wasmtime::Engine::global().await?;
        let component_bytes = tokio::fs::read(cache_path).await?;
        let res = unsafe { Component::deserialize(wasmtime_engine, &component_bytes) };
        match res {
            Ok(comp) => Ok(comp),
            Err(e) => {
                let _ = tokio::fs::remove_file(cache_path).await;
                Err(anyhow::anyhow!(
                    "Failed to deserialize cached component '{:?}': {}",
                    cache_path,
                    e
                ))
            }
        }
    }

    async fn get_or_compile_component(&self) -> Result<Component> {
        // Get hash or compute it if not present
        let mut wasm_hash_guard = self.cr.wasm_hash.lock().await;
        let wasm_bytes = if wasm_hash_guard.is_none() {
            debug!(
                "No hash found for operator '{}', computing hash...",
                self.cr.name
            );
            let bytes: Vec<u8> = self.load_wasm_file().await?;
            let hash = blake3::hash(&bytes).to_hex().to_string();
            *wasm_hash_guard = Some(hash);
            Some(bytes)
        } else {
            None
        };
        drop(wasm_hash_guard);

        let cache_path = self.get_cache_path().await;

        if cache_path.exists() {
            match self.load_cached_cwasm_file(&cache_path).await {
                Ok(component) => {
                    debug!(
                        "Found cached component for operator '{}', loaded from cache.",
                        self.cr.name
                    );
                    return Ok(component);
                }
                Err(e) => {
                    error!(
                        "Corrupt or invalid cache for operator '{}', recompiling: {}",
                        self.cr.name, e
                    );
                }
            }
        }

        // Load bytes (if not loaded during hash generation) and compile
        info!(
            "No valid cached component found for operator '{}', compiling from WASM...",
            self.cr.name
        );
        let wasm_bytes = match wasm_bytes {
            Some(bytes) => bytes,
            None => self.load_wasm_file().await?,
        };

        let wasmtime_engine = wasmtime::Engine::global().await?;
        let component = Component::new(wasmtime_engine, &wasm_bytes).map_err(|e| {
            anyhow::anyhow!(
                "Failed to compile WASM for operator '{}': {}",
                self.cr.name,
                e
            )
        })?;

        // Cache the compiled cwasm
        let component_bytes = component.serialize()?;
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "Failed to create cache directory structure for {}",
                    self.cr.name
                )
            })?;
        }

        let temp_path = cache_path.with_extension(format!("{}.tmp", self.cr.uid));
        tokio::fs::write(&temp_path, component_bytes)
            .await
            .with_context(|| {
                format!(
                    "Failed to write compiled cwasm to temp file for component {}",
                    self.cr.name
                )
            })?;

        tokio::fs::rename(&temp_path, &cache_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename temp file to cache path for component {}",
                    self.cr.name
                )
            })?;

        Ok(component)
    }

    async fn load_wasm_instance(self: Arc<Self>) -> Result<(bindings::Wasmoperator, Store<State>)> {
        let component = self.get_or_compile_component().await?;

        let wasmtime_engine = wasmtime::Engine::global().await?;

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
            .allow_blocking_current_thread(true) // Operator is mostly ran from a blocking thread (spawn_blocking), so allow blocking current thread for wasi calls,
            .monotonic_clock(GlobalMonotonicClock) // Needed for the tokio timer wheel
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

    async fn load_wasm_file(&self) -> Result<Vec<u8>> {
        match self.cr.wasm.clone() {
            WasmSource::Pvc { path, file } => {
                debug!(
                    "Loading WASM from PVC path '{}' and file '{}'...",
                    path, file
                );
                let path = std::path::Path::new(&path).join(&file);
                tokio::fs::read(&path).await.context(format!(
                    "Failed to read WASM file '{}' from PVC path '{:?}'",
                    &file, &path
                ))
            }
        }
    }

    async fn run_until_stalled(self: &Arc<Self>) -> Result<()> {
        let owned_state_guard = self.state.clone().read_owned().await;

        let start_time = Instant::now();
        let result = tokio::task::spawn_blocking(move || match &*owned_state_guard {
            OperatorState::Unloaded(_) => None,
            OperatorState::Loaded(ref loaded_state) => {
                let operator = &loaded_state.operator;
                let mut store = loaded_state.store.blocking_lock();
                let result = operator.wasi_cli_run().call_run(&mut *store);
                Some(result)
            }
        })
        .await;

        let elapsed_ms = start_time.elapsed().as_millis() as i64;
        self.executed_idle.fetch_add(elapsed_ms, Ordering::SeqCst);

        let out = match result {
            // Operator was unloaded
            Ok(None) => {
                info!(
                    "Operator '{}' has been unloaded, stopping operator loop.",
                    self.cr.name
                );
                Ok(())
            }
            // Operator execution failed
            Ok(Some(Err(e))) => {
                let error = format!("Operator '{}' crashed during run loop: {}", self.cr.name, e);
                Err(anyhow::anyhow!(error))
            }
            // Tokio join error (e.g. thread panic)
            Err(join_err) => {
                let error = format!(
                    "Blocking task for operator '{}' panicked or failed: {}",
                    self.cr.name, join_err
                );
                Err(anyhow::anyhow!(error))
            }
            // Successful execution
            Ok(Some(Ok(_))) => Ok(()),
        };
        out
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
        let cache_path = self.get_cache_path().await;
        let state_path = self.get_swap_path();
        tokio::join!(
            async {
                let _ = tokio::fs::remove_file(cache_path).await;
            },
            async {
                let _ = tokio::fs::remove_file(state_path).await;
            },
        );
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        info!("Pausing operator '{}'...", self.cr.name);
        // TODO: first unload the operator to disk so state can be shared
        self.stop_execution().await;
        self.patch_k8s_status_throttled(WasmOperatorState::Paused, true)
            .await?;
        Ok(())
    }

    pub fn update_last_active(&self) {
        let now_ms: i64 = Utc::now().timestamp_millis();
        self.last_active.store(now_ms, Ordering::SeqCst);
        self.executed_idle.store(0, Ordering::SeqCst);
    }

    pub async fn execute_via_wit<F, T>(self: Arc<Self>, f: F, update_last_active: bool) -> Result<T>
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

        // Execute the function
        let mut store_guard = loaded_state.store.lock().await;
        // TODO: could be changed to spawn_blocking, but then we would need to make sure that the function f is Send + 'static, which might not be the case for all functions. For now, we will use block_in_place to avoid blocking the async runtime.
        let result = tokio::task::block_in_place(|| f(&loaded_state.operator, &mut store_guard));

        if update_last_active {
            self.update_last_active();
        }
        result
    }

    pub async fn is_idle(&self, idle_threshold: Duration, execute_threshold: Duration) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if let Err(e) =
            self.cmd_tx
                .send(WORCommand::CheckIdle(idle_threshold, execute_threshold, tx))
        {
            error!(
                "Failed to send CheckIdle command to operator '{}': {}",
                self.cr.name, e
            );
            return false;
        }
        // If the command channel is blocked (e.g., operator is executing WASM), it's not idle.
        match tokio::time::timeout(std::time::Duration::from_millis(100), rx).await {
            Ok(Ok(res)) => res,
            _ => false, // Timeout or error
        }
    }

    pub async fn get_reconcile_history(&self) -> Vec<DateTime<Utc>> {
        self.stats.get_recent_reconcile_history().await
    }
}
