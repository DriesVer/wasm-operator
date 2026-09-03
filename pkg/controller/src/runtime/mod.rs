//! # Runtime Module
//!
//! This module provides the core WebAssembly (Wasm) runtime capabilities for the operator.
//! It manages the Wasmtime engine and orchestrates the execution of individual Wasm components,
//! ensuring they can interact with the Kubernetes API and other host functionalities.

use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use kube::runtime::watcher;
use kube::runtime::watcher::Event;
use kube::ResourceExt;
use tokio::sync::{OnceCell, mpsc};
use tracing::{debug, error, info, warn};

use crate::prediction::{get_next_reconcile_prediction, PredictionModel};
use crate::kubernetes::crd::WasmOperator as WasmOperatorCRD;
use crate::kubernetes::KubernetesService;
use crate::runtime::wasmengine::{WasmEngineSingleton,GlobalMonotonicClock};
use crate::runtime::wasmoperator::{OperatorUid, WORCommand, WasmOperatorReduced, WasmOperatorRuntime};

mod stats;
pub mod wasmengine;
pub mod wasmoperator;

// TODO: move all environment variable parsing into a single file

// TODO: split this env for cache and swap paths into separate env vars, so that they can be configured independently
pub const WASMOP_CACHE_DIR: &str = match option_env!("WASMOP_CACHE_DIR") {
    Some(path) => path,
    None => "/tmp/wasmop-cache",
};

/// Parses a duration string (e.g. "300000", "300000ms" "300s", "5m", "2h", "1d") at compile time.
const fn parse_duration(s: Option<&'static str>, default_ms: u64) -> Duration {
    // Convert to character bytes for easier manipulation
    let s = match s {
        Some(val) => val.as_bytes(),
        None => return Duration::from_millis(default_ms),
    };

    if s.is_empty() {
        return Duration::from_millis(default_ms);
    }

    // Determine unit multiplier based on suffix
    let len = s.len();
    if len == 0 {
        return Duration::from_millis(default_ms);
    }
    let last_byte = s[len - 1];
    let (digits_len, multiplier) = match last_byte {
        b's' | b'S' => {
            if len >= 2 && (s[len - 2] == b'm' || s[len - 2] == b'M') {
                (len - 2, 1) // ms
            } else {
                (len - 1, 1_000) // seconds
            }
        }
        b'm' | b'M' => (len - 1, 60_000), // minutes
        b'h' | b'H' => (len - 1, 3_600_000), // hours
        b'd' | b'D' => (len - 1, 86_400_000), // days
        b'0'..=b'9' => (len, 1), // Default to ms if no unit is given
        _ => return Duration::from_millis(default_ms),
    };

    if digits_len == 0 {
        return Duration::from_millis(default_ms);
    }

    // Parse ascii digits manually for const context
    let mut num: u64 = 0;
    let mut i = 0;
    while i < digits_len {
        let byte = s[i];
        match byte {
            b'0'..=b'9' => {
                let digit = (byte - b'0') as u64; // Convert ASCII to numeric value
                num = num * 10 + digit;
            }
            _ => return Duration::from_millis(default_ms), // Invalid character, fallback to default
        }
        i += 1;
    }

    Duration::from_millis(num * multiplier)
}

pub const IDLE_THRESHOLD: Duration = parse_duration(option_env!("WASMOP_IDLE_THRESHOLD"), 5000);
pub const EXECUTE_THRESHOLD: Duration = parse_duration(option_env!("WASMOP_EXECUTE_THRESHOLD"), 500);
const _: () = {
    assert!(
        IDLE_THRESHOLD.as_millis() > EXECUTE_THRESHOLD.as_millis(),
        "WASMOP_IDLE_THRESHOLD must be greater than WASMOP_EXECUTE_THRESHOLD"
    );
};

pub static CONTROLLER_UUID: OnceCell<String> = OnceCell::const_new();

pub struct MainController {
    operators: DashMap<OperatorUid, Arc<WasmOperatorRuntime>>,
    crashed_operators: DashMap<OperatorUid, Option<i64>>,
}

impl MainController {
    pub fn new() -> Arc<Self> {
        let _ = CONTROLLER_UUID.set(uuid::Uuid::new_v4().to_string());
        Arc::new(Self {
            operators: DashMap::new(),
            crashed_operators: DashMap::new(),
        })
    }

    /// Start the main runtime to pull WasmOperator CRs and start/stop them accordingly.
    pub async fn start(self: Arc<Self>) -> Result<()> {
        // Start a background task to periodically check for idle operators and unload them.
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.idle_check_loop().await;
        });

        // Start a task to handle operators shutdowns orginating from the wasmoperator runtime itself. E.g. fatal errors
        let runtime = self.clone();
        let (op_shutdown_tx, op_shutdown_rx) = mpsc::channel::<String>(10);
        tokio::spawn(async move {
            runtime.handle_operator_shutdown(op_shutdown_rx).await;
        });

        self.wasmoperator_watch_loop(op_shutdown_tx).await?;

        // Start watching for WasmOperator CRs and apply them accordingly.
        Ok(())
    }

    async fn apply_operator(&self, wasmop_cr: &WasmOperatorCRD, op_shutdown_tx: mpsc::Sender<String>) -> Result<()> {
        let op_uid = wasmop_cr
            .uid()
            .ok_or_else(|| anyhow::anyhow!("Kubernetes object is missing a UID"))?;

        let crash_gen = self.crashed_operators.get(&op_uid).and_then(|v| *v);
        if wasmop_cr.metadata.generation <= crash_gen {
            warn!(
                    "Skipping operator '{}' with generation '{}' since it previously crashed with generation '{}'",
                    wasmop_cr.name_any(), 
                    wasmop_cr.metadata.generation.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string()), 
                    crash_gen.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string())
                );
            return Ok(());
        }

        if let Some(op) = self.operators.get(&op_uid) {
            if op.cr.generation >= wasmop_cr.metadata.generation {
                return Ok(());
            }
            self.delete_operator(&op_uid).await;
        } else {
            // Completely new operator, refresh the k8s discovery cache to ensure we have the latest API resources available
            let k8s_client = KubernetesService::global().await?;
            let rd = k8s_client.refresh_discovery().await;
            if let Err(e) = rd {
                warn!("Failed to refresh Kubernetes discovery cache: {}", e);
            }
        }

        let red_cr = WasmOperatorReduced::from(wasmop_cr);
        let op = WasmOperatorRuntime::new(red_cr, op_shutdown_tx.clone());

        if let Err(e) = op.cmd_tx.send(WORCommand::StartOperator) {
            self.crashed_operators
                .insert(op_uid.clone(), op.cr.generation);
            error!(
                "Failed to send starting command for operator '{}' with generation '{}': {}",
                op.cr.name, op.cr.generation.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string()), e
            );
            return Ok(());
        }

        self.operators.insert(op_uid, op);

        Ok(())
    }

    async fn delete_operator(&self, uid: &str) {
        if let Some((_, op)) = self.operators.remove(uid) {
            if let Err(e) = op.cmd_tx.send(WORCommand::Shutdown) {
                error!(
                    "Failed to send shutdown command to operator '{}' with generation '{}': {}",
                    op.cr.name, op.cr.generation.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string()), e
                );
            }
            drop(op);
        }
    }

    async fn handle_operator_shutdown(&self, mut rx: mpsc::Receiver<String>) {
        while let Some(op_uid) = rx.recv().await {

            // Extract the operator info from the dashmap to avoind holding the lock
            let op_info = if let Some(op_ref) = self.operators.get(&op_uid) {
                let op = op_ref.value();
                Some((op.cr.name.clone(), op.cr.generation))
            } else {
                None
            };

            if let Some((name, generation)) = op_info {
                warn!(
                    "Operator '{}' with generation '{}' has shut down unexpectedly, removing it from execution",
                    name, 
                    generation.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string())
                );

                self.crashed_operators.insert(op_uid.clone(), generation);
                self.delete_operator(&op_uid).await;
            }
        }
    }

    async fn idle_check_loop(self: Arc<Self>) {
        debug!("Starting idle check loop with inactive threshold {:?} and idle threshold {:?}", IDLE_THRESHOLD, EXECUTE_THRESHOLD);
        let shutdown_token = crate::shutdown::shutdown_token();
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("Received shutdown signal, stopping idle check loop...");
                    return;
                }

                _ = tokio::time::sleep(IDLE_THRESHOLD / 2) => {
                    let ops: Vec<_> = self.operators.iter().map(|e| e.value().clone()).collect();
                    for op in ops {
                        if op.is_idle(IDLE_THRESHOLD, EXECUTE_THRESHOLD).await {
                            info!(
                                "Operator '{}' is idle for more than {:?}, unloading it.",
                                op.cr.name, IDLE_THRESHOLD
                            );
                            if let Err(e) = op.cmd_tx.send(WORCommand::Unload) {
                                error!("Failed to send unload command to operator '{}': {}", op.cr.name, e);
                                continue;
                            }
                            let use_prediction = matches!(
                                option_env!("WASMOP_USE_RECONCILE_PREDICTION"),
                                Some("true" | "TRUE" | "1")
                            );
                            if use_prediction {
                                let history = op.get_reconcile_history().await;
                                let wake_up_time = match get_next_reconcile_prediction(history, PredictionModel::AutoReg).await {
                                    Ok(prediction) => prediction,
                                    Err(e) => {
                                        error!("Failed to get reconcile prediction for operator '{}': {}", op.cr.name, e);
                                        continue;
                                    }
                                };
                                debug!("Next reconcile prediction for operator '{}' is in {:?}, sending LoadAt command.", op.cr.name, wake_up_time);
                                op.cmd_tx.send(WORCommand::LoadAt(wake_up_time)).unwrap_or_else(|e| {
                                    error!("Failed to send LoadAt command to operator '{}': {}", op.cr.name, e);
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    async fn wasmoperator_watch_loop(self: Arc<Self>, op_shutdown_tx: mpsc::Sender<String>) -> Result<()> {
        let k8s_service: Arc<KubernetesService> = KubernetesService::global().await?;

        // Get the K8S watcher stream for the WasmOperator CRD.
        let namespace = std::env::var("WASMOP_NAMESPACE").unwrap_or_else(|_| "default".to_string());

        let mut wasmop_watcher =
            watcher(k8s_service.wasmoperator_api(&namespace), Default::default()).boxed();

        // Empty vector to keep track of operators when watch stream is restarted
        let mut control_restarted: Vec<OperatorUid> = Vec::new();

        // Watch for changes to WasmOperator CRs
        let shutdown_token = crate::shutdown::shutdown_token();
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("Received shutdown signal, stopping WasmOperator watcher loop and pausing all operators...");
                    let shutdown_futures = self.operators.iter().map(|entry| {
                        let op = entry.value().clone();
                        async move {
                            if let Err(e) = op.cmd_tx.send(WORCommand::Pause) {
                                error!("Failed to send pause command to operator '{}': {}", op.cr.name, e);
                            }
                        }
                    });
                    futures::future::join_all(shutdown_futures).await;
                    info!("All operators have been shut down, exiting.");
                    return Ok(());
                },
                event = wasmop_watcher.next() => {
                    match event {
                        Some(Ok(event)) => {
                            match event {
                                Event::Apply(wasmop_cr) => {
                                    // TODO: handle the result if loading is unsuccessful
                                    self.apply_operator(&wasmop_cr, op_shutdown_tx.clone()).await?;
                                }
                                Event::Delete(obj) => {
                                    let op_uid = obj.uid().unwrap();
                                    self.delete_operator(&op_uid).await;
                                }
                                Event::Init => {
                                    // This event is emitted when the watcher stream is (re)started
                                    // We clear the control list to track which operators still need to be active after the restart
                                    control_restarted.clear();
                                }
                                Event::InitApply(obj) => {
                                    // This event is emitted for each existing object when the watcher stream is (re)started
                                    // We add the operator to the control list and ensure it's active, if not already restarted
                                    let op_uid = obj.uid().unwrap();
                                    control_restarted.push(op_uid.clone());
                                    self.apply_operator(&obj, op_shutdown_tx.clone()).await?;
                                }
                                Event::InitDone => {
                                    // This event is emitted when the initial list of existing objects has been processed after a watcher stream restart
                                    // We check which operators from the control list are not restarted and stop them, this ensures that deleted operators while the stream was down are properly stopped
                                    let active_op: Vec<OperatorUid> = self
                                        .operators
                                        .iter()
                                        .map(|entry| entry.key().clone())
                                        .collect();
                                    for op_uid in active_op {
                                        if !control_restarted.contains(&op_uid) {
                                            self.delete_operator(&op_uid).await;
                                        }
                                    }
                                    control_restarted.clear();
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!("WasmOperator controller's watcher for 'WasmOperator' in namespace '{}' encountered a fatal error, exiting execution: {}", namespace, e);
                            crate::shutdown::shutdown_token().cancel();
                        }
                        None => {
                            info!(
                                "Watcher for 'WasmOperator' in namespace '{}' stream ended.",
                                namespace,
                            );
                            return Err(anyhow::anyhow!(
                                "WasmOperator watcher stream ended unexpectedly."
                            ));
                        }
                    }
                }
            }
        }
    }
}
