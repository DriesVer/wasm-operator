//! # Runtime Module
//!
//! This module provides the core WebAssembly (Wasm) runtime capabilities for the operator.
//! It manages the Wasmtime engine and orchestrates the execution of individual Wasm components,
//! ensuring they can interact with the Kubernetes API and other host functionalities.

use crate::prediction::{get_next_reconcile_prediction, PredictionModel};
use crate::runtime::watcher::watcher;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use kube::runtime::watcher::{self, Event};
use kube::ResourceExt;
use tokio::sync::{OnceCell, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};


use crate::kubernetes::crd::WasmOperator as WasmOperatorCRD;
use crate::kubernetes::KubernetesService;
use crate::runtime::wasmengine::WasmEngineSingleton;
use crate::runtime::wasmoperator::{OperatorUid, WORCommand, WasmOperatorReduced, WasmOperatorRuntime};

mod stats;
pub mod wasmengine;
mod wasmoperator;

// TODO: change back to 5 minutes in production, set to 5 seconds for testing purposes
const IDLE_THRESHOLD: Duration = Duration::from_secs(5); // 5 minutes

pub const WASMOP_CACHE_DIR: &str = match option_env!("WASMOP_CACHE_DIR") {
    Some(path) => path,
    None => "/tmp/wasmop-cache",
};

pub static CONTROLLER_UUID: OnceCell<String> = OnceCell::const_new();

pub struct MainController {
    operators: DashMap<OperatorUid, Arc<WasmOperatorRuntime>>,
    crashed_operators: DashMap<OperatorUid, Option<i64>>,
    shutdown_token: CancellationToken,
}

impl MainController {
    pub fn new(shutdown_token: CancellationToken) -> Arc<Self> {
        let _ = CONTROLLER_UUID.set(uuid::Uuid::new_v4().to_string());
        Arc::new(Self {
            operators: DashMap::new(),
            crashed_operators: DashMap::new(),
            shutdown_token,
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

        if let Err(e) = op.clone().start_watching().await {
            self.crashed_operators
                .insert(op_uid.clone(), op.cr.generation);
            error!(
                "Failed to start operator '{}' with generation '{}': {}",
                op.cr.name, op.cr.generation.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string()), e
            );
            return Ok(());
        }

        self.operators.insert(op_uid, op);

        Ok(())
    }

    async fn delete_operator(&self, uid: &str) {
        if let Some((_, op)) = self.operators.remove(uid) {
            if let Err(e) = op.shutdown().await {
                error!("Failed to shutdown operator '{}': {}", op.cr.name, e);
            }
            drop(op);
        }
    }

    async fn handle_operator_shutdown(&self, mut rx: mpsc::Receiver<String>) {
        while let Some(op_uid) = rx.recv().await {
            if let Some(op) = self.operators.get(&op_uid) {
                let op = op.value();
                self.crashed_operators
                    .insert(op_uid.clone(), op.cr.generation);
                warn!(
                    "Operator '{}' with generation '{}' has shut down unexpectedly, removing it from execution",
                    op.cr.name, op.cr.generation.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string())
                );
                self.delete_operator(&op_uid).await;
            }
        }
    }

    async fn idle_check_loop(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    info!("Received shutdown signal, stopping idle check loop...");
                    return;
                }

                _ = tokio::time::sleep(IDLE_THRESHOLD / 2) => {
                    for entry in self.operators.iter() {
                        let op = entry.value();
                        if op.is_idle(IDLE_THRESHOLD).await {
                            info!(
                                "Operator '{}' is idle for more than {:?}, unloading it.",
                                op.cr.name, IDLE_THRESHOLD
                            );
                            if let Err(e) = op.unload().await {
                                error!("Failed to unload idle operator '{}', removing it from execution: {}", op.cr.name, e);
                                self.crashed_operators
                                    .insert(entry.key().clone(), op.cr.generation);
                                self.delete_operator(entry.key()).await;
                            }
                            let history = op.get_reconcile_history().await;
                            let wake_up_time = match get_next_reconcile_prediction(history, PredictionModel::SES).await {
                                Ok(prediction) => prediction,
                                Err(e) => {
                                    error!("Failed to get reconcile prediction for operator '{}': {}", op.cr.name, e);
                                    continue;
                                }
                            };
                            op.cmd_tx.send(WORCommand::LoadAt(wake_up_time)).await.unwrap_or_else(|e| {
                                error!("Failed to send LoadAt command to operator '{}': {}", op.cr.name, e);
                            });
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
        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    info!("Received shutdown signal, stopping WasmOperator watcher loop and pausing all operators...");
                    let shutdown_futures = self.operators.iter().map(|entry| {
                        let op = entry.value().clone();
                        async move {
                            if let Err(e) = op.pause().await {
                                error!("Failed to pause operator '{}': {}", op.cr.name, e);
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
                            warn!(
                                "Watcher for 'WasmOperator' in namespace '{}' encountered an error: {}",
                                namespace, e
                            );
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
