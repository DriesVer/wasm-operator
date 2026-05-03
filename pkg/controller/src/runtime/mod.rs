//! # Runtime Module
//!
//! This module provides the core WebAssembly (Wasm) runtime capabilities for the operator.
//! It manages the Wasmtime engine and orchestrates the execution of individual Wasm components,
//! ensuring they can interact with the Kubernetes API and other host functionalities.

use crate::runtime::watcher::watcher;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use kube::api::ResourceExt;
use kube::runtime::watcher::{self, Event};
use kube::Resource;
use tokio::sync::OnceCell;
use tracing::{error, info, warn};

use crate::config::metadata::WasmComponentMetadata;
use crate::kubernetes::crd::WasmOperator as WasmOperatorCRD;
use crate::kubernetes::KubernetesService;

pub use self::wasmengine::WasmEngineSingleton;
use self::wasmoperator::WasmOperator;

pub mod wasmengine;
mod wasmoperator;

type OperatorUid = String;

// TODO: change back to 5 minutes in production, set to 5 seconds for testing purposes
const IDLE_THRESHOLD: Duration = Duration::from_secs(5); // 5 minutes

pub static CONTROLLER_UUID: OnceCell<String> = OnceCell::const_new();

pub struct MainController {
    operators: DashMap<OperatorUid, Arc<WasmOperator>>,
}

impl MainController {
    pub fn new() -> Arc<Self> {
        let _ = CONTROLLER_UUID.set(uuid::Uuid::new_v4().to_string());
        Arc::new(Self {
            operators: DashMap::new(),
        })
    }

    /// Start the main runtime to pull WasmOperator CRs and start/stop them accordingly.
    pub async fn start(self: Arc<Self>) -> Result<()> {
        // Start a background task to periodically check for idle operators and unload them.
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.idle_check_loop().await;
        });

        self.wasmoperator_watch_loop().await?;

        // Start watching for WasmOperator CRs and apply them accordingly.
        Ok(())
    }

    async fn apply_operator(&self, obj: &kube::api::DynamicObject) -> Result<()> {
        let op_uid = obj
            .uid()
            .ok_or_else(|| anyhow::anyhow!("Kubernetes object is missing a UID"))?;

        if let Some(op) = self.operators.get(&op_uid) {
            if op.metadata.generation >= obj.metadata.generation {
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

        let op_metadata = WasmComponentMetadata::load_from_k8s_object(obj)?;
        let op = WasmOperator::new(op_metadata);
        op.clone().start_watching().await?;

        self.operators.insert(op_uid, op);

        Ok(())
    }

    async fn delete_operator(&self, uid: &str) {
        if let Some((_, op)) = self.operators.remove(uid) {
            op.stop_watching().await;
            drop(op);
        }
    }

    async fn idle_check_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(IDLE_THRESHOLD / 2).await;
            for entry in self.operators.iter() {
                let op = entry.value();
                if op.is_idle(IDLE_THRESHOLD).await {
                    info!(
                        "Operator '{}' is idle for more than {:?}, unloading it.",
                        op.metadata.name, IDLE_THRESHOLD
                    );
                    if let Err(e) = op.unload().await {
                        error!(
                            "Failed to unload idle operator '{}': {}",
                            op.metadata.name, e
                        );
                    }
                }
            }
        }
    }

    async fn wasmoperator_watch_loop(self: Arc<Self>) -> Result<()> {
        let k8s_service: Arc<KubernetesService> = KubernetesService::global().await?;

        // Get the API resource for WasmOperator CRD to be able to watch it for changes.
        let kind = WasmOperatorCRD::kind(&());
        let api_resource = k8s_service.find_api_resource(&kind).await?;

        // Get the K8S watcher stream for the WasmOperator CRD.
        let namespace = std::env::var("WASMOP_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let mut wasmop_watcher = watcher(
            k8s_service.dynamic_api(api_resource, &namespace),
            Default::default(),
        )
        .boxed();

        // Empty vector to keep track of operators when watch stream is restarted
        let mut control_restarted: Vec<OperatorUid> = Vec::new();

        // Watch for changes to WasmOperator CRs
        loop {
            match wasmop_watcher.next().await {
                Some(Ok(event)) => {
                    match event {
                        Event::Apply(obj) => {
                            // TODO: handle the result if loading is unsuccessful
                            self.apply_operator(&obj).await?;
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
                            self.apply_operator(&obj).await?;
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
                        "Watcher for '{}' in namespace '{}' encountered an error: {}",
                        &kind, namespace, e
                    );
                }
                None => {
                    info!(
                        "Watcher for '{}' in namespace '{}' stream ended.",
                        &kind, namespace,
                    );
                    return Err(anyhow::anyhow!(
                        "WasmOperator watcher stream ended unexpectedly."
                    ));
                }
            }
        }
    }
}
