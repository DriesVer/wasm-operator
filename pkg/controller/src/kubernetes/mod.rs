//! # Kubernetes Module
//!
//! This module provides the KubernetesService singleton for interacting with the cluster API.
//! It handles the creation of a Kubernetes client, execution of HTTP requests, and
//! dynamic resource management for both the parent controller and child operators.

pub mod crd;

use anyhow::{anyhow, Context, Result};
use kube::api::{Api, DynamicObject, Patch, PatchParams};
use kube::discovery::ApiResource;
use kube::{Client, Config, Discovery};
use serde_json::Value;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

use crate::kubernetes::crd::WasmOperator as WasmOperatorCRD;

/// A service for interacting with the Kubernetes API dynamically.
///
/// This service discovers available API resources at startup and provides
/// methods to interact with them using dynamic objects, allowing it to work
/// with any Kubernetes resource kind, including Custom Resources.
pub struct KubernetesService {
    pub client: Client,
    //discovery: Discovery,
    discovery: RwLock<Discovery>,
}

//static INSTANCE: OnceCell<KubernetesService> = OnceCell::const_new();
static INSTANCE: OnceCell<Arc<KubernetesService>> = OnceCell::const_new();

impl KubernetesService {
    /// Returns a reference to the global `KubernetesService` instance.
    ///
    /// During initialization, the function infers the Kubernetes configuration
    /// from the environment, creates a Kubernetes client, and performs API discovery.
    pub async fn global() -> Result<Arc<Self>> {
        let instance = INSTANCE
            .get_or_try_init(|| async {
                let config = Config::infer()
                    .await
                    .context("Failed to infer Kubernetes config")?;

                let client =
                    Client::try_from(config).context("Failed to create Kubernetes client")?;

                let discovery = Discovery::new(client.clone())
                    .run()
                    .await
                    .context("Failed to run Kubernetes API discovery")?;

                Ok::<Arc<KubernetesService>, anyhow::Error>(Arc::new(KubernetesService {
                    client,
                    discovery: RwLock::new(discovery),
                }))
            })
            .await?;

        // Clone the Arc to return an owned handle with a 'static lifetime
        Ok(Arc::clone(instance))
    }

    /// Returns a clone of the global Kubernetes client.
    pub async fn global_client() -> Result<Client> {
        let service = Self::global().await?;
        Ok(service.client.clone())
    }

    /// Refreshes the cached Kubernetes API discovery.
    pub async fn refresh_discovery(&self) -> Result<()> {
        let discovery = Discovery::new(self.client.clone())
            .run()
            .await
            .context("Failed to refresh Kubernetes API discovery")?;
        *self.discovery.write().await = discovery;
        Ok(())
    }

    /// Finds an API resource by its kind in the discovered groups.
    pub async fn find_api_resource(&self, kind: &str) -> Result<ApiResource> {
        let discovery_guard = self.discovery.read().await;

        for group in discovery_guard.groups() {
            for version in group.versions() {
                for (ar, _caps) in group.versioned_resources(version) {
                    if ar.kind.eq_ignore_ascii_case(kind) {
                        return Ok(ar.clone());
                    }
                }
            }
        }

        Err(anyhow!(
            "Kind '{}' not found in discovered API resources",
            kind
        ))
    }

    /// Returns a dynamic, namespaced API client for a given `ApiResource`.
    pub fn dynamic_api(&self, ar: ApiResource, namespace: &str) -> Api<DynamicObject> {
        Api::namespaced_with(self.client.clone(), namespace, &ar)
    }

    /// Returns a typed API client for the WasmOperator Custom Resource.
    pub fn wasmoperator_api(&self, namespace: &str) -> Api<WasmOperatorCRD> {
        Api::namespaced(self.client.clone(), namespace)
    }

    /// Patches the status of a Kubernetes resource using a JSON string.
    pub async fn patch_status(
        &self,
        kind: &str,
        name: &str,
        namespace: &str,
        status_json: &str,
    ) -> Result<()> {
        let ar = self.find_api_resource(kind).await?;
        let api = self.dynamic_api(ar, namespace);

        let status: Value = serde_json::from_str(status_json)
            .context("Failed to deserialize status from JSON for patching")?;

        let pp = PatchParams::default();
        if let Err(e) = api.patch_status(name, &pp, &Patch::Merge(&status)).await {
            return Err(anyhow!("Failed to patch status: {}", e));
        }

        Ok(())
    }
}
