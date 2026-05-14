//! # Kubernetes Module
//!
//! This module provides a service for interacting with the Kubernetes API. It handles
//! the creation of a Kubernetes client, execution of HTTP requests against the API,
//! and serialization/deserialization of Kubernetes API responses.

pub mod crd;

use anyhow::{anyhow, Context, Result};
use kube::api::{Api, DeleteParams, DynamicObject, Patch, PatchParams, PostParams};
use kube::discovery::ApiResource;
use kube::{Client, Config, Discovery};
use serde_json::Value;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

use crate::kubernetes::crd::WasmOperator as WasmOperatorCRD;

const IMMUTABLE_METADATA_FIELDS: &[&str] = &[
    "creationTimestamp",
    "deletionGracePeriodSeconds",
    "deletionTimestamp",
    "generateName",
    "generation",
    "managedFields",
    "resourceVersion",
    "selfLink",
    "uid",
];
const IMMUTABLE_METADATA_FIELDS_SNAKE_CASE: &[&str] = &[
    "creation_timestamp",
    "deletion_grace_period_seconds",
    "deletion_timestamp",
    "generate_name",
    "generation",
    "managed_fields",
    "resource_version",
    "self_link",
    "uid",
];

fn sanitize_patch_payload(resource: &mut Value) {
    let Some(obj) = resource.as_object_mut() else {
        return;
    };

    if let Some(metadata) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
        // Avoid sending immutable/server-managed metadata fields in updates.
        for &key in IMMUTABLE_METADATA_FIELDS {
            metadata.remove(key);
        }
        for &key in IMMUTABLE_METADATA_FIELDS_SNAKE_CASE {
            metadata.remove(key);
        }

        metadata.retain(|_, value| !value.is_null());
    }
}

/// A service for interacting with the Kubernetes API dynamically.
///
/// This service discovers available API resources at startup and provides
/// methods to interact with them using dynamic objects, allowing it to work
/// with any Kubernetes resource kind, including Custom Resources.
pub struct KubernetesService {
    client: Client,
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

    pub async fn refresh_discovery(&self) -> Result<()> {
        let discovery = Discovery::new(self.client.clone())
            .run()
            .await
            .context("Failed to refresh Kubernetes API discovery")?;
        *self.discovery.write().await = discovery;
        Ok(())
    }

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

    pub fn wasmoperator_api(&self, namespace: &str) -> Api<WasmOperatorCRD> {
        Api::namespaced(self.client.clone(), namespace)
    }

    pub async fn get_resource(&self, kind: &str, name: &str, namespace: &str) -> Result<String> {
        let ar = self.find_api_resource(kind).await?;
        let api = self.dynamic_api(ar, namespace);
        let resource = api.get(name).await.context("Failed to get resource")?;
        serde_json::to_string(&resource).context("Failed to serialize resource to JSON")
    }

    pub async fn list_resources(&self, kind: &str, namespace: &str) -> Result<Vec<String>> {
        let ar = self.find_api_resource(kind).await?;
        let api = self.dynamic_api(ar, namespace);

        let list = api
            .list(&kube::api::ListParams::default())
            .await
            .context("Failed to list resources")?;

        // Serialize each resource in the list to a JSON string
        let json_list = list
            .items
            .into_iter()
            .map(|item| serde_json::to_string(&item))
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to serialize resource list to JSON")?;

        Ok(json_list)
    }

    pub async fn create_resource(
        &self,
        kind: &str,
        namespace: &str,
        resource_json: &str,
    ) -> Result<()> {
        let ar = self.find_api_resource(kind).await?;
        let api = self.dynamic_api(ar, namespace);
        let resource: DynamicObject = serde_json::from_str(resource_json)
            .context("Failed to deserialize resource from JSON")?;
        api.create(&PostParams::default(), &resource)
            .await
            .context("Failed to create resource")?;
        Ok(())
    }

    pub async fn update_resource(
        &self,
        kind: &str,
        name: &str,
        namespace: &str,
        resource_json: &str,
        sanitize: bool,
    ) -> Result<()> {
        let ar = self.find_api_resource(kind).await?;
        let api = self.dynamic_api(ar, namespace);
        let mut resource: Value = serde_json::from_str(resource_json)
            .context("Failed to deserialize resource from JSON for update")?;

        if sanitize {
            sanitize_patch_payload(&mut resource);
        }

        // Force needed if the resource was created by another controller e.g. client-side apply
        let pp = PatchParams::apply(kind).force();

        let _ = api
            .patch(name, &pp, &Patch::Apply(&resource))
            .await
            .context("Failed to update resource")?;
        Ok(())
    }

    pub async fn delete_resource(&self, kind: &str, name: &str, namespace: &str) -> Result<()> {
        let ar = self.find_api_resource(kind).await?;
        let api = self.dynamic_api(ar, namespace);
        api.delete(name, &DeleteParams::default())
            .await
            .context("Failed to delete resource")?;
        Ok(())
    }

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
