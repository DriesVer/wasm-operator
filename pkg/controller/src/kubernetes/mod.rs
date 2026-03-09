//! # Kubernetes Module
//!
//! This module provides a service for interacting with the Kubernetes API. It handles
//! the creation of a Kubernetes client, execution of HTTP requests against the API,
//! and serialization/deserialization of Kubernetes API responses.

use anyhow::{anyhow, Context, Result};
use kube::api::{Api, DeleteParams, DynamicObject, Patch, PatchParams, PostParams};
use kube::discovery::{ApiGroup, ApiResource};
use kube::{Client, Config, Discovery};
use serde_json::Value;
use std::convert::TryFrom;

/// A service for interacting with the Kubernetes API dynamically.
///
/// This service discovers available API resources at startup and provides
/// methods to interact with them using dynamic objects, allowing it to work
/// with any Kubernetes resource kind, including Custom Resources.
pub struct KubernetesService {
    client: Client,
    discovery: Discovery,
}

impl KubernetesService {
    /// Creates a new `KubernetesService`.
    ///
    /// This function infers the Kubernetes configuration from the environment,
    /// creates a Kubernetes client, and performs API discovery.
    pub async fn new() -> Result<Self> {
        let config = Config::infer()
            .await
            .context("Failed to infer Kubernetes config")?;
        let client = Client::try_from(config).context("Failed to create Kubernetes client")?;
        let discovery = Discovery::new(client.clone())
            .run()
            .await
            .context("Failed to run Kubernetes API discovery")?;
        Ok(KubernetesService { client, discovery })
    }

    /// Finds the `ApiResource` and (optional) `ApiGroup` for a given kind.
    ///
    /// This function searches the discovered API resources for a kind matching
    /// the provided name (case-insensitive).
    pub fn find_api_resource(&self, kind: &str) -> Result<(ApiResource, Option<&ApiGroup>)> {
        for group in self.discovery.groups() {
            for version in group.versions() {
                for (ar, _caps) in group.versioned_resources(version) {
                    if ar.kind.eq_ignore_ascii_case(kind) {
                        return Ok((ar.clone(), Some(group)));
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

    pub async fn get_resource(&self, kind: &str, name: &str, namespace: &str) -> Result<String> {
        let (ar, _) = self.find_api_resource(kind)?;
        let api = self.dynamic_api(ar, namespace);
        let resource = api.get(name).await.context("Failed to get resource")?;
        serde_json::to_string(&resource).context("Failed to serialize resource to JSON")
    }

    pub async fn list_resources(&self, kind: &str, namespace: &str) -> Result<Vec<String>> {
        let (ar, _) = self.find_api_resource(kind)?;
        let api = self.dynamic_api(ar, namespace);
        
        let list = api.list(&kube::api::ListParams::default())
            .await
            .context("Failed to list resources")?;
        
        // Serialize each resource in the list to a JSON string
        let json_list = list.items.into_iter()
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
        let (ar, _) = self.find_api_resource(kind)?;
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
    ) -> Result<()> {
        let (ar, _) = self.find_api_resource(kind)?;
        let api = self.dynamic_api(ar, namespace);
        let resource: Value = serde_json::from_str(resource_json)
            .context("Failed to deserialize resource from JSON for update")?;
        api.patch(name, &PatchParams::apply(kind), &Patch::Apply(&resource))
            .await
            .context("Failed to update resource")?;
        Ok(())
    }

    pub async fn delete_resource(&self, kind: &str, name: &str, namespace: &str) -> Result<()> {
        let (ar, _) = self.find_api_resource(kind)?;
        let api = self.dynamic_api(ar, namespace);
        api.delete(name, &DeleteParams::default())
            .await
            .context("Failed to delete resource")?;
        Ok(())
    }
}
