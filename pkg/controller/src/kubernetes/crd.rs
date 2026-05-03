//! # Custom Resource Definitions
//!
//! This module defines the Rust structs for Kubernetes Custom Resources (CRDs)
//! used by the operator to represent Wasm components and their configuration.

use crate::config::metadata::EnvironmentVariable;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "test.dev",
    version = "v1",
    kind = "WasmOperator",
    namespaced,
    status = "WasmOperatorStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct WasmOperatorSpec {
    pub wasm: WasmSource,
    #[serde(default)]
    pub env: Vec<EnvironmentVariable>,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WasmOperatorStatus {
    pub loaded: bool,
    pub last_updated: Option<String>,
    pub observed_generation: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "sourceType", content = "source", rename_all = "camelCase")]
pub enum WasmSource {
    Pvc(String),
    // add other options like Git, S3, etc. in the future
}
