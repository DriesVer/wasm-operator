//! # Custom Resource Definitions
//!
//! This module defines the Rust structs for Kubernetes Custom Resources (CRDs)
//! used by the operator to represent Wasm components and their configuration.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentVariable {
    pub name: String,
    pub value: String,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "test.dev",
    version = "v1",
    kind = "WasmOperator",
    namespaced,
    status = "WasmOperatorStatus",
    shortname = "wasmop",
    doc = "A custom resource that defines a WebAssembly-based operator used in the wasm-operator framework."
)]
#[serde(rename_all = "camelCase")]
pub struct WasmOperatorSpec {
    pub wasm: WasmSource,
    #[serde(default)]
    pub env: Vec<EnvironmentVariable>,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WasmSource {
    #[serde(rename_all = "camelCase")]
    Pvc { path: String, file: String },
    // add other options like Git, S3, etc. in the future
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WasmOperatorStatus {
    pub loaded: bool,

    pub last_updated: Option<String>,
    pub observed_generation: Option<i64>,
    pub owner: Option<String>,

    pub statistics: Option<WasmOperatorStatistics>,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WasmOperatorStatistics {
    pub reconcile_total_24h: u32,
    pub reconcile_cold_start_ratio: u8,
    pub wasm_load_duration_ms_avg: u32,
    pub wasm_load_duration_ms_max: u32,
    pub reconcile_duration_ms_avg: u32,
    pub reconcile_duration_ms_max: u32,
    pub memory_usage_bytes: u32,
    pub activity_ratio: u8,
    pub idle_duration_s_avg: u64,
    pub idle_duration_s_max: u64,
    pub active_duration_s_avg: u64,
    pub active_duration_s_max: u64,
}
