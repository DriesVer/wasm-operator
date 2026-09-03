//! # Custom Resource Definitions
//!
//! This module defines the Rust structs for Kubernetes Custom Resources (CRDs)
//! used by the operator to represent Wasm components and their configuration.

use chrono::{DateTime, Utc};
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
    doc = "A custom resource that defines a WebAssembly-based operator used in the wasm-operator framework.",
    printcolumn = r#"{"name":"State", "type":"string", "jsonPath":".status.state"}"#,
    printcolumn = r#"{"name":"Memory", "type":"integer", "jsonPath":".status.statistics.memoryUsageBytes", "description":"Current memory usage in bytes"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
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
    pub state: WasmOperatorState,
    pub last_updated: DateTime<Utc>,
    pub observed_generation: Option<i64>,
    pub owner: Option<String>,

    pub statistics: Option<WasmOperatorStatistics>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WasmOperatorState {
    Unclaimed,
    Running,
    Idle,
    Paused,
    Error,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WasmOperatorStatistics {
    pub reconcile_total_24h: u32,
    #[schemars(range(min = 0, max = 255))]
    pub reconcile_cold_start_ratio: u8, // If higher than 100, it means it loaded more (due to predicted loads) than it reconciled
    pub wasm_load_duration_msec_avg: u32,
    pub wasm_load_duration_msec_max: u32,
    pub memory_usage_bytes: u32,

    #[schemars(range(min = 0, max = 100))]
    pub activity_ratio: u8,
    pub idle_duration_sec_avg: u64,
    pub idle_duration_sec_max: u64,
    pub active_duration_sec_avg: u64,
    pub active_duration_sec_max: u64,
    pub recent_errors: Vec<String>,
}
