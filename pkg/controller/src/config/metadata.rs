//! # Metadata Module
//!
//! This module defines the data structures for representing WebAssembly (Wasm) component
//! metadata, including environment variables and command-line arguments. It also provides
//! functionality for loading this metadata from YAML configuration files.

use anyhow::Result;
use kube::api::ResourceExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::from_value;
use std::fs;
use std::path::PathBuf;

use crate::kubernetes::crd::WasmSource;

pub type OperatorUid = String;

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
pub struct EnvironmentVariable {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WasmComponentMetadata {
    pub name: String,
    pub wasm: WasmSource,
    pub generation: Option<i64>,
    pub uid: OperatorUid,
    #[serde(default)]
    pub env: Vec<EnvironmentVariable>,
    #[serde(default)]
    pub args: Vec<String>,
}

impl WasmComponentMetadata {
    /// Load component metadata from a YAML file
    #[allow(unused)]
    pub fn load_from_yaml(path: &PathBuf) -> Result<Vec<WasmComponentMetadata>> {
        let contents = fs::read_to_string(path)?;

        if contents.trim().is_empty() {
            return Ok(Vec::new());
        }

        contents
            .split("\n---")
            .filter_map(
                |yaml_doc| match serde_yaml::from_str::<WasmComponentMetadata>(yaml_doc) {
                    Err(err) if err.to_string().contains("EOF while parsing a value") => None,
                    result => {
                        Some(result.map_err(|e| anyhow::anyhow!("Failed to parse module: {}", e)))
                    }
                },
            )
            .collect()
    }

    pub fn load_from_k8s_object(k8s_object: &kube::api::DynamicObject) -> Result<Self> {
        let name = k8s_object.name_any();

        let wasm_value = k8s_object.data.pointer("/spec/wasm").ok_or_else(|| {
            anyhow::anyhow!(
                "Missing 'wasm' field in Kubernetes object '{}'",
                name.clone()
            )
        })?;
        let wasm = match from_value::<WasmSource>(wasm_value.clone()) {
            Ok(source) => Some(source),
            Err(e) => {
                eprintln!("Failed to deserialize WasmSource: {}", e);
                None
            }
        };
        let wasm = wasm.ok_or_else(|| {
            anyhow::anyhow!(
                "Missing or invalid 'wasm' field in Kubernetes object '{}'",
                name.clone()
            )
        })?;

        let generation = k8s_object.metadata.generation;

        let uid = k8s_object.uid().ok_or_else(|| {
            anyhow::anyhow!("Missing UID in Kubernetes object '{}'", name.clone())
        })?;

        let env: Vec<EnvironmentVariable> = k8s_object
            .data
            .pointer("/spec/env")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|env_entry| {
                        let name = env_entry.get("name")?.as_str()?;
                        let value = env_entry.get("value")?.as_str()?;
                        Some(EnvironmentVariable {
                            name: name.to_string(),
                            value: value.to_string(),
                        })
                    })
                    .collect::<Vec<EnvironmentVariable>>()
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Missing or invalid 'env' field in Kubernetes object '{}'",
                    name.clone()
                )
            })?;

        let args: Vec<String> = k8s_object
            .data
            .pointer("/spec/args")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|arg| arg.as_str().map(|s| s.to_string()))
                    .collect::<Vec<String>>()
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Missing or invalid 'args' field in Kubernetes object '{}'",
                    name.clone()
                )
            })?;

        Ok(Self {
            name,
            wasm,
            generation,
            uid,
            env,
            args,
        })
    }
}
