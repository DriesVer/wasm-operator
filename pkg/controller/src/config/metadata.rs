//! # Metadata Module
//!
//! This module defines the data structures for representing WebAssembly (Wasm) component
//! metadata, including environment variables and command-line arguments. It also provides
//! functionality for loading this metadata from YAML configuration files.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use schemars::JsonSchema;

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
pub struct EnvironmentVariable {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WasmComponentMetadata {
    pub name: String,
    pub wasm: PathBuf,
    #[serde(default)]
    pub env: Vec<EnvironmentVariable>,
    #[serde(default)]
    pub args: Vec<String>,
}

impl WasmComponentMetadata {
    /// Load component metadata from a YAML file
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
}
