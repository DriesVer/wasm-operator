//! # Wasm Instance Module
//!
//! This module defines the `WasmInstance` struct, which encapsulates the execution
//! of a single WebAssembly (Wasm) component. It handles the loading, instantiation,
//! and execution of Wasm modules, providing them with access to host functionalities
//! like Kubernetes API interactions.

use std::sync::Arc;

use anyhow::Result;
use tracing::{debug, info};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::p2::{WasiCtxBuilder, add_to_linker_async};

use crate::config::metadata::WasmComponentMetadata;
use crate::host::api::bindings;
use crate::host::state::State;
use crate::kubernetes::KubernetesService;

pub struct WasmInstance {
    engine: Engine,
    kubernetes_service: Arc<KubernetesService>,
    metadata: WasmComponentMetadata,
}

impl WasmInstance {
    pub fn new(
        engine: Engine,
        kubernetes_service: Arc<KubernetesService>,
        metadata: WasmComponentMetadata,
    ) -> Self {
        Self {
            engine,
            kubernetes_service,
            metadata,
        }
    }

    pub async fn load(self) -> Result<(bindings::KubeOperator, Store<State>)> {
        info!("Loading component: {}", self.metadata.name);

        debug!(
            "Loading component from file: {}",
            self.metadata.wasm.display()
        );
        let component = Component::from_file(&self.engine, &self.metadata.wasm).map_err(|e| {
            anyhow::anyhow!("Failed to load component '{}': {}", self.metadata.name, e)
        })?;
        debug!("Component loaded successfully: {}", self.metadata.name);

        let wasi_ctx = WasiCtxBuilder::new()
            .inherit_stdio()
            .args(&self.metadata.args)
            .envs(
                &self
                    .metadata
                    .env
                    .iter()
                    .map(|e| (e.name.as_str(), e.value.as_str()))
                    .collect::<Vec<_>>(),
            )
            .build();

        let state = State {
            wasi_ctx,
            kubernetes_service: self.kubernetes_service.clone(),
            resources: Default::default(),
        };
        let mut store = Store::new(&self.engine, state);

        let mut linker = Linker::new(&self.engine);
        add_to_linker_async(&mut linker)?;

        bindings::KubeOperator::add_to_linker::<_, HasSelf<_>>(&mut linker, |ctx: &mut State| ctx)?;

        debug!("Instantiating component: {}", self.metadata.name);
        let operator =
            bindings::KubeOperator::instantiate_async(&mut store, &component, &linker).await?;
        debug!(
            "Component instantiated successfully: {}",
            self.metadata.name
        );

        Ok((operator, store))
    }
}
