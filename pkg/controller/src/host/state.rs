//! # Host State Module
//!
//! This module defines the `State` struct, which holds the necessary context and resources
//! for a WebAssembly (Wasm) component instance. It provides access to WASI (WebAssembly
//! System Interface) functionalities, the Kubernetes service, and a resource table for
//! managing host-defined resources, enabling Wasm modules to interact with the host
//! environment.

use std::sync::Arc;

use crate::runtime::wasmoperator::WasmOperatorRuntime;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

pub struct State {
    pub operator: Arc<WasmOperatorRuntime>,
    pub wasi_ctx: WasiCtx,
    pub resources: ResourceTable,
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resources,
        }
    }
}
