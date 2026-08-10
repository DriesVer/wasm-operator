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

impl State {
    /// Common runner executed for every WIT host call
    pub fn execute_host_function<F, Fut, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        // Update the last active timestamp for the operator (before to account for long blocks)
        self.operator.update_last_active();

        // Do async task
        let result =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f()));

        // Update the last active timestamp for the operator
        self.operator.update_last_active();

        result
    }
}
