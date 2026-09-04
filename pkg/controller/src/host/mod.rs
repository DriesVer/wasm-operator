//! # Host Module
//!
//! This module defines the host-side functionality and state management for the Wasmtime
//! environment. It provides the necessary WebAssembly System Interface (WASI) and custom
//! bindings for Wasm modules to interact with the host, including Kubernetes API access.

pub mod api;
pub mod helper;
pub mod state;
pub mod watch_stream_handler;
pub mod wit;
