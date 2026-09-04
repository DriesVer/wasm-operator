//! # Host Module
//!
//! This module defines the host-side functionalities and state management for the
//! WebAssembly (Wasm) operator. It provides the necessary interfaces and structures
//! for Wasm modules to interact with the host environment, including Kubernetes API
//! access and resource management.

pub mod api;
pub mod helper;
pub mod state;
pub mod watch_stream_handler;
pub mod wit;
