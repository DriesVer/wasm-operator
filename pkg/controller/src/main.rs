//! # Main Module
//!
//! This module serves as the entry point for the Wasm Operator. It is responsible for
//! parsing command-line arguments, setting up logging, loading the WASM component
//! configuration, and orchestrating the Kubernetes service and the WASM runtime
//! to execute the Wasm modules.

mod config;
mod host;
mod kubernetes;
mod runtime;

use std::sync::Arc;
use std::{env, path::PathBuf};

use config::metadata::WasmComponentMetadata;
use kubernetes::KubernetesService;
use runtime::WasmRuntime;
use tracing::{debug, info};
use tracing_subscriber::FmtSubscriber;

fn main() -> anyhow::Result<()> {
    let (config_path, debug) = parse_args()?;

    setup_logging(debug);
    let components_metadata = WasmComponentMetadata::load_from_yaml(&config_path)?;

    info!("Loaded {} WASM component(s):", components_metadata.len());
    for metadata in &components_metadata {
        info!(" - {}", metadata.name);
    }

    // Create a tokio runtime and run the async code
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&tokio_runtime, async {
        let k8s_service = Arc::new(KubernetesService::new().await?);
        let wasm_runtime = Arc::new(WasmRuntime::new(k8s_service.clone())?);
        // The future inside block_on needs to return a Result.
        // After run_components (which returns a Result) is awaited, we wrap the
        // successful `()` value in an `Ok` to match the expected return type.
        wasm_runtime.run_components(components_metadata).await?;
        Ok::<(), anyhow::Error>(())
    })?;

    info!("All components finished successfully.");
    info!("Exiting...");

    Ok(())
}

fn setup_logging(debug: bool) {
    let level = if debug {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing::subscriber::set_global_default(
        FmtSubscriber::builder().with_max_level(level).finish(),
    )
        .expect("setting default subscriber failed");

    if debug {
        debug!("Debug logging enabled.");
    } else {
        info!("Running in normal mode, debug logging is disabled.");
    }
}

fn parse_args() -> anyhow::Result<(PathBuf, bool)> {
    let args: Vec<String> = env::args().collect();
    let mut debug = false;
    let mut config_path: Option<PathBuf> = None;

    for arg in &args[1..] {
        if arg == "--debug" {
            debug = true;
        } else if config_path.is_none() {
            config_path = Some(PathBuf::from(arg));
        } else {
            anyhow::bail!("Unexpected argument: {}", arg);
        }
    }

    let config_path = config_path.ok_or_else(|| {
        anyhow::anyhow!("Usage: {} [--debug] <path_to_wasm_config.yaml>", args[0])
    })?;

    Ok((config_path, debug))
}
