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

use std::{env, path::PathBuf};

use kubernetes::KubernetesService;
use runtime::MainController;
use tracing::{debug, info};
use tracing_subscriber::FmtSubscriber;

use crate::runtime::WasmEngineSingleton;

fn main() -> anyhow::Result<()> {
    let (config_path, debug) = parse_args()?;

    setup_logging(debug);
    debug!("Config path: {}", config_path.display());

    // TODO: maybe go to a non local runtime
    // Create a tokio runtime to run the async code
    let global_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1) // 1 worker thread for checking idle + watching CRs, in case of heavier idling logic we can increase this to 2 or more
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();

    // Initialize global singletons before starting the main async block
    global_rt.block_on(async {
        KubernetesService::global().await?;
        wasmtime::Engine::global().await?;
        Ok::<(), anyhow::Error>(())
    })?;

    local.block_on(&global_rt, async {
        let main_controller = MainController::new();
        main_controller.start().await?;
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
