//! # Main Module
//!
//! This module serves as the entry point for the Wasm Operator. It is responsible for
//! parsing command-line arguments, setting up logging, loading the WASM component
//! configuration, and orchestrating the Kubernetes service and the WASM runtime
//! to execute the Wasm modules.

mod host;
mod kubernetes;
mod prediction;
mod runtime;

use std::{env, path::PathBuf};

use kubernetes::KubernetesService;
use runtime::MainController;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::runtime::wasmengine::WasmEngineSingleton;

fn main() -> anyhow::Result<()> {
    let (config_path, debug) = parse_args()?;

    setup_logging(debug);

    // TODO: maybe go to a non local runtime
    // Create a tokio runtime to run the async code
    let global_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3) // 3 threads: 2 global, one local
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();

    // Initialize global singletons before starting the main async block
    global_rt.block_on(async {
        KubernetesService::global().await?;
        wasmtime::Engine::global().await?;
        Ok::<(), anyhow::Error>(())
    })?;

    let shutdown_token = CancellationToken::new();
    let shutdown_token_clone = shutdown_token.clone();
    global_rt.spawn(async move {
        wait_for_shutdown(shutdown_token_clone).await;
    });

    local.block_on(&global_rt, async {
        let main_controller = MainController::new(shutdown_token);
        main_controller.start().await?;
        Ok::<(), anyhow::Error>(())
    })?;

    info!("All components finished successfully.");
    info!("Exiting...");

    Ok(())
}

async fn wait_for_shutdown(shutdown_token: CancellationToken) {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating shutdown...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM from Kubernetes, initiating shutdown...");
        }
    }
    shutdown_token.cancel();
}

struct LoggingParams {
    base_level: String,
    http_level: Option<String>,
    kube_level: Option<String>,
}

fn setup_logging(params: LoggingParams) {
    let mut filter = EnvFilter::new(&params.base_level);

    if let Some(http_level) = &params.http_level {
        filter = filter
            .add_directive(format!("hyper={}", http_level).parse().unwrap())
            .add_directive(format!("hyper_util={}", http_level).parse().unwrap())
            .add_directive(format!("tower={}", http_level).parse().unwrap());
    }

    if let Some(kube_level) = &params.kube_level {
        filter = filter.add_directive(format!("kube={}", kube_level).parse().unwrap());
    }

    if let Ok(env_val) = std::env::var("RUST_LOG") {
        if let Ok(env_filter) = EnvFilter::try_new(env_val) {
            filter = env_filter;
        }
    }

    tracing::subscriber::set_global_default(
        FmtSubscriber::builder().with_env_filter(filter).finish(),
    )
    .expect("setting default subscriber failed");

    info!(
        "Logging initialized with base level: {}",
        &params.base_level
    );
    if let Some(http_level) = &params.http_level {
        info!("HTTP logging level set to: {}", http_level);
    }
    if let Some(kube_level) = &params.kube_level {
        info!("Kubernetes logging level set to: {}", kube_level);
    }
}

fn parse_args() -> anyhow::Result<(PathBuf, LoggingParams)> {
    let args: Vec<String> = env::args().collect();
    let mut config_path: Option<PathBuf> = None;
    let mut logging_params = LoggingParams {
        base_level: "info".to_string(),
        http_level: None,
        kube_level: None,
    };

    for arg in &args[1..] {
        if arg == "--debug" {
            logging_params.base_level = "debug".to_string();
        } else if let Some(val) = arg.strip_prefix("--http_log=") {
            logging_params.http_level = Some(val.to_string());
        } else if let Some(val) = arg.strip_prefix("--kube_log=") {
            logging_params.kube_level = Some(val.to_string());
        } else if config_path.is_none() {
            config_path = Some(PathBuf::from(arg));
        } else {
            anyhow::bail!("Unexpected argument: {}", arg);
        }
    }

    let config_path = config_path.ok_or_else(|| {
        anyhow::anyhow!("Usage: {} [--debug] <path_to_wasm_config.yaml>", args[0])
    })?;

    Ok((config_path, logging_params))
}
