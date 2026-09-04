//! # Main Module
//!
//! This module serves as the entry point for the parent controller. It is responsible for
//! parsing command-line arguments, setting up logging, initializing global singletons
//! (Kubernetes service, Wasmtime engine), and starting the MainController to orchestrate
//! child Wasm operators.

mod host;
mod kubernetes;
mod prediction;
mod runtime;
mod shutdown;

use std::env;

use kubernetes::KubernetesService;
use runtime::MainController;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::runtime::wasmengine::WasmEngineSingleton;
use crate::shutdown::wait_for_shutdown;

/// Main entry point for the parent controller.
fn main() -> anyhow::Result<()> {
    let debug = parse_args()?;

    setup_logging(debug);

    // Create a tokio runtime to run the async code
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let worker_threads = cores.max(2); // Ensure at least 2 async threads for the runtime

    tracing::info!(
        "Detected {} CPU cores, using {} worker threads for the async runtime.",
        cores,
        worker_threads
    );

    // Maybe add thread_keep_alive to reduce the overhead of construction of construction of new OS threads for spawn_blocking
    let global_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;

    // Initialize global singletons before starting the main async block
    global_rt.block_on(async {
        KubernetesService::global().await?;
        wasmtime::Engine::global().await?;
        Ok::<(), anyhow::Error>(())
    })?;

    global_rt.spawn(async move {
        wait_for_shutdown().await;
    });

    global_rt.block_on(async {
        let main_controller = MainController::new();
        main_controller.start().await?;
        Ok::<(), anyhow::Error>(())
    })?;

    info!("All components finished successfully.");
    info!("Exiting...");

    Ok(())
}

/// Parameters for configuring logging levels/
struct LoggingParams {
    base_level: String,
    http_level: Option<String>,
    kube_level: Option<String>,
}

/// Initializes the global logging subscriber.
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

/// Parses command-line arguments to determine logging levels.
fn parse_args() -> anyhow::Result<LoggingParams> {
    let args: Vec<String> = env::args().collect();
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
        } else {
            anyhow::bail!("Unexpected argument: {}", arg);
        }
    }

    Ok(logging_params)
}
