use std::sync::LazyLock;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::info;

static SHUTDOWN_TOKEN: LazyLock<CancellationToken> = LazyLock::new(CancellationToken::new);

#[inline]
/// Retrieves the global cancellation token for graceful shutdown.
pub fn shutdown_token() -> CancellationToken {
    SHUTDOWN_TOKEN.clone()
}

/// Waits for a SIGINT or SIGTERM signal to initiate shutdown.
pub async fn wait_for_shutdown() {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating shutdown...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM from Kubernetes, initiating shutdown...");
        }
    }
    SHUTDOWN_TOKEN.cancel();
}
