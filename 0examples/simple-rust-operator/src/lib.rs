use chrono::{DateTime, Utc};
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::{error, info};

use kube::api::{Api, Patch, PatchParams, ResourceExt};
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::{Client, CustomResource, Resource};

// --- 1. Entrypoint for the WasmOperator ---
struct Component;

use send_wrapper::SendWrapper;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Runtime;
use tokio::task::LocalSet;

static RUNTIME: OnceLock<Mutex<SendWrapper<(Runtime, LocalSet)>>> =
    OnceLock::new();

fn get_runtime() -> &'static Mutex<SendWrapper<(Runtime, LocalSet)>> {
    RUNTIME.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = LocalSet::new();
        Mutex::new(SendWrapper::new((rt, local)))
    })
}

static IS_MAIN_RUNNING: AtomicBool = AtomicBool::new(false);

// Do we need a localset? Allesinds geen probleem want we zijn in een single-threaded runtime.
impl wasip2::exports::cli::run::Guest for Component {
    fn run() -> Result<(), ()> {
        let _ = tracing_subscriber::fmt()
            .with_writer(CustomStdout)
            .try_init();

        let rt_mutex = get_runtime();
        let mut rt_guard = rt_mutex.lock().unwrap();
        let (rt, local) = &mut **rt_guard;

        if !IS_MAIN_RUNNING.swap(true, Ordering::SeqCst) {
            // Start the main async function in the localset if it hasn't been started yet
            local.spawn_local(main_async());
        }
        // Drive the async runtime to completion for a single tick
        local.block_on(rt, async {
            tokio::task::yield_now().await;
        });
        Ok(())
    }
}

wasip2::cli::command::export!(Component);

// --- 2. Custom Stdout Writer for Tracing ---
// Custom stdout writer that fetchs the stdout stream from the WASI environment and writes to it
// Needed because stdout stream changes on each wake up of the operator
struct CustomStdout;

impl std::io::Write for CustomStdout {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let stdout = wasip2::cli::stdout::get_stdout();
        stdout
            .blocking_write_and_flush(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CustomStdout {
    type Writer = CustomStdout;
    fn make_writer(&'a self) -> Self::Writer {
        CustomStdout
    }
}

// --- 3. Custom Resource Definition ---
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "test.dev",
    version = "v1",
    kind = "TestResource",
    namespaced,
    status = "TestResourceStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct TestResourceSpec {
    pub factor: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TestResourceStatus {
    pub outcome: i64,
    pub observed_generation: Option<i64>,
    pub last_updated: Option<DateTime<Utc>>,
}

// --- 4. Controller Context & Error Handling ---
struct Data {
    client: Client,
    counter: Arc<Mutex<i64>>,
}

#[derive(Debug, Error)]
enum Error {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Resource is missing a namespace")]
    NamespaceRequired,
}

// --- 5. Reconciliation Loop ---
async fn reconcile(resource: Arc<TestResource>, ctx: Arc<Data>) -> Result<Action, Error> {
    let namespace = resource.namespace().ok_or(Error::NamespaceRequired)?;
    let api: Api<TestResource> = Api::namespaced(ctx.client.clone(), &namespace);
    let name = resource.name_any();

    if resource.meta().deletion_timestamp.is_some() {
        info!(
            "Resource {} is being deleted, skipping reconciliation",
            name
        );
        return Ok(Action::await_change());
    }

    if let Some(ref status) = resource.status {
        if status.observed_generation == resource.metadata.generation {
            return Ok(Action::await_change());
        }
    }

    info!("Reconciling resource {}: {:?}", &name, resource);

    let current_counter = {
        let mut counter = ctx.counter.lock().unwrap();
        *counter += 1;
        *counter
    };

    let computed_outcome = resource.spec.factor * current_counter;

    let new_status = TestResourceStatus {
        outcome: computed_outcome,
        observed_generation: resource.metadata.generation,
        last_updated: Some(Utc::now()),
    };

    let patch = serde_json::json!({
        "status": new_status
    });

    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;

    Ok(Action::await_change())
}

fn error_policy(resource: Arc<TestResource>, error: &Error, _ctx: Arc<Data>) -> Action {
    error!(
        "Reconciliation failed for resource '{}': {:?}",
        resource.name_any(),
        error
    );
    Action::requeue(std::time::Duration::from_secs(5))
}


// --- 6. Main Async Function ---
async fn main_async() {
    let client = Client::try_default().await.unwrap();
    let namespace =
        std::env::var("TESTRESOURCE_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    let api: Api<TestResource> = Api::namespaced(client.clone(), &namespace);

    let context = Arc::new(Data {
        client,
        counter: Arc::new(Mutex::new(0)),
    });
        
    info!("Starting TestResource controller loop...");

    Controller::new(api, Config::default())
        //.shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|_| async {})
        .await;
}
