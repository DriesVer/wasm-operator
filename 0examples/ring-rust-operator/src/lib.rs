use chrono::{DateTime, Utc};
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info};

use kube::api::{Api, Patch, PatchParams, ResourceExt};
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::{Client, CustomResource};
use send_wrapper::SendWrapper;

struct Component;

static RUNTIME: OnceLock<Mutex<SendWrapper<(tokio::runtime::Runtime, tokio::task::LocalSet)>>> =
    OnceLock::new();

fn get_runtime() -> &'static Mutex<SendWrapper<(tokio::runtime::Runtime, tokio::task::LocalSet)>> {
    RUNTIME.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        Mutex::new(SendWrapper::new((rt, local)))
    })
}

static IS_MAIN_RUNNING: AtomicBool = AtomicBool::new(false);

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

// Custom stdout writer that fetches the stdout stream from the WASI environment
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

// Include generated code bloat from build.rs if DUMMY_CODE_SIZE is set
include!(concat!(env!("OUT_DIR"), "/wasm_bloat.rs"));

#[unsafe(no_mangle)]
pub extern "C" fn touch_code() -> usize {
    let result = touch_bloat_code(42);
    result as usize
}

#[derive(Debug, Error)]
enum Error {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),
}

/// Custom Resource definition mirroring the RingTestResource spec
#[derive(CustomResource, Debug, Serialize, Deserialize, Default, Clone, JsonSchema)]
#[kube(
    kind = "RingTestResource",
    group = "test.dev",
    version = "v1",
    plural = "ring-testresources",
    singular = "ring-testresource",
    shortname = "ringtr",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct RingTestResourceSpec {
    pub nonce: i64,
    pub updated_at: Option<DateTime<Utc>>,
    pub reconcile_count: Option<i64>,
}

/// Context data shared across reconciliation calls
struct Data {
    client: Client,
    out_namespace: String,
    counter: Arc<AtomicI64>,
    huge_mem_alloc: Arc<Vec<u32>>,
}

/// Controller error policy fallback
fn error_policy(resource: Arc<RingTestResource>, error: &Error, _ctx: Arc<Data>) -> Action {
    error!(
        "Reconciliation failed for resource '{}': {:?}",
        resource.name_any(),
        error
    );
    Action::requeue(Duration::from_secs(1))
}

async fn main_async() {
    info!("Starting RingTestResource controller");

    let client = Client::try_default()
        .await
        .expect("Could not create Kubernetes client");

    let in_namespace = env::var("IN_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    let out_namespace = env::var("OUT_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    // Memory pre-allocation for benchmarking purposes
    let heap_mem_size: usize = env::var("HEAP_MEM_SIZE")
        .unwrap_or_default()
        .parse()
        .unwrap_or(0);
    let heap_mem_size = (heap_mem_size / 4) * 1024 * 1024; // Convert MiB to u32 count

    let now: i64 = Utc::now().timestamp_millis();
    let timecode_u32: u32 = (now & 0xFFFF_FFFF) as u32;

    let mut huge_mem_alloc = Vec::with_capacity(heap_mem_size);
    for i in 0..heap_mem_size {
        huge_mem_alloc.push(timecode_u32 + i as u32);
    }

    let huge_mem_alloc = Arc::new(huge_mem_alloc);
    let counter = Arc::new(AtomicI64::new(0));

    let in_resources: Api<RingTestResource> = Api::namespaced(client.clone(), &in_namespace);

    let context = Arc::new(Data {
        client,
        out_namespace,
        counter,
        huge_mem_alloc,
    });

    Controller::new(in_resources, Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => debug!("Reconciled {:?}", obj),
                Err(e) => debug!("Reconcile error: {:?}", e),
            }
        })
        .await;
}

async fn reconcile(resource: Arc<RingTestResource>, ctx: Arc<Data>) -> Result<Action, Error> {
    std::thread::sleep(Duration::from_millis(1000)); // Simulate some processing time

    // Do not start reconciling if nonce is 0 (initial state)
    if resource.spec.nonce == 0 {
        return Ok(Action::await_change());
    }

    let client = ctx.client.clone();
    let out_namespace = ctx.out_namespace.clone();
    let name = resource.name_any();

    let out_resources: Api<RingTestResource> = Api::namespaced(client, &out_namespace);

    match out_resources.get(&name).await {
        Ok(mut out_resource) => {
            // Early exit if resource is already up-to-date
            if out_resource.spec.nonce == resource.spec.nonce + 1 {
                return Ok(Action::await_change());
            }

            // Increment execution counter
            let new_counter = ctx.counter.fetch_add(1, Ordering::SeqCst) + 1;

            // Update output resource spec
            out_resource.spec.nonce = resource.spec.nonce + 1;
            out_resource.spec.updated_at = Some(Utc::now());
            out_resource.spec.reconcile_count = Some(new_counter);

            // Commit update back to Kubernetes cluster
            out_resource.metadata.managed_fields = None;
            let pp = PatchParams::apply("RingTestResource").force();
            if let Err(e) = out_resources
                .patch(&name, &pp, &Patch::Apply(&out_resource))
                .await
            {
                error!(
                    "Failed to patch target resource in namespace '{}': {:?}",
                    out_namespace, e
                );
            }
        }
        Err(e) => {
            error!(
                "Failed to fetch target resource in namespace '{}': {:?}",
                out_namespace, e
            );
            return Err(Error::KubeError(e));
        }
    }

    Ok(Action::await_change())
}
