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

struct Component;

// TODO move to imports instead of using the long tokio::runtime::Builder::... etc
// TODO maybe move to a seperate crate for the wasm-operator runtime, so that it can be reused in other operators
use send_wrapper::SendWrapper;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

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

// --- 1. Custom Resource Definition ---
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

// --- 2. Controller Context & Error Handling ---
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

// --- 3. Reconciliation Loop ---
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

// // --- 4. Mock WASIp3 Task support for Tokio Integration ---
// #[repr(C)]
// pub struct wasip3_task {
//     pub version: u32,
//     pub ptr: *mut c_void,
//     pub waitable_register: unsafe extern "C" fn(
//         ptr: *mut c_void,
//         waitable: u32,
//         callback: unsafe extern "C" fn(callback_ptr: *mut c_void, code: u32),
//         callback_ptr: *mut c_void,
//     ) -> *mut c_void,
//     pub waitable_unregister: unsafe extern "C" fn(ptr: *mut c_void, waitable: u32) -> *mut c_void,
// }

// unsafe extern "C" {
//     fn wasip3_task_set(ptr: *mut wasip3_task) -> *mut wasip3_task;
// }

// #[link(wasm_import_module = "$root")]
// unsafe extern "C" {
//     #[link_name = "[waitable-set-new]"]
//     fn waitable_set_new() -> u32;
//     #[link_name = "[waitable-join]"]
//     fn waitable_join(waitable: u32, set: u32);
//     #[link_name = "[waitable-set-poll]"]
//     fn waitable_set_poll(set: u32, payload: *mut [u32; 2]) -> u32;
//     #[link_name = "[waitable-set-wait]"]
//     fn waitable_set_wait(set: u32, payload: *mut [u32; 2]) -> u32;
// }

// struct MockTaskRegistry {
//     waitable_set: Option<u32>,
//     callbacks: BTreeMap<u32, (unsafe extern "C" fn(*mut c_void, u32), *mut c_void)>,
// }

// unsafe impl Send for MockTaskRegistry {}
// unsafe impl Sync for MockTaskRegistry {}

// static REGISTRY: Mutex<MockTaskRegistry> = Mutex::new(MockTaskRegistry {
//     waitable_set: None,
//     callbacks: BTreeMap::new(),
// });

// unsafe extern "C" fn mock_waitable_register(
//     _ptr: *mut c_void,
//     waitable: u32,
//     callback: unsafe extern "C" fn(*mut c_void, u32),
//     callback_ptr: *mut c_void,
// ) -> *mut c_void {
//     eprintln!("[WASM-OP] mock_waitable_register: {}", waitable);
//     let mut reg = REGISTRY.lock().unwrap();
//     if reg.waitable_set.is_none() {
//         reg.waitable_set = Some(unsafe { waitable_set_new() });
//     }
//     let set = reg.waitable_set.unwrap();
//     unsafe {
//         waitable_join(waitable, set);
//     }
//     let prev = reg.callbacks.insert(waitable, (callback, callback_ptr));
//     match prev {
//         Some((_, prev_ptr)) => prev_ptr,
//         None => std::ptr::null_mut(),
//     }
// }

// unsafe extern "C" fn mock_waitable_unregister(_ptr: *mut c_void, waitable: u32) -> *mut c_void {
//     eprintln!("[WASM-OP] mock_waitable_unregister: {}", waitable);
//     let mut reg = REGISTRY.lock().unwrap();
//     unsafe {
//         waitable_join(waitable, 0); // 0 means remove from all sets
//     }
//     let prev = reg.callbacks.remove(&waitable);
//     match prev {
//         Some((_, prev_ptr)) => prev_ptr,
//         None => std::ptr::null_mut(),
//     }
// }

// static mut MOCK_TASK: wasip3_task = wasip3_task {
//     version: 1,
//     ptr: std::ptr::null_mut(),
//     waitable_register: mock_waitable_register,
//     waitable_unregister: mock_waitable_unregister,
// };

// --- 5. Main Entrypoint ---
fn main() {
    // unsafe {
    //     wasip3_task_set(&raw mut MOCK_TASK);
    // }

    // Start the async runtime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, main_async());
}

async fn main_async() {
    // // Spawn the background poll loop

    // tokio::task::spawn_local(async {
    //     eprintln!("[WASM-OP] Background poll loop spawned!");
    //     loop {
    //         let mut events_to_run = Vec::new();
    //         {
    //             let mut reg = REGISTRY.lock().unwrap();
    //             if let Some(set) = reg.waitable_set {
    //                 let mut payload = [0; 2];
    //                 let event0 = unsafe { waitable_set_poll(set, &mut payload) };
    //                 if event0 != 0 {
    //                     // EVENT_NONE is 0
    //                     eprintln!(
    //                         "[WASM-OP] waitable completed: {}, code: {}",
    //                         payload[0], payload[1]
    //                     );
    //                     if let Some((callback, callback_ptr)) = reg.callbacks.remove(&payload[0]) {
    //                         events_to_run.push((callback, callback_ptr, payload[1]));
    //                     }
    //                 }
    //             }
    //         }
    //         for (callback, callback_ptr, code) in events_to_run {
    //             unsafe {
    //                 callback(callback_ptr, code);
    //             }
    //         }
    //         tokio::task::yield_now().await;
    //     }
    // });

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
