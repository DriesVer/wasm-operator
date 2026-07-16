use chrono::{DateTime, Utc};
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::sync::{Arc, Mutex, Once};
use thiserror::Error;
use tracing::{error, info};

use kube::api::{Api, Patch, PatchParams, ResourceExt};
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::{Client, CustomResource, Resource};

use futures::executor::{LocalPool, LocalSpawner};
use futures::task::SpawnExt;
use send_wrapper::SendWrapper;
use std::cell::RefCell;
use std::mem;
use std::ops::Deref;
use std::rc::Rc;

struct Component;

static mut SPAWNER: Option<LocalSpawner> = None;

pub fn get_mut_executor() -> Rc<RefCell<LocalPool>> {
    // Initialize it to a null value
    static mut SINGLETON: *const Rc<RefCell<LocalPool>> = 0 as *const Rc<RefCell<LocalPool>>;
    static ONCE: Once = Once::new();

    unsafe {
        ONCE.call_once(|| {
            // Make it
            let singleton = Rc::new(RefCell::new(LocalPool::new()));

            // Put it in the heap so it can outlive this call
            SINGLETON = mem::transmute::<
                Box<Rc<RefCell<LocalPool>>>,
                *const Rc<RefCell<LocalPool>>,
            >(Box::new(singleton));
        });

        let pool = (*SINGLETON).clone();
        SPAWNER = Some(pool.borrow_mut().spawner());

        pool
    }
}

impl wasip2::exports::cli::run::Guest for Component {
    fn run() -> Result<(), ()> {
        // WASI p3 try
        // main_async().await.map_err(|_| ())?;

        // WASI p2 try
        // let rt = tokio::runtime::Builder::new_current_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap();

        // //let local = tokio::task::LocalSet::new();
        // rt.block_on(main_async()).map_err(|_| ())?;

        let exec = get_mut_executor();

        let local_future = main_async();
        let send_safe_future = SendWrapper::new(local_future);

        // Start the main
        exec.deref()
            .borrow_mut()
            .spawner()
            .spawn(send_safe_future)
            .unwrap();
        // Give a little push to the executor
        exec.deref().borrow_mut().run_until_stalled();
        Ok(())
    }
}

wasip2::cli::command::export!(Component);

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
        info!(name, "Resource is being deleted, skipping reconciliation");
        return Ok(Action::await_change());
    }

    if let Some(ref status) = resource.status {
        if status.observed_generation == resource.metadata.generation {
            return Ok(Action::await_change());
        }
    }

    info!(name, "Reconciling resource");

    let current_counter = {
        let mut counter = ctx.counter.lock().unwrap();
        *counter += 3;
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

fn error_policy(_resource: Arc<TestResource>, error: &Error, _ctx: Arc<Data>) -> Action {
    error!(%error, "Reconciliation failed");
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
    tracing_subscriber::fmt::init();

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

    info!(namespace, "Starting TestResource controller loop...");

    Controller::new(api, Config::default())
        //.shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconcile success: {:?}", o),
                Err(e) => error!("Reconcile error: {:?}", e),
            }
        })
        .await;
}
