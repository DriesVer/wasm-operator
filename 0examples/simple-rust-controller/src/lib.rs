use crate::local::operator::kubernetes;
use crate::local::operator::kubernetes::LogLevel;
use bincode;
use serde::{Deserialize, Serialize};

use std::sync::{Mutex, OnceLock};

wit_bindgen::generate!(
    {
        path: "../../pkg/controller/wit",
        world: "child-world",
    }
);

// Artificially inflate the size of the code base
#[unsafe(no_mangle)]
//static BIG_DATA: &[u8; 1048576] = include_bytes!("1MB_of_junk.bin");
static BIG_DATA: &[u8; 5242880] = include_bytes!("5MB_of_junk.bin");
#[unsafe(no_mangle)]
pub extern "C" fn check_data() -> usize {
    // Doing a simple calculation on the data prevents
    // the compiler from optimizing it away.
    BIG_DATA.as_ptr() as usize
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestResourceSpec {
    nonce: i64,
    updated_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    name: String,
    namespace: Option<String>,
    resource_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestResource {
    api_version: String,
    kind: String,
    metadata: ObjectMeta,
    spec: TestResourceSpec,
}

static MEM_ALLOC: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();

fn get_mem_alloc() -> &'static Mutex<Vec<u32>> {
    MEM_ALLOC.get_or_init(|| Mutex::new(Vec::new()))
}

struct SimpleOperator;

impl Guest for SimpleOperator {
    fn get_watch_requests() -> Vec<WatchRequest> {
        // TODO: get this from the environment
        const NAMESPACE: &str = "default";

        vec![WatchRequest {
            kind: "TestResource".to_string(),
            namespace: NAMESPACE.to_string(),
        }]
    }

    fn serialize() -> Vec<u8> {
        kubernetes::log(LogLevel::Info, "Rust operator serialize called");
        let data = get_mem_alloc().lock().unwrap();
        bincode::serialize(&*data).unwrap_or_else(|e| {
            kubernetes::log(LogLevel::Error, &format!("Failed to serialize data: {}", e));
            Vec::new()
        })
    }

    fn deserialize(_bytes: Vec<u8>) {
        kubernetes::log(LogLevel::Info, "Rust operator deserialize called");
        let decoded = bincode::deserialize::<Vec<u32>>(&_bytes);
        match decoded {
            Ok(vec) => {
                kubernetes::log(
                    LogLevel::Info,
                    &format!("Successfully deserialized data with {} elements", vec.len()),
                );
                let mut data = get_mem_alloc().lock().unwrap();
                *data = vec;
            }
            Err(e) => {
                kubernetes::log(
                    LogLevel::Error,
                    &format!("Failed to deserialize data: {}", e),
                );
            }
        }
    }

    fn reconcile(req: ReconcileRequest) -> ReconcileResult {
        let mut resource: TestResource = match serde_json::from_str(&req.resource_json) {
            Ok(r) => r,
            Err(e) => {
                kubernetes::log(LogLevel::Error, &format!("Failed to parse resource: {}", e));
                return ReconcileResult::Error(format!("Failed to parse resource: {}", e));
            }
        };

        let mut data = get_mem_alloc().lock().unwrap();
        let first_element = data.first().cloned().unwrap_or(0);
        if first_element == 0 {
            let now: i64 = chrono::Utc::now().timestamp_millis();
            let timecode_u32: u32 = (now & 0xFFFF_FFFF) as u32;
            kubernetes::log(
                LogLevel::Info,
                &format!(
                    "In-memory data is empty, filling it with new values. First value is {}",
                    timecode_u32
                ),
            );
            const HEAP_MEM_SIZE: usize = 10 * 1024 * 1024; // 10 million u32s, ~40MB
            let mut huge_mem_alloc = Vec::<u32>::with_capacity(HEAP_MEM_SIZE);
            for i in 0..HEAP_MEM_SIZE {
                huge_mem_alloc.push(timecode_u32 + i as u32);
            }
            //let mut data = get_mem_alloc().lock().unwrap();
            *data = huge_mem_alloc.clone();
        } else {
            kubernetes::log(
                LogLevel::Info,
                &format!("First element of in-memory data: {}", first_element),
            );
        }

        let namespace = resource
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let needs_change = resource.spec.nonce == 0 || resource.spec.updated_at.is_none();

        if !needs_change {
            return ReconcileResult::Ok;
        }

        let all_resources = match kubernetes::list_resources("TestResource", &namespace) {
            Ok(resources) => resources,
            Err(e) => {
                kubernetes::log(
                    LogLevel::Error,
                    &format!("Failed to list resources in namespace {}: {}", namespace, e),
                );
                Vec::new()
            }
        };
        let max_nonce = all_resources
            .iter()
            .filter_map(|resource_json| serde_json::from_str::<TestResource>(resource_json).ok())
            .map(|r| r.spec.nonce)
            .max()
            .unwrap_or(0);

        resource.spec.updated_at = Some(chrono::Utc::now().to_rfc3339());
        resource.spec.nonce = max_nonce + 1;

        if let Ok(updated_json) = serde_json::to_string(&resource) {
            let result = kubernetes::update_resource(
                "TestResource",
                &resource.metadata.name,
                &namespace,
                &updated_json,
                true,
            );
            if let Err(e) = result {
                kubernetes::log(
                    LogLevel::Error,
                    &format!(
                        "Failed to update resource {} in namespace {}: {}",
                        resource.metadata.name, namespace, e
                    ),
                );
                return ReconcileResult::Error(format!(
                    "Failed to update resource {}: {}",
                    resource.metadata.name, e
                ));
            }
        } else {
            let msg = format!(
                "Failed to serialize updated resource: {}",
                resource.metadata.name
            );
            kubernetes::log(LogLevel::Error, &msg);
            return ReconcileResult::Error(msg);
        }

        ReconcileResult::Ok
    }
}

export!(SimpleOperator);
