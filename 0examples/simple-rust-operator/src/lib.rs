// ### Packages ###
// Packages to interact with the Kubernetes API via the parent controller.
use crate::local::operator::kubernetes;
use crate::local::operator::kubernetes::LogLevel;

// Pacakge to handle serialization and deserialization of memory.
use bincode;

// Packages for CR handling
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;

// Packages for managing operator state in memory.
use std::sync::{Mutex, OnceLock};

// Import the WIT bindings to interact with the parent controller.
wit_bindgen::generate!(
    {
        path: "../../pkg/controller/wit",
        world: "child-world",
    }
);

// ### Kubernetes CRD Structs ###
// Structs that mirror the TestResource CRD used in the operator.
// This can be done via Kube-rs to enable the derivation of the YAML CRD schema, but for simplicity we define them manually here.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestResource {
    api_version: String,
    kind: String,
    metadata: ObjectMeta,
    spec: TestResourceSpec,
    status: Option<TestResourceStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    name: String,
    namespace: Option<String>,
    resource_version: Option<String>,
    generation: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestResourceSpec {
    factor: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestResourceStatus {
    outcome: i64,
    observed_generation: Option<i64>,
    last_updated: DateTime<Utc>,
}

// ### Memory of operator ###
static COUNTER: OnceLock<Mutex<i64>> = OnceLock::new();

fn get_counter() -> &'static Mutex<i64> {
    COUNTER.get_or_init(|| Mutex::new(0))
}

// ### Controller logic ###
// The main logic of the operator, which will be called by the parent controller. The child operator must implement get_watch_requests, serialize, deserialize and reconcile.
struct SimpleOperator;

impl Guest for SimpleOperator {
    // Returns a list of WatchRequests that specify which Kubernetes resources the parent controller should watch for changes.
    fn get_watch_requests() -> Vec<WatchRequest> {
        const KIND: &str = "TestResource";
        let namespace =
            env::var("TESTRESOURCE_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        vec![WatchRequest {
            kind: KIND.to_string(),
            namespace: namespace.to_string(),
        }]
    }

    // Serializes the operator's internal state into a byte vector that can be stored by the parent controller.
    fn serialize() -> Vec<u8> {
        // The data to serialize can be anything that is serializable.
        let data = get_counter().lock().unwrap();
        let data = *data;

        // Logging for demo purposes
        kubernetes::log(LogLevel::Info, &format!("Serializing data: {}", &data));

        bincode::serialize(&data).unwrap_or_else(|e| {
            kubernetes::log(LogLevel::Error, &format!("Failed to serialize data: {}", e));
            Vec::new()
        })
    }

    // Deserializes the byte vector back into the operator's internal state.
    fn deserialize(bytes: Vec<u8>) {
        let decoded = bincode::deserialize::<i64>(&bytes);
        match decoded {
            Ok(data) => {
                // Logging for demo purposes
                kubernetes::log(LogLevel::Info, &format!("Deserialized data: {}", data));

                let mut counter = get_counter().lock().unwrap();
                *counter = data;
            }
            Err(e) => {
                kubernetes::log(
                    LogLevel::Error,
                    &format!("Failed to deserialize data: {}", e),
                );
            }
        }
    }

    // The reconcile function is called by the parent controller whenever a watched resource changes. It contains the main logic of the operator.
    fn reconcile(request: ReconcileRequest) -> ReconcileResult {
        // Get the resource from the request
        let mut resource: TestResource = match serde_json::from_str(&request.resource_json) {
            Ok(r) => r,
            Err(e) => {
                return ReconcileResult::Error(format!("Failed to parse resource: {}", e));
            }
        };

        // Check if the observed generation matches the current generation, if so, it means there are no changes to reconcile and we can return early.
        if resource.status.is_some() {
            let status = resource.status.as_ref().unwrap();
            if status.observed_generation == resource.metadata.generation {
                // No changes to reconcile, return Ok without updating the resource.
                return ReconcileResult::Ok;
            }
        }

        // Log the updated resource for demo purposes
        kubernetes::log(
            LogLevel::Info,
            &format!("Reconciling resource: {:?}", resource),
        );

        // Increment the counter and update the resource's status with outcome of the base_number times the counter
        let mut counter = get_counter().lock().unwrap();
        *counter += 1;

        resource.status = Some(TestResourceStatus {
            outcome: resource.spec.factor * *counter,
            observed_generation: resource.metadata.generation,
            last_updated: Utc::now(),
        });

        drop(counter);

        // Return the updated resource as a JSON string to be applied by the parent controller.
        match serde_json::to_string(&resource) {
            Ok(updated_json) => {
                let namespace = resource.metadata.namespace.as_deref().unwrap_or("default");

                kubernetes::log(
                    LogLevel::Warn,
                    &format!(
                        "Updating resource {} in namespace {} as: {:?}",
                        resource.metadata.name, namespace, updated_json
                    ),
                );

                let result = kubernetes::update_resource(
                    "TestResource",
                    &resource.metadata.name,
                    &namespace,
                    &updated_json,
                    true,
                );

                if let Err(e) = result {
                    return ReconcileResult::Error(format!(
                        "Failed to update resource {}: {}",
                        resource.metadata.name, e
                    ));
                }
            }
            Err(e) => {
                return ReconcileResult::Error(format!(
                    "Failed to serialize updated resource: {}",
                    e
                ));
            }
        }
        ReconcileResult::Ok
    }
}

export!(SimpleOperator);
