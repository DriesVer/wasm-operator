use crate::local::operator::kubernetes;
use crate::local::operator::kubernetes::LogLevel;
use serde::{Deserialize, Serialize};

wit_bindgen::generate!(
    {
        path: "../../pkg/controller/wit",
        world: "child-world",
    }
);

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
        // Not implemented for this example
        Vec::new()
    }

    fn deserialize(_bytes: Vec<u8>) {
        // Not implemented for this example
    }

    fn reconcile(req: ReconcileRequest) -> ReconcileResult {
        // Log the incoming request for demonstration purposes
        let log_message = format!(
            "Received watch event: {:?} for resource: {:?}",
            req.event_type, req.resource_json
        );
        kubernetes::log(LogLevel::Info, &log_message);

        let mut resource: TestResource = match serde_json::from_str(&req.resource_json) {
            Ok(r) => r,
            Err(e) => {
                kubernetes::log(LogLevel::Error, &format!("Failed to parse resource: {}", e));
                return ReconcileResult::Error(format!("Failed to parse resource: {}", e));
            }
        };
        let namespace = resource
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let needs_change = resource.spec.nonce == 0 || resource.spec.updated_at.is_none();

        if !needs_change {
            kubernetes::log(
                LogLevel::Info,
                &format!(
                    "Resource {}/{} is already reconciled; skipping update",
                    namespace, resource.metadata.name
                ),
            );
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
        kubernetes::log(
            LogLevel::Info,
            &format!("Found resources: {:?}", all_resources),
        );
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

        kubernetes::log(LogLevel::Info, "Rust operator reconciliation complete.");
        ReconcileResult::Ok
    }
}

export!(SimpleOperator);
