use anyhow::Result;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use kube::api::{ApiResource as KubeApiResource, WatchParams as KubeWatchParams};
use kube::core::{dynamic::DynamicObject, WatchEvent as KubeWatchEvent};
use kube::ResourceExt;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::host::helper::get_dynamic_api;
use crate::host::wit::bindings::local::kube::api::{
    ApiResource, Error, HttpError, WatchEvent, WatchId, WatchParams,
};
use crate::kubernetes::KubernetesService;
use crate::runtime::wasmoperator::{WORCommand, WasmOperatorRuntime};

use kube::Error as KubeError;
use std::sync::{Arc, LazyLock};
use tokio_stream::StreamMap;

type WatcherResult = Result<KubeWatchEvent<DynamicObject>, KubeError>;
type BoxedWatchStream = Pin<Box<dyn Stream<Item = Option<WatcherResult>> + Send>>;

/// Commands for the stream manager background worker.
enum StreamManagerCmd {
    Register {
        signature: WatchStreamSignature,
        cluster_version: String,
        bookmarks: bool,
        operator: Arc<WasmOperatorRuntime>,
    },
}

/// Manages Kubernetes watch streams and distributes events to operators.
/// Is used in tandem with a WatchStreamHandler in the WasmOperator itself. (Inside Kube-rs package)
pub struct WatchStreamHandler {
    cmd_tx: mpsc::UnboundedSender<StreamManagerCmd>,
    last_event_versions: DashMap<u64, String>, // Maps WatchStreamSignature hash to last seen resource version that was an actual event (not a bookmark)
    next_temp_id: AtomicU64,
}

#[derive(Clone, Debug)]
/// Signature identifying a unique watch stream configuration.
pub struct WatchStreamSignature {
    group: String,
    api_version: String,
    kind: String,
    plural: String,
    namespace: Option<String>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    send_initial_events: bool,

    temporary: u64,
    hash: u64, // Precomputed internal hash for performance
}

impl WatchStreamSignature {
    /// Returns the precomputed hash in O(1) time.
    #[inline]
    pub fn get_hash(&self) -> u64 {
        self.hash
    }

    /// Calculates and caches the hash for the watch stream signature.
    fn calculate_hash(&mut self) {
        let mut hasher = DefaultHasher::new();
        self.group.hash(&mut hasher);
        self.api_version.hash(&mut hasher);
        self.kind.hash(&mut hasher);
        self.plural.hash(&mut hasher);
        self.namespace.hash(&mut hasher);
        self.label_selector.hash(&mut hasher);
        self.field_selector.hash(&mut hasher);
        self.send_initial_events.hash(&mut hasher);
        self.temporary.hash(&mut hasher);
        let hash = hasher.finish();
        self.hash = hash;
    }

    pub fn set_temporary(&mut self, temporary: u64) {
        self.temporary = temporary;
        self.calculate_hash();
    }
}

impl Hash for WatchStreamSignature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl PartialEq for WatchStreamSignature {
    fn eq(&self, other: &Self) -> bool {
        // Fast-path comparison using precomputed hash before comparing full fields
        self.hash == other.hash
            && self.temporary == other.temporary
            && self.group == other.group
            && self.api_version == other.api_version
            && self.kind == other.kind
            && self.plural == other.plural
            && self.namespace == other.namespace
            && self.label_selector == other.label_selector
            && self.field_selector == other.field_selector
            && self.send_initial_events == other.send_initial_events
    }
}

impl Eq for WatchStreamSignature {}

impl From<(ApiResource, WatchParams)> for WatchStreamSignature {
    fn from((api, params): (ApiResource, WatchParams)) -> Self {
        let mut signature = WatchStreamSignature {
            group: api.group,
            api_version: api.version,
            kind: api.kind,
            plural: api.plural,
            namespace: api.namespace,
            label_selector: params.label_selector,
            field_selector: params.field_selector,
            send_initial_events: params.send_initial_events,
            temporary: 0,
            hash: 0, // Placeholder, will be calculated below
        };

        signature.calculate_hash();
        signature
    }
}

impl From<&WatchStreamSignature> for KubeApiResource {
    fn from(sig: &WatchStreamSignature) -> Self {
        let api_version = if sig.group.is_empty() {
            sig.api_version.clone()
        } else {
            format!("{}/{}", sig.group, sig.api_version)
        };
        KubeApiResource {
            group: sig.group.clone(),
            version: sig.api_version.clone(),
            api_version,
            kind: sig.kind.clone(),
            plural: sig.plural.clone(),
        }
    }
}

impl From<&WatchStreamSignature> for KubeWatchParams {
    fn from(sig: &WatchStreamSignature) -> Self {
        KubeWatchParams {
            label_selector: sig.label_selector.clone(),
            field_selector: sig.field_selector.clone(),
            timeout: None, // Timout is managed by the stream handler, not the operators
            bookmarks: true, // Always request bookmarks from the API server to track resource versions
            send_initial_events: sig.send_initial_events,
        }
    }
}

// One global stream handler, this reduces overhead in comparrison to spawning a new task for each operator.
// This also creates an artificial bottleneck which is not bad because not all operators will be trying at the same time if watch events arive at the same time.
static WATCH_STREAM_HANDLER: LazyLock<Arc<WatchStreamHandler>> = LazyLock::new(|| {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<StreamManagerCmd>();

    let _self = Arc::new(WatchStreamHandler {
        cmd_tx,
        last_event_versions: DashMap::new(),
        next_temp_id: AtomicU64::new(1),
    });
    let self_clone = _self.clone();

    let shutdown_token = crate::shutdown::shutdown_token();

    // Spawn the background worker that listens to all registered streams
    tokio::spawn(async move {
        let mut streams = StreamMap::new();
        let operators = DashMap::<WatchId, Vec<(Arc<WasmOperatorRuntime>, bool)>>::new();
        let cluster_resource_versions = DashMap::<WatchId, String>::new();
        let mut gone_streams = HashSet::<WatchId>::new();

        loop {
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => {
                    info!("Watch stream handler received shutdown signal. Closing all streams and exiting.");
                    break;
                }

                // Handle new registrations
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(StreamManagerCmd::Register { signature, cluster_version, bookmarks, operator }) => {
                            debug!("Registering watch stream for signature: {:?}, cluster_version: {}, operator: {}", signature, cluster_version, &operator.cr.name);
                            let watch_id = signature.get_hash();
                            if signature.temporary > 0 {
                                let stream_res = if signature.send_initial_events {
                                    WatchStreamHandler::get_watch_stream_from_signature(&signature, &cluster_version).await
                                } else {
                                    // Requested watch stream is behind the current streams, registering a temporary stream with 410 Gone error
                                    let s: BoxedWatchStream = futures::stream::once(async {
                                        Some(Err(KubeError::Api(Box::new(kube::core::Status {
                                            status: Some(kube::core::response::StatusSummary::Failure),
                                            message: "Requested watch stream is behind the current streams".to_string(),
                                            reason: "Gone".to_string(),
                                            code: 410,
                                            ..Default::default()
                                        }))))
                                    })
                                    .chain(futures::stream::once(async { None }))
                                    .boxed();
                                    Ok(s)
                                };

                                match stream_res {
                                    Ok(stream) => {
                                        cluster_resource_versions.insert(watch_id, cluster_version);
                                        operators.entry(watch_id).or_default().push((operator, bookmarks));
                                        streams.insert(signature, stream);
                                    }
                                    Err(e) => {
                                        warn!("Failed to create temporary watch stream for signature {:?}: {}", watch_id, e);
                                        let watch_event = WatchEvent::Error(e);
                                        let _ = operator.cmd_tx.send(WORCommand::ProcessWatchEvent(watch_id, watch_event));
                                    }
                                }
                            } else {
                                if streams.contains_key(&signature) {
                                    // Watch stream is already active. Share it with the new operator.
                                    operators.entry(watch_id).or_default().push((operator, bookmarks));
                                } else {
                                    // Watch stream is not active yet, open a new connection to API server
                                    match WatchStreamHandler::get_watch_stream_from_signature(&signature, &cluster_version).await {
                                        Ok(stream) => {
                                            cluster_resource_versions.insert(watch_id, cluster_version);
                                            operators.entry(watch_id).or_default().push((operator, bookmarks));
                                            streams.insert(signature, stream);
                                        }
                                        Err(e) => {
                                            warn!("Failed to create watch stream for signature {:?}: {}", watch_id, e);
                                            let watch_event = WatchEvent::Error(e);
                                            let _ = operator.cmd_tx.send(WORCommand::ProcessWatchEvent(watch_id, watch_event));
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            // Command channel was closed (WatchStreamHandler was dropped)
                            warn!("Command channel closed for watch stream handler. Shutting down worker.");
                            break;
                        }
                    }
                }

                // Handle next event from any registered stream
                Some((id, stream_event)) = streams.next(), if !streams.is_empty() => {
                    let op_list = operators
                        .get(&id.get_hash())
                        .map(|entry| entry.value().clone())
                        .unwrap_or_default();

                    match stream_event {
                        Some(event) => {
                            let process_event = |obj: &DynamicObject, constructor: fn(String) -> WatchEvent| {
                                cluster_resource_versions.insert(id.get_hash(), obj.resource_version().unwrap_or_default());
                                _self.last_event_versions.insert(id.get_hash(), obj.resource_version().unwrap_or_default());
                                match serde_json::to_string(obj) {
                                    Ok(json) => constructor(json),
                                    Err(e) => WatchEvent::Error(Error::Other(e.to_string())),
                                }
                            };

                            let watch_event: WatchEvent = match event {
                                Ok(KubeWatchEvent::Added(obj)) => process_event(&obj, WatchEvent::Added),
                                Ok(KubeWatchEvent::Modified(obj)) => process_event(&obj, WatchEvent::Modified),
                                Ok(KubeWatchEvent::Deleted(obj)) => process_event(&obj, WatchEvent::Deleted),

                                Ok(KubeWatchEvent::Bookmark(bookmark)) => {
                                    if id.temporary > 0 {
                                        // If the stream is temporary and sends bookmark, it means initial events are done and we can switch to the main stream.
                                        streams.remove(&id);
                                        let old_ops: Option<(WatchId, Vec<(Arc<WasmOperatorRuntime>, bool)>)>
                                            = operators.remove(&id.get_hash());
                                        cluster_resource_versions.remove(&id.get_hash());

                                        let cv: String = bookmark.metadata.resource_version.clone();
                                        let mut new_id = id.clone();
                                        new_id.set_temporary(0);
                                        new_id.send_initial_events = false;
                                        new_id.calculate_hash();
                                        let norm_hash = new_id.get_hash();

                                        if let Some((_, ops)) = old_ops {
                                            // Send last bookmark to operators and redirect them to the new stream.
                                            let watch_event = WatchEvent::UpgradeBookmark((norm_hash,serde_json::to_string(&bookmark).unwrap_or_else(|e| format!("Bookmark serialization error: {e}"))));

                                            for (op, _) in ops.clone() {
                                                let _ = op.cmd_tx.send(WORCommand::ProcessWatchEvent(id.get_hash(), watch_event.clone()));
                                            }

                                            // If the new stream is already active, just add the operators to it. Otherwise, create a new stream to Kube API server.
                                            if streams.contains_key(&new_id) {
                                                operators.entry(norm_hash).or_default().extend(ops);
                                            } else {
                                                match WatchStreamHandler::get_watch_stream_from_signature(&new_id, &cv).await {
                                                    Ok(stream) => {
                                                        streams.insert(new_id.clone(), stream);
                                                        cluster_resource_versions.insert(norm_hash, cv);
                                                        operators.entry(norm_hash).or_default().extend(ops);
                                                    },
                                                    Err(e) => {
                                                        warn!("Failed to recreate watch stream for signature {:?}: {}", norm_hash, e);
                                                    }
                                                }
                                            }
                                        }
                                        continue;
                                    }
                                    cluster_resource_versions.insert(id.get_hash(), bookmark.metadata.resource_version.clone());
                                    serde_json::to_string(&bookmark)
                                        .map(WatchEvent::Bookmark)
                                        .unwrap_or_else(|e| WatchEvent::Error(Error::Other(format!("Bookmark serialization error: {e}"))))
                                }

                                Ok(KubeWatchEvent::Error(status)) => {
                                    warn!("Received error from watch stream for signature {:?}: {:?}", id.get_hash(), status);
                                    if status.code == 410 {
                                        gone_streams.insert(id.get_hash());
                                    }
                                    WatchEvent::Error(Error::Http(HttpError {
                                        code: status.code,
                                        reason: status.reason,
                                        message: status.message,
                                    }))
                                },

                                // Handle a stream transport error
                                Err(e) =>  {
                                    error!("Error in watch stream for signature {:?}: {}", id.get_hash(), e);
                                    let wit_err = Error::from(e);
                                    if let Error::Http(ref status) = wit_err {
                                        if status.code == 410 {
                                            gone_streams.insert(id.get_hash());
                                        }
                                    }
                                    WatchEvent::Error(wit_err)
                                }
                            };

                            for (operator, op_accepts_bookmarks) in &op_list {
                                if let WatchEvent::Bookmark(_) = &watch_event {
                                    if !op_accepts_bookmarks {
                                        continue;
                                    }
                                }
                                if let Err(e) = operator.cmd_tx.send(WORCommand::ProcessWatchEvent(id.get_hash(), watch_event.clone())) {
                                    warn!("Failed to queue watch event for stream {} and operator {}, removing the operator as listener: {:?}", id.get_hash(), operator.cr.name, e);
                                    let mut ops = operators.entry(id.get_hash()).or_default();
                                    ops.retain(|(op, _)| !Arc::ptr_eq(op, operator));
                                    if ops.is_empty() {
                                        operators.remove(&id.get_hash());
                                        streams.remove(&id);
                                        cluster_resource_versions.remove(&id.get_hash());
                                    }
                                }
                            }
                        }
                        None => {
                            let hash = id.get_hash();
                            // Stream hit EOF (closed). Attempt to recreate the stream.
                            if gone_streams.remove(&hash) {
                                // If the stream hit a 410 error, we remove it permanently so the operator can restart it from scratch.
                                streams.remove(&id);
                                operators.remove(&hash);
                                cluster_resource_versions.remove(&hash);
                                continue;
                            }

                            let cv: String = cluster_resource_versions.get(&hash)
                                .expect("Cluster version option should exist for registered stream")
                                .value()
                                .clone();
                            match WatchStreamHandler::get_watch_stream_from_signature(&id, &cv).await {
                                Ok(stream) => { streams.insert(id.clone(), stream); },
                                Err(e) => {
                                    warn!("Failed to recreate watch stream for signature {:?}: {}", hash, e);
                                    if let Some((_, ops)) = operators.remove(&hash) {
                                        let watch_event = WatchEvent::Error(e);
                                        for (op, _) in ops {
                                            let _ = op.cmd_tx.send(WORCommand::ProcessWatchEvent(hash, watch_event.clone()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    self_clone
});

impl WatchStreamHandler {
    /// Returns the global instance of the watch stream handler.
    pub fn get_instance() -> Arc<Self> {
        WATCH_STREAM_HANDLER.clone()
    }

    /// Creates a new watch stream from the Kubernetes API.
    async fn get_watch_stream_from_signature(
        signature: &WatchStreamSignature,
        cluster_version: &str,
    ) -> Result<BoxedWatchStream, Error> {
        let api_res = KubeApiResource::from(signature);
        let wp = KubeWatchParams::from(signature);

        let k8s_client = KubernetesService::global_client().await?;
        let kube_api = get_dynamic_api(k8s_client, signature.namespace.clone(), &api_res);

        let stream = kube_api.watch(&wp, cluster_version).await?;

        let stream_with_eof = stream
            .map(Some) // Wrap each event in Some to indicate it's a valid event
            .chain(futures::stream::once(async { None })); // Indicate the end of the stream with None

        Ok(Box::pin(stream_with_eof))
    }

    /// Registers an operator to a watch stream and returns the watch ID.
    pub async fn register_watch_stream(
        &self,
        operator: Arc<WasmOperatorRuntime>,
        mut signature: WatchStreamSignature,
        cluster_version: String,
        bookmarks: bool,
    ) -> Result<WatchId, Error> {
        let is_behind = match self.last_event_versions.get(&signature.get_hash()) {
            Some(last_version) if !cluster_version.as_str().is_empty() => {
                // IMPORTANT: comparing resource versions is only valid if in a cluster with monotonic resource versions.
                let last_event_version: u64 = last_version.as_str().parse().map_err(|e| {
                    Error::Other(format!("Failed to parse last event version: {e}"))
                })?;

                let req_cluster_version: u64 = cluster_version
                    .as_str()
                    .parse()
                    .map_err(|e| Error::Other(format!("Failed to parse cluster version: {e}")))?;

                last_event_version > req_cluster_version
            }
            _ => false, // Either no last version, or requested version is empty (treat as latest)
        };

        if signature.send_initial_events || is_behind {
            let temp_id = self.next_temp_id.fetch_add(1, Ordering::SeqCst);
            signature.set_temporary(temp_id);
        }

        let watch_id = signature.get_hash();

        let cmd = StreamManagerCmd::Register {
            signature,
            cluster_version,
            bookmarks,
            operator,
        };
        self.cmd_tx
            .send(cmd)
            .map_err(|e| Error::Other(format!("Failed to send register command: {}", e)))?;

        Ok(watch_id)
    }
}
