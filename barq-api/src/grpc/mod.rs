pub mod compat;

use crate::{ApiError, ApiPermission, AppState, ReadPreference};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use barq_core::{
    CatalogError, CollectionSchema, DistanceMetric, Document, DocumentId, FieldSchema, FieldType,
    Filter, IndexState, PayloadValue, TenantId,
};
use barq_proto::barq::barq_server::Barq;
use barq_proto::barq::{
    BatchSearchRequest, BatchSearchResponse, Consistency, CreateCollectionRequest,
    CreateCollectionResponse, HealthRequest, HealthResponse, InsertAsyncResponse,
    InsertDocumentRequest, InsertDocumentResponse, InsertOptions, InsertRequest, InsertResponse,
    QueryResults, SearchOptions, SearchRequest, SearchResponse, SearchResult, StatusRequest,
    StatusResponse,
};
use barq_storage::StorageError;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

pub struct GrpcService {
    pub(crate) state: AppState,
}

impl GrpcService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    fn status_response() -> StatusResponse {
        StatusResponse {
            ok: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    fn require_non_empty(value: &str, field: &str) -> Result<(), Status> {
        if value.trim().is_empty() {
            Err(Status::invalid_argument(format!(
                "{field} must not be empty"
            )))
        } else {
            Ok(())
        }
    }

    fn parse_insert_payload(payload_json: &str) -> Result<Option<PayloadValue>, Status> {
        if payload_json.trim().is_empty() {
            return Ok(None);
        }

        let payload_json: serde_json::Value = serde_json::from_str(payload_json)
            .map_err(|err| Status::invalid_argument(format!("invalid JSON payload: {err}")))?;
        Ok(Some(json_to_payload(payload_json)))
    }

    fn parse_document_id(id: &str) -> DocumentId {
        if let Ok(value) = id.parse::<u64>() {
            DocumentId::U64(value)
        } else {
            DocumentId::Str(id.to_string())
        }
    }

    fn authenticate(
        &self,
        metadata: &MetadataMap,
        required: ApiPermission,
    ) -> Result<TenantId, Status> {
        let headers = metadata_to_headers(metadata)?;
        Ok(self
            .state
            .auth
            .authenticate(&headers, required, None)
            .map_err(api_error_to_status)?
            .tenant)
    }

    async fn insert_internal(
        &self,
        tenant: TenantId,
        collection: String,
        id: String,
        vector: Vec<f32>,
        payload_json: String,
        wait_for_commit: bool,
    ) -> Result<InsertResponse, Status> {
        Self::require_non_empty(&collection, "collection")?;
        Self::require_non_empty(&id, "id")?;
        if vector.is_empty() {
            return Err(Status::invalid_argument("vector must not be empty"));
        }

        let document = Document {
            id: Self::parse_document_id(&id),
            vector,
            payload: Self::parse_insert_payload(&payload_json)?,
        };

        self.state
            .ensure_primary_for_document(&tenant, &document.id)
            .map_err(api_error_to_status)?;
        self.state
            .enqueue_insert_for_tenant(&tenant, &collection, document, false, wait_for_commit)
            .await
            .map_err(api_error_to_status)?;

        Ok(InsertResponse { success: true })
    }

    fn parse_insert_document(
        collection: String,
        id: String,
        vector: Vec<f32>,
        payload_json: String,
    ) -> Result<(String, Document), Status> {
        Self::require_non_empty(&collection, "collection")?;
        Self::require_non_empty(&id, "id")?;
        if vector.is_empty() {
            return Err(Status::invalid_argument("vector must not be empty"));
        }

        let document = Document {
            id: Self::parse_document_id(&id),
            vector,
            payload: Self::parse_insert_payload(&payload_json)?,
        };
        Ok((collection, document))
    }

    async fn insert_async_internal(
        &self,
        tenant: TenantId,
        collection: String,
        id: String,
        vector: Vec<f32>,
        payload_json: String,
    ) -> Result<InsertAsyncResponse, Status> {
        let (collection, document) =
            Self::parse_insert_document(collection, id, vector, payload_json)?;

        self.state
            .ensure_primary_for_document(&tenant, &document.id)
            .map_err(api_error_to_status)?;
        let request_id = self
            .state
            .enqueue_insert_for_tenant_async(&tenant, &collection, document, false)
            .await
            .map_err(api_error_to_status)?;

        Ok(InsertAsyncResponse {
            accepted: true,
            request_id,
        })
    }

    async fn search_internal(
        &self,
        tenant: TenantId,
        request: SearchRequest,
    ) -> Result<SearchResponse, Status> {
        Self::require_non_empty(&request.collection, "collection")?;
        if request.vector.is_empty() {
            return Err(Status::invalid_argument("vector must not be empty"));
        }
        if request.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be positive"));
        }

        apply_search_consistency(&self.state, &tenant, request.options.as_ref())
            .map_err(api_error_to_status)?;
        let mut storage = self.state.storage.lock().await;
        if !allow_fallback(request.options.as_ref()) {
            let collection = storage
                .catalog()
                .collection(&tenant, &request.collection)
                .map_err(StorageError::Catalog)
                .map_err(storage_error_to_status)?;
            if collection.index_state() != IndexState::Ready {
                return Err(Status::failed_precondition(format!(
                    "vector index for collection {} is not ready; allow_fallback=false",
                    request.collection
                )));
            }
        }
        let results = storage
            .search_for_tenant(
                &tenant,
                &request.collection,
                &request.vector,
                request.top_k as usize,
                None,
            )
            .map_err(storage_error_to_status)?;

        Ok(SearchResponse {
            results: results
                .into_iter()
                .map(|result| SearchResult {
                    id: match result.id {
                        DocumentId::U64(value) => value.to_string(),
                        DocumentId::Str(value) => value,
                    },
                    score: result.score,
                    payload_json: "{}".to_string(),
                })
                .collect(),
        })
    }
}

fn wait_for_commit(options: Option<&InsertOptions>) -> bool {
    options.map_or(true, |options| options.wait_for_commit)
}

fn allow_fallback(options: Option<&SearchOptions>) -> bool {
    options.map_or(true, |options| options.allow_fallback)
}

fn apply_search_consistency(
    state: &AppState,
    tenant: &TenantId,
    options: Option<&SearchOptions>,
) -> Result<(), ApiError> {
    match search_consistency(options)? {
        SearchConsistency::Default | SearchConsistency::Any => {
            state.ensure_local_for_tenant(tenant)
        }
        SearchConsistency::Primary => {
            state.ensure_read_target_for_tenant(tenant, ReadPreference::Primary)
        }
        SearchConsistency::Followers => {
            state.ensure_read_target_for_tenant(tenant, ReadPreference::Followers)
        }
    }
}

fn search_consistency(options: Option<&SearchOptions>) -> Result<SearchConsistency, ApiError> {
    let Some(options) = options else {
        return Ok(SearchConsistency::Default);
    };

    let consistency = Consistency::try_from(options.consistency).map_err(|_| {
        ApiError::BadRequest(format!(
            "invalid consistency value: {}",
            options.consistency
        ))
    })?;

    Ok(match consistency {
        Consistency::Unspecified => SearchConsistency::Default,
        Consistency::Primary => SearchConsistency::Primary,
        Consistency::Followers => SearchConsistency::Followers,
        Consistency::Any => SearchConsistency::Any,
    })
}

enum SearchConsistency {
    Default,
    Primary,
    Followers,
    Any,
}

fn metadata_to_headers(metadata: &MetadataMap) -> Result<HeaderMap, Status> {
    let mut headers = HeaderMap::new();
    copy_ascii_metadata(metadata, "x-api-key", &mut headers)?;
    copy_ascii_metadata(metadata, "x-tenant-id", &mut headers)?;
    copy_ascii_metadata(metadata, "authorization", &mut headers)?;
    Ok(headers)
}

fn copy_ascii_metadata(
    metadata: &MetadataMap,
    key: &'static str,
    headers: &mut HeaderMap,
) -> Result<(), Status> {
    if let Some(value) = metadata.get(key) {
        let value = value
            .to_str()
            .map_err(|_| Status::invalid_argument(format!("{key} metadata must be ascii")))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|_| Status::invalid_argument(format!("{key} metadata is invalid")))?;
        headers.insert(HeaderName::from_static(key), header_value);
    }
    Ok(())
}

fn storage_error_to_status(error: StorageError) -> Status {
    match error {
        StorageError::Catalog(CatalogError::CollectionMissing(message)) => {
            Status::not_found(message)
        }
        StorageError::Catalog(other) => Status::invalid_argument(other.to_string()),
        StorageError::QuotaExceeded { reason, .. } => Status::resource_exhausted(reason),
        StorageError::SegmentNotWritable { collection, state } => Status::failed_precondition(
            format!("segment for collection {collection} is not writable: {state:?}"),
        ),
        other => Status::internal(other.to_string()),
    }
}

fn api_error_to_status(error: ApiError) -> Status {
    match error {
        ApiError::Storage(storage_error) => storage_error_to_status(storage_error),
        ApiError::DocumentId(error) => Status::invalid_argument(error.to_string()),
        ApiError::BadRequest(message) => Status::invalid_argument(message),
        ApiError::Unauthorized(message) => Status::unauthenticated(message),
        ApiError::Forbidden(message) => Status::permission_denied(message),
        ApiError::Redirect(address) => {
            Status::failed_precondition(format!("request must be routed to {address}"))
        }
        ApiError::Busy(message) => Status::resource_exhausted(message),
        ApiError::Cluster(error) => Status::unavailable(error.to_string()),
        ApiError::Tls(message) => Status::internal(message),
    }
}

fn json_to_payload(v: serde_json::Value) -> PayloadValue {
    match v {
        serde_json::Value::Null => PayloadValue::Null,
        serde_json::Value::Bool(b) => PayloadValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PayloadValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                PayloadValue::F64(f)
            } else {
                // Fallback for unlikely case
                PayloadValue::Null
            }
        }
        serde_json::Value::String(s) => PayloadValue::String(s),
        serde_json::Value::Array(arr) => {
            PayloadValue::Array(arr.into_iter().map(json_to_payload).collect())
        }
        serde_json::Value::Object(map) => {
            let mut new_map = std::collections::HashMap::new();
            for (k, v) in map {
                new_map.insert(k, json_to_payload(v));
            }
            PayloadValue::Object(new_map)
        }
    }
}

#[tonic::async_trait]
impl Barq for GrpcService {
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(Self::status_response()))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ok: Self::status_response().ok,
            version: Self::status_response().version,
        }))
    }

    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let tenant = self.authenticate(request.metadata(), ApiPermission::Write)?;
        let wait_for_commit = wait_for_commit(request.get_ref().options.as_ref());
        let response = self
            .insert_internal(
                tenant,
                request.get_ref().collection.clone(),
                request.get_ref().id.clone(),
                request.get_ref().vector.clone(),
                request.get_ref().payload_json.clone(),
                wait_for_commit,
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn insert_async(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertAsyncResponse>, Status> {
        let tenant = self.authenticate(request.metadata(), ApiPermission::Write)?;
        let response = self
            .insert_async_internal(
                tenant,
                request.get_ref().collection.clone(),
                request.get_ref().id.clone(),
                request.get_ref().vector.clone(),
                request.get_ref().payload_json.clone(),
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let tenant = self.authenticate(request.metadata(), ApiPermission::TenantAdmin)?;
        let req = request.into_inner();

        let metric = match req.metric.to_uppercase().as_str() {
            "COSINE" => DistanceMetric::Cosine,
            "DOT" => DistanceMetric::Dot,
            _ => DistanceMetric::L2,
        };

        let schema = CollectionSchema {
            name: req.name.clone(),
            fields: vec![FieldSchema {
                name: "vector".to_string(),
                field_type: FieldType::Vector {
                    dimension: req.dimension as usize,
                    metric,
                    index: None,
                },
                required: true,
            }],
            bm25_config: None,
            tenant_id: tenant.clone(),
        };

        self.state
            .ensure_primary_for_tenant(&tenant)
            .map_err(api_error_to_status)?;

        let mut storage = self.state.storage.lock().await;

        match storage.create_collection_for_tenant(tenant, schema) {
            Ok(_) => Ok(Response::new(CreateCollectionResponse { success: true })),
            Err(e) => Err(storage_error_to_status(e)),
        }
    }

    async fn insert_document(
        &self,
        request: Request<InsertDocumentRequest>,
    ) -> Result<Response<InsertDocumentResponse>, Status> {
        let tenant = self.authenticate(request.metadata(), ApiPermission::Write)?;
        let req = request.into_inner();
        let response = self
            .insert_internal(
                tenant,
                req.collection,
                req.id,
                req.vector,
                req.payload_json,
                true,
            )
            .await?;
        Ok(Response::new(InsertDocumentResponse {
            success: response.success,
        }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let tenant = self.authenticate(request.metadata(), ApiPermission::Read)?;
        let response = self.search_internal(tenant, request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn batch_search(
        &self,
        request: Request<BatchSearchRequest>,
    ) -> Result<Response<BatchSearchResponse>, Status> {
        let tenant = self.authenticate(request.metadata(), ApiPermission::Read)?;
        let req = request.into_inner();
        let collection_name = req.collection;

        let storage = self.state.storage.lock().await;
        let collection = storage
            .catalog()
            .collection(&tenant, &collection_name)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let mut queries = Vec::new();
        for q in req.queries {
            let filter: Option<Filter> =
                if q.filter_json.is_empty() {
                    None
                } else {
                    Some(serde_json::from_str(&q.filter_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid Filter JSON: {}", e))
                    })?)
                };
            queries.push((q.vector, filter));
        }

        let results_vec = collection
            .batch_search(&queries, req.top_k as usize)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut resp_results = Vec::new();
        for batch in results_vec {
            let mut hits = Vec::new();
            for res in batch {
                let id_str = match res.id {
                    DocumentId::U64(v) => v.to_string(),
                    DocumentId::Str(s) => s,
                };
                hits.push(SearchResult {
                    id: id_str,
                    score: res.score,
                    payload_json: "{}".to_string(),
                });
            }
            resp_results.push(QueryResults { hits });
        }

        Ok(Response::new(BatchSearchResponse {
            results: resp_results,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::TrackedInsertState;
    use crate::{ApiAuth, ClusterConfig, ClusterRouter};
    use barq_cluster::{NodeConfig, NodeId, ShardId, ShardPlacement};
    use barq_sdk_rust::BarqClient as PublicBarqClient;
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::{Mutex, OnceLock};
    use tokio::process::{Child, Command};
    use tonic::transport::Server;
    use tonic::Code;

    fn sdk_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: impl Into<String>) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value.into());
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn grpc_service() -> (tempfile::TempDir, AppState, GrpcService) {
        let dir = tempfile::tempdir().unwrap();
        let storage = barq_storage::Storage::open(dir.path()).unwrap();
        let state = AppState::new(
            storage,
            ApiAuth::new(),
            ClusterRouter::from_config(ClusterConfig::single_node()).unwrap(),
        );
        let service = GrpcService::new(state.clone());
        (dir, state, service)
    }

    fn follower_grpc_service() -> (tempfile::TempDir, AppState, GrpcService) {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = barq_storage::Storage::open(dir.path()).unwrap();
        storage
            .create_collection_for_tenant(
                TenantId::default(),
                CollectionSchema {
                    name: "docs".to_string(),
                    fields: vec![FieldSchema {
                        name: "vector".to_string(),
                        field_type: FieldType::Vector {
                            dimension: 2,
                            metric: DistanceMetric::Cosine,
                            index: None,
                        },
                        required: true,
                    }],
                    bm25_config: None,
                    tenant_id: TenantId::default(),
                },
            )
            .unwrap();
        storage
            .insert_for_tenant(
                &TenantId::default(),
                "docs",
                Document {
                    id: DocumentId::Str("doc-1".to_string()),
                    vector: vec![1.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();

        let config = ClusterConfig {
            node_id: NodeId::new("node-1"),
            nodes: vec![
                NodeConfig {
                    id: NodeId::new("node-0"),
                    address: "http://node-0:50051".to_string(),
                },
                NodeConfig {
                    id: NodeId::new("node-1"),
                    address: "http://node-1:50051".to_string(),
                },
            ],
            shard_count: 1,
            replication_factor: 2,
            read_preference: ReadPreference::Primary,
            placements: HashMap::from([(
                ShardId(0),
                ShardPlacement {
                    shard: ShardId(0),
                    primary: NodeId::new("node-0"),
                    replicas: vec![NodeId::new("node-1")],
                },
            )]),
        };

        let state = AppState::new(
            storage,
            ApiAuth::new(),
            ClusterRouter::from_config(config).unwrap(),
        );
        let service = GrpcService::new(state.clone());
        (dir, state, service)
    }

    async fn start_grpc_server(
        service: GrpcService,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(barq_proto::barq::barq_server::BarqServer::new(service))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        rx.await.ok();
                    },
                )
                .await
                .unwrap();
        });

        (local_addr, handle, tx)
    }

    async fn spawn_command<I, S>(
        workdir: &Path,
        envs: &[(&str, &str)],
        program: &str,
        args: I,
    ) -> Child
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(program);
        command
            .current_dir(workdir)
            .args(args)
            .envs(envs.iter().copied())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.spawn().unwrap_or_else(|error| {
            panic!("failed to start {program}: {error}");
        })
    }

    async fn wait_for_success(label: &str, child: Child) {
        let output = child.wait_with_output().await.unwrap_or_else(|error| {
            panic!("{label} failed to wait: {error}");
        });

        if !output.status.success() {
            panic!(
                "{label} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
                output.status.code(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }

    async fn create_collection(service: &GrpcService, name: &str) {
        service
            .create_collection(Request::new(CreateCollectionRequest {
                name: name.to_string(),
                dimension: 2,
                metric: "Cosine".to_string(),
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn insert_via_grpc_stores_document() {
        let (_dir, state, service) = grpc_service();
        create_collection(&service, "docs").await;

        let response = service
            .insert(Request::new(InsertRequest {
                collection: "docs".to_string(),
                id: "doc-1".to_string(),
                vector: vec![1.0, 0.0],
                payload_json: "{\"kind\":\"grpc\"}".to_string(),
                options: None,
            }))
            .await
            .unwrap();
        assert!(response.into_inner().success);

        let document = state
            .storage
            .lock()
            .await
            .get_document(
                &TenantId::default(),
                "docs",
                &DocumentId::Str("doc-1".to_string()),
            )
            .unwrap();
        assert!(document.is_some());
    }

    #[tokio::test]
    async fn search_via_grpc_returns_ranked_results() {
        let (_dir, _state, service) = grpc_service();
        create_collection(&service, "docs").await;

        for (id, vector) in [("doc-1", vec![1.0, 0.0]), ("doc-2", vec![0.0, 1.0])] {
            service
                .insert(Request::new(InsertRequest {
                    collection: "docs".to_string(),
                    id: id.to_string(),
                    vector,
                    payload_json: "{}".to_string(),
                    options: None,
                }))
                .await
                .unwrap();
        }

        let response = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, "doc-1");
        assert!((response.results[0].score - 1.0).abs() < 0.0001);
    }

    #[tokio::test]
    async fn grpc_invalid_requests_return_invalid_argument() {
        let (_dir, _state, service) = grpc_service();
        create_collection(&service, "docs").await;

        let invalid_insert = service
            .insert(Request::new(InsertRequest {
                collection: "docs".to_string(),
                id: "doc-1".to_string(),
                vector: vec![1.0, 0.0],
                payload_json: "{not-json}".to_string(),
                options: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(invalid_insert.code(), Code::InvalidArgument);

        let invalid_search = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 0,
                options: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(invalid_search.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn insert_wait_for_commit_false_returns_after_queue_admission() {
        let (_dir, state, service) = grpc_service();
        create_collection(&service, "docs").await;
        let hook = state.ingestion.install_pause_before_dequeue();

        let response = service
            .insert(Request::new(InsertRequest {
                collection: "docs".to_string(),
                id: "doc-async".to_string(),
                vector: vec![1.0, 0.0],
                payload_json: "{}".to_string(),
                options: Some(InsertOptions {
                    wait_for_commit: false,
                }),
            }))
            .await
            .unwrap();
        assert!(response.into_inner().success);

        hook.wait_until_reached().await;
        assert_eq!(state.ingestion.queue_len(), 1);
        let document = state
            .storage
            .lock()
            .await
            .get_document(
                &TenantId::default(),
                "docs",
                &DocumentId::Str("doc-async".to_string()),
            )
            .unwrap();
        assert!(document.is_none());

        hook.release();
        state.ingestion.drain().await;
        let document = state
            .storage
            .lock()
            .await
            .get_document(
                &TenantId::default(),
                "docs",
                &DocumentId::Str("doc-async".to_string()),
            )
            .unwrap();
        assert!(document.is_some());
    }

    #[tokio::test]
    async fn insert_async_returns_request_id_without_waiting_for_commit() {
        let (_dir, state, service) = grpc_service();
        create_collection(&service, "docs").await;
        let hook = state.ingestion.install_pause_before_dequeue();

        let response = service
            .insert_async(Request::new(InsertRequest {
                collection: "docs".to_string(),
                id: "doc-handle".to_string(),
                vector: vec![1.0, 0.0],
                payload_json: "{}".to_string(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.accepted);
        assert!(response.request_id.starts_with("ingest-"));

        hook.wait_until_reached().await;
        assert_eq!(state.ingestion.queue_len(), 1);
        let tracked = state
            .ingestion
            .tracked_insert_status(&response.request_id)
            .expect("tracked insert should exist");
        assert_eq!(tracked.state, TrackedInsertState::Queued);

        hook.release();
        state.ingestion.drain().await;
    }

    #[tokio::test]
    async fn insert_async_background_worker_persists_document() {
        let (_dir, state, service) = grpc_service();
        create_collection(&service, "docs").await;
        let hook = state.ingestion.install_pause_before_dequeue();

        let response = service
            .insert_async(Request::new(InsertRequest {
                collection: "docs".to_string(),
                id: "doc-async-worker".to_string(),
                vector: vec![1.0, 0.0],
                payload_json: "{}".to_string(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        hook.wait_until_reached().await;
        let document = state
            .storage
            .lock()
            .await
            .get_document(
                &TenantId::default(),
                "docs",
                &DocumentId::Str("doc-async-worker".to_string()),
            )
            .unwrap();
        assert!(document.is_none());

        hook.release();
        state.ingestion.drain().await;

        let document = state
            .storage
            .lock()
            .await
            .get_document(
                &TenantId::default(),
                "docs",
                &DocumentId::Str("doc-async-worker".to_string()),
            )
            .unwrap();
        assert!(document.is_some());
        let tracked = state
            .ingestion
            .tracked_insert_status(&response.request_id)
            .expect("tracked insert should exist");
        assert_eq!(tracked.state, TrackedInsertState::Succeeded);
    }

    #[tokio::test]
    async fn sdk_async_clients_accept_concurrent_inserts_and_respect_queue_backlog() {
        let _env_lock = sdk_env_lock().lock().unwrap();
        let (_dir, state, service) = grpc_service();
        let hook = state.ingestion.install_pause_before_dequeue();
        let (addr, handle, shutdown) = start_grpc_server(service).await;
        let grpc_addr = addr.to_string();
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let _grpc_override = EnvVarGuard::set("BARQ_GRPC_ADDR", &grpc_addr);

        let rust_task = tokio::spawn(async move {
            let client = PublicBarqClient::new("http://127.0.0.1:8080", "");
            client
                .create_collection("sdk-rust-async", 2, DistanceMetric::Cosine, None, None)
                .await
                .expect("rust create collection");
            client
                .collection("sdk-rust-async")
                .insert_async(
                    "rust-async-doc",
                    vec![1.0, 0.0],
                    Some(serde_json::json!({"sdk": "rust", "mode": "async"})),
                )
                .await
                .expect("rust async insert")
        });

        let python = spawn_command(
            &workspace_root.join("barq-sdk-python"),
            &[
                ("PYTHONPATH", "."),
                ("BARQ_BASE_URL", "http://127.0.0.1:8080"),
                ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
                ("BARQ_TEST_COLLECTION", "sdk-python-async"),
            ],
            "python3",
            [
                "-m",
                "unittest",
                "discover",
                "-s",
                "tests",
                "-p",
                "test_async_smoke.py",
            ],
        )
        .await;

        let go = spawn_command(
            &workspace_root.join("barq-sdk-go"),
            &[
                ("BARQ_BASE_URL", "http://127.0.0.1:8080"),
                ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
                ("BARQ_TEST_COLLECTION", "sdk-go-async"),
            ],
            "go",
            [
                "test",
                "./...",
                "-run",
                "TestAsyncInsertReturnsRequestID",
                "-count=1",
            ],
        )
        .await;

        let typescript_build = spawn_command(
            &workspace_root.join("barq-sdk-ts"),
            &[],
            "node",
            ["./node_modules/typescript/lib/tsc.js", "--pretty", "false"],
        )
        .await;
        wait_for_success("typescript build", typescript_build).await;

        let typescript = spawn_command(
            &workspace_root.join("barq-sdk-ts"),
            &[
                ("BARQ_BASE_URL", "http://127.0.0.1:8080"),
                ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
                ("BARQ_TEST_COLLECTION", "sdk-ts-async"),
            ],
            "node",
            ["--test", "test/async_smoke.test.js"],
        )
        .await;

        hook.wait_until_reached().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while state.ingestion.queue_len() < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all async inserts should reach the queue");
        assert_eq!(state.ingestion.queue_len(), 4);

        for (collection, id) in [
            ("sdk-rust-async", "rust-async-doc"),
            ("sdk-python-async", "python-async-doc"),
            ("sdk-go-async", "go-async-doc"),
            ("sdk-ts-async", "ts-async-doc"),
        ] {
            let document = state
                .storage
                .lock()
                .await
                .get_document(
                    &TenantId::default(),
                    collection,
                    &DocumentId::Str(id.to_string()),
                )
                .unwrap();
            assert!(document.is_none(), "document {id} should still be queued");
        }

        hook.release();
        let rust_request_id = rust_task.await.unwrap();
        assert!(rust_request_id.starts_with("ingest-"));
        wait_for_success("python async smoke", python).await;
        wait_for_success("go async smoke", go).await;
        wait_for_success("typescript async smoke", typescript).await;
        state.ingestion.drain().await;

        for (collection, id) in [
            ("sdk-rust-async", "rust-async-doc"),
            ("sdk-python-async", "python-async-doc"),
            ("sdk-go-async", "go-async-doc"),
            ("sdk-ts-async", "ts-async-doc"),
        ] {
            let document = state
                .storage
                .lock()
                .await
                .get_document(
                    &TenantId::default(),
                    collection,
                    &DocumentId::Str(id.to_string()),
                )
                .unwrap();
            assert!(
                document.is_some(),
                "document {id} should flush after release"
            );
        }

        shutdown.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn insert_wait_for_commit_defaults_to_existing_behavior() {
        let (_dir, state, service) = grpc_service();
        create_collection(&service, "docs").await;
        let hook = state.ingestion.install_pause_before_dequeue();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let insert_service = service;
        tokio::spawn(async move {
            let response = insert_service
                .insert(Request::new(InsertRequest {
                    collection: "docs".to_string(),
                    id: "doc-sync".to_string(),
                    vector: vec![1.0, 0.0],
                    payload_json: "{}".to_string(),
                    options: None,
                }))
                .await;
            tx.send(response.map(|response| response.into_inner().success))
                .unwrap();
        });

        hook.wait_until_reached().await;
        assert!(rx.try_recv().is_err());

        hook.release();
        let completed = rx.recv().await.expect("insert should complete");
        assert!(completed.unwrap());

        let document = state
            .storage
            .lock()
            .await
            .get_document(
                &TenantId::default(),
                "docs",
                &DocumentId::Str("doc-sync".to_string()),
            )
            .unwrap();
        assert!(document.is_some());
    }

    #[tokio::test]
    async fn search_allow_fallback_false_rejects_non_ready_index() {
        let (_dir, state, service) = grpc_service();
        create_collection(&service, "docs").await;
        service
            .insert(Request::new(InsertRequest {
                collection: "docs".to_string(),
                id: "doc-1".to_string(),
                vector: vec![1.0, 0.0],
                payload_json: "{}".to_string(),
                options: None,
            }))
            .await
            .unwrap();

        state
            .storage
            .lock()
            .await
            .seal_segment_for_tenant(&TenantId::default(), "docs")
            .unwrap();

        let err = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                options: Some(SearchOptions {
                    consistency: Consistency::Unspecified as i32,
                    allow_fallback: false,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("allow_fallback=false"));

        let response = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, "doc-1");
    }

    #[tokio::test]
    async fn search_consistency_options_route_reads_without_changing_default_behavior() {
        let (_dir, _state, service) = follower_grpc_service();

        let default_response = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(default_response.results.len(), 1);
        assert_eq!(default_response.results[0].id, "doc-1");

        let primary_err = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                options: Some(SearchOptions {
                    consistency: Consistency::Primary as i32,
                    allow_fallback: true,
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(primary_err.code(), Code::FailedPrecondition);
        assert!(primary_err.message().contains("request must be routed to"));

        let follower_response = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                options: Some(SearchOptions {
                    consistency: Consistency::Followers as i32,
                    allow_fallback: true,
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(follower_response.results.len(), 1);
        assert_eq!(follower_response.results[0].id, "doc-1");
    }
}
