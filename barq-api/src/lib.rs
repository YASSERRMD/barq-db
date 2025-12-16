use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use barq_bm25::Bm25Config;
use barq_admin::auth::{JwtVerifier, JwtClaims, AuthMethod};
pub use barq_cluster::{ClusterConfig, ClusterError, ClusterRouter};
use barq_core::{
    CollectionSchema, Document, FieldSchema, FieldType, Filter, HybridSearchResult, HybridWeights,
    PayloadValue, TenantId,
};
use barq_index::{DistanceMetric, DocumentId, DocumentIdError, IndexType};
use barq_storage::{Storage, TenantQuota, TenantUsageReport};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tracing::info;
use tracing_subscriber::EnvFilter;
use std::time::Instant;

// Re-export auth types for convenience and backward compatibility
pub use barq_admin::auth::{ApiAuth, ApiError, ApiPermission, ApiRole, ApiIdentity, TlsConfig};
pub use barq_admin::{admin_routes, AdminState};
pub mod grpc;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Mutex<Storage>>,
    pub auth: ApiAuth,
    pub metrics: PrometheusHandle,
    pub cluster: ClusterRouter,
}

impl AppState {
    pub fn new(storage: Storage, auth: ApiAuth, cluster: ClusterRouter) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
            auth,
            metrics: init_metrics_recorder(),
            cluster,
        }
    }
}

// Implement conversion from AppState to AdminState
impl From<AppState> for AdminState {
    fn from(state: AppState) -> Self {
        Self {
            storage: state.storage,
            cluster: state.cluster,
            auth: state.auth,
        }
    }
}

fn init_metrics_recorder() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("failed to install metrics recorder")
        })
        .clone()
}

fn audit_log(action: &str, identity: &ApiIdentity, details: &str) {
    info!(
        target: "audit",
        action,
        tenant = identity.tenant.as_str(),
        role = ?identity.role,
        actor = identity.actor.as_deref().unwrap_or("unknown"),
        method = identity.method.as_str(),
        details,
        "security event"
    );
}

// Re-export AuthMethod if needed or use from ApiIdentity?
// ApiIdentity.method is AuthMethod.
// barq-admin needs to expose AuthMethod if we want to call as_str on it?
// Or we just rely on Debug impl?
// ApiIdentity struct fields are pub in barq-admin.

impl AppState {
    fn ensure_primary_for_tenant(&self, tenant: &TenantId) -> Result<(), ApiError> {
        self.map_cluster_local_result(self.cluster.ensure_primary(tenant.as_str()))
    }

    fn ensure_primary_for_document(
        &self,
        tenant: &TenantId,
        document: &DocumentId,
    ) -> Result<(), ApiError> {
        let key = format!("{}:{}", tenant.as_str(), document);
        self.map_cluster_local_result(self.cluster.ensure_primary(&key))
    }

    fn ensure_local_for_tenant(&self, tenant: &TenantId) -> Result<(), ApiError> {
        self.map_cluster_local_result(self.cluster.ensure_local(tenant.as_str(), None))
    }

    fn map_cluster_local_result(&self, result: Result<(), ClusterError>) -> Result<(), ApiError> {
        match result {
            Ok(()) => Ok(()),
            Err(ClusterError::NotLocal {
                target_address: Some(address),
                ..
            }) => Err(ApiError::Redirect(address)),
            Err(err) => Err(ApiError::Cluster(err)),
        }
    }
}

// ... (Restoring missing structs)

#[derive(Debug, Deserialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub dimension: usize,
    pub metric: DistanceMetric,
    #[serde(default)]
    pub index: Option<IndexType>,
    #[serde(default)]
    pub text_fields: Vec<TextFieldRequest>,
    #[serde(default)]
    pub bm25_config: Option<Bm25Config>,
}

#[derive(Debug, Deserialize)]
pub struct TextFieldRequest {
    pub name: String,
    #[serde(default)]
    pub indexed: bool,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum DocumentIdInput {
    U64(u64),
    Str(String),
}

impl TryFrom<DocumentIdInput> for DocumentId {
    type Error = DocumentIdError;

    fn try_from(value: DocumentIdInput) -> Result<Self, Self::Error> {
        let id = match value {
            DocumentIdInput::U64(v) => DocumentId::U64(v),
            DocumentIdInput::Str(s) => DocumentId::Str(s),
        };
        id.validate()?;
        Ok(id)
    }
}

#[derive(Debug, Deserialize)]
pub struct InsertDocumentRequest {
    pub id: DocumentIdInput,
    pub vector: Vec<f32>,
    pub payload: Option<PayloadValue>,
    #[serde(default)]
    pub upsert: bool,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub vector: Vec<f32>,
    pub top_k: usize,
    #[serde(default)]
    pub filter: Option<Filter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<barq_index::SearchResult>,
}

#[derive(Debug, Deserialize)]
pub struct TextSearchRequest {
    pub query: String,
    pub top_k: usize,
    #[serde(default)]
    pub filter: Option<Filter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextSearchResponse {
    pub results: Vec<barq_index::SearchResult>,
}

#[derive(Debug, Deserialize)]
pub struct HybridSearchRequest {
    pub query: String,
    pub vector: Vec<f32>,
    pub top_k: usize,
    #[serde(default)]
    pub weights: Option<HybridWeights>,
    #[serde(default)]
    pub filter: Option<Filter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HybridSearchResponse {
    pub results: Vec<HybridSearchResult>,
}

#[derive(Debug, Deserialize)]
pub struct ExplainRequest {
    pub id: DocumentIdInput,
    pub query: String,
    pub vector: Vec<f32>,
    pub top_k: usize,
    #[serde(default)]
    pub weights: Option<HybridWeights>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExplainResponse {
    pub result: Option<HybridSearchResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetDocumentResponse {
    pub document: Option<Document>,
}

#[derive(Debug, Deserialize)]
pub struct RebuildIndexRequest {
    #[serde(default)]
    pub index: Option<IndexType>,
}

#[derive(Debug, Deserialize)]
pub struct TenantQuotaRequest {
    pub max_collections: Option<usize>,
    pub max_disk_bytes: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_qps: Option<u32>,
}

impl From<TenantQuotaRequest> for TenantQuota {
    fn from(value: TenantQuotaRequest) -> Self {
        TenantQuota {
            max_collections: value.max_collections,
            max_disk_bytes: value.max_disk_bytes,
            max_memory_bytes: value.max_memory_bytes,
            max_qps: value.max_qps,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiKeyRequest {
    pub key: String,
    pub role: ApiRole,
}

pub fn build_router(storage: Storage) -> Router {
    build_router_with_auth(storage, ApiAuth::new())
}

pub fn build_router_with_auth(storage: Storage, auth: ApiAuth) -> Router {
    let cluster_config =
        ClusterConfig::from_env_or_default().expect("failed to load cluster config");
    let cluster = ClusterRouter::from_config(cluster_config).expect("invalid cluster config");
    build_router_with_state(storage, auth, cluster)
}

fn build_router_with_state(storage: Storage, auth: ApiAuth, cluster: ClusterRouter) -> Router {
    let state = AppState::new(storage, auth, cluster);
    build_router_from_state(state)
}

pub fn build_router_from_state(state: AppState) -> Router {

    let admin_state = AdminState::from(state.clone());

    Router::new()
        .route("/health", get(health))
        .route("/info", get(info))
        .route("/metrics", get(render_metrics))
        .route("/collections", post(create_collection))
        .route("/collections/:name", delete(drop_collection))
        .route("/collections/:name/documents", post(insert_document))
        .route(
            "/collections/:name/documents/:id",
            get(get_document).delete(delete_document),
        )
        .route(
            "/collections/:name/index/rebuild",
            post(rebuild_collection_index),
        )
        .route("/collections/:name/search", post(search_collection))
        .route(
            "/collections/:name/search/text",
            post(search_text_collection),
        )
        .route(
            "/collections/:name/search/hybrid",
            post(search_hybrid_collection),
        )
        .route(
            "/collections/:name/search/hybrid/explain",
            post(explain_hybrid_collection),
        )
        .route("/tenants/:tenant/usage", get(tenant_usage))
        .route("/tenants/:tenant/quota", put(set_tenant_quota))
        .route("/tenants/:tenant/api-keys", post(register_api_key))
        .nest("/admin", admin_routes().with_state(admin_state))
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http())
}

// ... Rest of handlers (health, info, etc.) ...


pub async fn start_server(
    listener: TcpListener,
    storage: Storage,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<Result<(), std::io::Error>> {
    start_server_with_auth(listener, storage, ApiAuth::new(), shutdown).await
}

pub async fn start_server_with_auth(
    listener: TcpListener,
    storage: Storage,
    auth: ApiAuth,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<Result<(), std::io::Error>> {
    let app = build_router_with_auth(storage, auth);
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
    })
}

pub async fn start_tls_server(
    addr: SocketAddr,
    storage: Storage,
    auth: ApiAuth,
    tls: TlsConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<JoinHandle<Result<(), std::io::Error>>, ApiError> {
    let app = build_router_with_auth(storage, auth);
    let rustls_config = tls.into_rustls_config().await?;
    let server_handle = axum_server::Handle::new();
    let shutdown_handle = server_handle.clone();
    tokio::spawn(async move {
        shutdown.await;
        shutdown_handle.graceful_shutdown(None);
    });

    Ok(tokio::spawn(async move {
        axum_server::bind_rustls(addr, rustls_config)
            .handle(server_handle)
            .serve(app.into_make_service())
            .await
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn render_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state
        .auth
        .authenticate(&headers, ApiPermission::Ops, None)?;
    let body = state.metrics.render();
    Ok((StatusCode::OK, body).into_response())
}

async fn info(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = state
        .auth
        .authenticate(&headers, ApiPermission::Read, None)?;
    state.ensure_local_for_tenant(&identity.tenant)?;
    let mut storage = state.storage.lock().await;
    let usage = storage.tenant_usage_report(&identity.tenant);
    let count = usage.collections;
    Ok(Json(serde_json::json!({
        "collections": count,
        "tenant": usage.tenant.as_str(),
        "usage": {
            "documents": usage.documents,
            "disk_bytes": usage.disk_bytes,
            "memory_bytes": usage.memory_bytes,
            "current_qps": usage.current_qps
        },
        "quota": usage.quota,
    })))
}

async fn create_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateCollectionRequest>,
) -> Result<StatusCode, ApiError> {
    if payload.dimension == 0 {
        return Err(ApiError::BadRequest("dimension must be positive".into()));
    }
    let mut fields = vec![FieldSchema {
        name: "vector".to_string(),
        field_type: FieldType::Vector {
            dimension: payload.dimension,
            metric: payload.metric,
            index: payload.index,
        },
        required: true,
    }];

    for text_field in payload.text_fields {
        fields.push(FieldSchema {
            name: text_field.name,
            field_type: FieldType::Text {
                indexed: text_field.indexed,
            },
            required: text_field.required,
        });
    }

    let identity = state
        .auth
        .authenticate(&headers, ApiPermission::TenantAdmin, None)?;
    let tenant = identity.tenant.clone();
    state.ensure_primary_for_tenant(&tenant)?;
    let schema = CollectionSchema {
        name: payload.name.clone(),
        fields,
        bm25_config: payload.bm25_config,
        tenant_id: tenant.clone(),
    };

    let mut storage = state.storage.lock().await;
    storage.create_collection_for_tenant(tenant, schema)?;
    audit_log(
        "create-collection",
        &identity,
        &format!("collection={} schema", payload.name),
    );
    metrics::counter!(
        "collection_operations_total",
        "operation" => "create",
        "tenant" => identity.tenant.as_str().to_string()
    ).increment(1);
    Ok(StatusCode::CREATED)
}

async fn drop_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let identity = state
        .auth
        .authenticate(&headers, ApiPermission::TenantAdmin, None)?;
    let tenant = identity.tenant.clone();
    state.ensure_primary_for_tenant(&tenant)?;
    let mut storage = state.storage.lock().await;
    storage.drop_collection_for_tenant(&tenant, &name)?;
    audit_log(
        "drop-collection",
        &identity,
        &format!("collection={}", name),
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn insert_document(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<InsertDocumentRequest>,
) -> Result<StatusCode, ApiError> {
    let identity = state
        .auth
        .authenticate(&headers, ApiPermission::Write, None)?;
    let tenant = identity.tenant.clone();
    let id_for_log = payload.id.clone();
    let document_id: DocumentId = payload.id.try_into()?;
    state.ensure_primary_for_document(&tenant, &document_id)?;
    let document = Document {
        id: document_id,
        vector: payload.vector,
        payload: payload.payload,
    };
    let mut storage = state.storage.lock().await;
    storage.insert_for_tenant(&tenant, &name, document, payload.upsert)?;
    audit_log(
        "insert-document",
        &identity,
        &format!("collection={} id={:?}", name, id_for_log),
    );
    metrics::counter!(
        "document_operations_total",
        "operation" => "insert",
        "collection" => name.clone(),
        "tenant" => identity.tenant.as_str().to_string()
    ).increment(1);
    Ok(StatusCode::CREATED)
}

async fn delete_document(
    AxumPath((name, id)): AxumPath<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let document_id: DocumentId = id.parse()?;
    let identity = state
        .auth
        .authenticate(&headers, ApiPermission::Write, None)?;
    let tenant = identity.tenant.clone();
    state.ensure_primary_for_document(&tenant, &document_id)?;
    let mut storage = state.storage.lock().await;
    let id_for_log = document_id.clone();
    let existed = storage.delete_for_tenant(&tenant, &name, document_id)?;
    audit_log(
        "delete-document",
        &identity,
        &format!("collection={} id={:?}", name, id_for_log),
    );
    Ok(if existed {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    })
}

async fn get_document(
    AxumPath((name, id)): AxumPath<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<GetDocumentResponse>, ApiError> {
    let document_id: DocumentId = id.parse()?;
    let identity = state
        .auth
        .authenticate(&headers, ApiPermission::Read, None)?;
    let tenant = identity.tenant.clone();
    
    // Check if primary? Read can usually be from anywhere, but consistent read might need primary.
    // For now, simple read.
    
    let storage = state.storage.lock().await; // Acquire lock
    // Note: Storage::get_document is synchronous? Yes.
    
    let doc = storage.get_document(&tenant, &name, &document_id)?;
    
    Ok(Json(GetDocumentResponse { document: doc }))
}


async fn rebuild_collection_index(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<RebuildIndexRequest>,
) -> Result<StatusCode, ApiError> {
    let identity = state
        .auth
        .authenticate(&headers, ApiPermission::TenantAdmin, None)?;
    let tenant = identity.tenant.clone();
    state.ensure_primary_for_tenant(&tenant)?;
    {
        let storage = state.storage.lock().await;
        storage.collection_schema_for_tenant(&tenant, &name)?;
    }

    let storage = state.storage.clone();
    let requested_index = payload.index.clone();
    let tenant_for_spawn = tenant.clone();
    let name_for_spawn = name.clone();
    tokio::spawn(async move {
        let mut storage = storage.lock().await;
        if let Err(err) =
            storage.rebuild_index_for_tenant(&tenant_for_spawn, &name_for_spawn, requested_index)
        {
            eprintln!("failed to rebuild index for {}: {}", name_for_spawn, err);
        }
    });

    audit_log("rebuild-index", &identity, &format!("collection={}", name));
    Ok(StatusCode::ACCEPTED)
}

async fn search_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    let tenant = state
        .auth
        .authenticate(&headers, ApiPermission::Read, None)?
        .tenant;
    state.ensure_local_for_tenant(&tenant)?;
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let start = Instant::now();
    let mut storage = state.storage.lock().await;
    let results = storage.search_for_tenant(
        &tenant,
        &name,
        &payload.vector,
        payload.top_k,
        payload.filter.as_ref(),
    )?;
    let duration = start.elapsed().as_secs_f64();
    metrics::histogram!("search_duration_seconds").record(duration);
    metrics::counter!(
        "search_requests_total",
        "type" => "vector",
        "collection" => name.clone(),
        "tenant" => tenant.as_str().to_string()
    ).increment(1);
    Ok(Json(SearchResponse { results }))
}

async fn search_text_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<TextSearchRequest>,
) -> Result<Json<TextSearchResponse>, ApiError> {
    let tenant = state
        .auth
        .authenticate(&headers, ApiPermission::Read, None)?
        .tenant;
    state.ensure_local_for_tenant(&tenant)?;
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let start = Instant::now();
    let mut storage = state.storage.lock().await;
    let results = storage.search_text_for_tenant(
        &tenant,
        &name,
        &payload.query,
        payload.top_k,
        payload.filter.as_ref(),
    )?;
    let duration = start.elapsed().as_secs_f64();
    metrics::histogram!("search_duration_seconds").record(duration);
    metrics::counter!(
        "search_requests_total",
        "type" => "text",
        "collection" => name.clone(),
        "tenant" => tenant.as_str().to_string()
    ).increment(1);
    Ok(Json(TextSearchResponse { results }))
}

async fn search_hybrid_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<HybridSearchRequest>,
) -> Result<Json<HybridSearchResponse>, ApiError> {
    let tenant = state
        .auth
        .authenticate(&headers, ApiPermission::Read, None)?
        .tenant;
    state.ensure_local_for_tenant(&tenant)?;
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let mut storage = state.storage.lock().await;
    let results = storage.search_hybrid_for_tenant(
        &tenant,
        &name,
        &payload.vector,
        &payload.query,
        payload.top_k,
        payload.weights,
        payload.filter.as_ref(),
    )?;
    Ok(Json(HybridSearchResponse { results }))
}

async fn explain_hybrid_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ExplainRequest>,
) -> Result<Json<ExplainResponse>, ApiError> {
    let tenant = state
        .auth
        .authenticate(&headers, ApiPermission::Read, None)?
        .tenant;
    state.ensure_local_for_tenant(&tenant)?;
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let id: DocumentId = payload.id.try_into()?;
    let mut storage = state.storage.lock().await;
    let result = storage.explain_hybrid_for_tenant(
        &tenant,
        &name,
        &payload.vector,
        &payload.query,
        payload.top_k,
        &id,
        payload.weights,
    )?;
    Ok(Json(ExplainResponse { result }))
}

async fn tenant_usage(
    AxumPath(tenant): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TenantUsageReport>, ApiError> {
    let tenant_id = TenantId::new(tenant);
    let _identity =
        state
            .auth
            .authenticate(&headers, ApiPermission::TenantAdmin, Some(&tenant_id))?;
    state.ensure_local_for_tenant(&tenant_id)?;
    let mut storage = state.storage.lock().await;
    let report = storage.tenant_usage_report(&tenant_id);
    Ok(Json(report))
}

async fn set_tenant_quota(
    AxumPath(tenant): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<TenantQuotaRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant_id = TenantId::new(tenant);
    let identity =
        state
            .auth
            .authenticate(&headers, ApiPermission::TenantAdmin, Some(&tenant_id))?;
    state.ensure_primary_for_tenant(&tenant_id)?;
    let mut storage = state.storage.lock().await;
    storage.set_tenant_quota(tenant_id, payload.into());
    audit_log(
        "set-tenant-quota",
        &identity,
        &format!("tenant={}", identity.tenant.as_str()),
    );
    Ok(StatusCode::ACCEPTED)
}

async fn register_api_key(
    AxumPath(tenant): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ApiKeyRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant_id = TenantId::new(tenant);
    let identity =
        state
            .auth
            .authenticate(&headers, ApiPermission::TenantAdmin, Some(&tenant_id))?;
    let role = payload.role.clone();
    state
        .auth
        .insert(payload.key, tenant_id.clone(), payload.role);
    audit_log(
        "register-api-key",
        &identity,
        &format!("tenant={} role={:?}", tenant_id.as_str(), role),
    );
    Ok(StatusCode::CREATED)
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use barq_cluster::{NodeConfig, NodeId, ReadPreference, ShardId, ShardPlacement};
    use barq_core::TenantId;
    use barq_index::DocumentId;
    use axum::http::{header, HeaderMap, HeaderValue};
    use reqwest::{Client, StatusCode};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::oneshot;
    use tokio::time::{sleep, Duration};

    fn sample_storage(dir: &Path) -> Storage {
        Storage::open(dir).unwrap()
    }

    async fn start_test_server(
        dir: &Path,
    ) -> (
        SocketAddr,
        oneshot::Sender<()>,
        JoinHandle<Result<(), std::io::Error>>,
    ) {
        start_test_server_with_auth(dir, ApiAuth::new()).await
    }

    async fn start_test_server_with_auth(
        dir: &Path,
        auth: ApiAuth,
    ) -> (
        SocketAddr,
        oneshot::Sender<()>,
        JoinHandle<Result<(), std::io::Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let storage = sample_storage(dir);
        let (tx, rx) = oneshot::channel();
        let handle = start_server_with_auth(listener, storage, auth, async move {
            let _ = rx.await;
        })
        .await;
        (addr, tx, handle)
    }

    async fn start_test_server_with_cluster(
        dir: &Path,
        cluster: ClusterRouter,
    ) -> (
        SocketAddr,
        oneshot::Sender<()>,
        JoinHandle<Result<(), std::io::Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let storage = sample_storage(dir);
        let app = super::build_router_with_state(storage, ApiAuth::new(), cluster);
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await
        });
        (addr, tx, handle)
    }

    struct MapJwtVerifier {
        claims: HashMap<String, (TenantId, ApiRole)>,
    }

    impl JwtVerifier for MapJwtVerifier {
        fn verify(&self, token: &str) -> Result<JwtClaims, ApiError> {
            let (tenant, role) = self
                .claims
                .get(token)
                .ok_or_else(|| ApiError::Unauthorized("invalid token".into()))?;
            Ok(JwtClaims {
                tenant: tenant.clone(),
                role: role.clone(),
                subject: Some(format!("subject:{token}")),
            })
        }
    }

    #[test]
    fn anonymous_allowed_when_keys_optional() {
        let auth = ApiAuth::new();
        let headers = HeaderMap::new();

        let identity = auth
            .authenticate(&headers, ApiPermission::Read, None)
            .expect("anonymous access should be allowed when no keys are required");

        assert_eq!(identity.role, ApiRole::Admin);
        assert_eq!(identity.method, AuthMethod::Anonymous);
    }

    #[test]
    fn bearer_token_rejected_without_verifier() {
        let auth = ApiAuth::new().require_keys();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer token-1"),
        );

        let err = auth
            .authenticate(&headers, ApiPermission::Read, None)
            .expect_err("jwt should be rejected when no verifier is configured");

        match err {
            ApiError::Unauthorized(message) => {
                assert!(message.contains("jwt auth not configured"));
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn missing_api_key_is_unauthorized() {
        let auth = ApiAuth::new().require_keys();
        let headers = HeaderMap::new();

        let err = auth
            .authenticate(&headers, ApiPermission::Read, None)
            .expect_err("calls without credentials must be rejected when keys are required");

        assert!(matches!(err, ApiError::Unauthorized(_)));
    }

    #[test]
    fn jwt_enforces_role_and_tenant_scope() {
        let claims = HashMap::from([(
            "jwt-token".to_string(),
            (TenantId::new("tenant-a"), ApiRole::Writer),
        )]);
        let auth = ApiAuth::new()
            .require_keys()
            .with_jwt_verifier(Arc::new(MapJwtVerifier { claims }));

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer jwt-token"),
        );

        let path_tenant = TenantId::new("tenant-a");
        let identity = auth
            .authenticate(&headers, ApiPermission::Write, Some(&path_tenant))
            .expect("writer role should allow write operations for matching tenant");
        assert_eq!(identity.role, ApiRole::Writer);
        assert_eq!(identity.tenant, path_tenant);

        let err = auth
            .authenticate(&headers, ApiPermission::Admin, Some(&path_tenant))
            .expect_err("writer role must not have admin access");
        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn tenant_mismatch_denied_for_jwt_and_header() {
        let claims = HashMap::from([(
            "jwt-token".to_string(),
            (TenantId::new("tenant-a"), ApiRole::Reader),
        )]);
        let auth = ApiAuth::new()
            .require_keys()
            .with_jwt_verifier(Arc::new(MapJwtVerifier { claims }));

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer jwt-token"),
        );
        headers.insert(
            "x-tenant-id",
            HeaderValue::from_static("tenant-b"),
        );

        let err = auth
            .authenticate(&headers, ApiPermission::Read, None)
            .expect_err("tenant mismatch should be forbidden");
        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn ops_role_cannot_access_admin_endpoints() {
        let auth = ApiAuth::new().require_keys();
        auth.insert("ops-key", TenantId::new("tenant-a"), ApiRole::Ops);

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("ops-key"));

        let err = auth
            .authenticate(&headers, ApiPermission::TenantAdmin, None)
            .expect_err("ops role must not be able to perform tenant admin actions");

        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn api_key_tenant_mismatch_is_forbidden() {
        let auth = ApiAuth::new().require_keys();
        auth.insert("key-1", TenantId::new("tenant-a"), ApiRole::TenantAdmin);

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("key-1"));

        let err = auth
            .authenticate(
                &headers,
                ApiPermission::TenantAdmin,
                Some(&TenantId::new("tenant-b")),
            )
            .expect_err("mismatched tenant must be forbidden");
        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn tls_config_validation_flags_missing_files() {
        let tls = TlsConfig::new("/missing/cert.pem", "/missing/key.pem");

        let err = tls
            .validate()
            .expect_err("missing files should trigger validation error");
        assert!(matches!(err, ApiError::Tls(_)));
    }

    #[test]
    fn tls_config_requires_existing_client_ca() {
        let tempdir = tempfile::tempdir().unwrap();
        let cert_path = tempdir.path().join("cert.pem");
        let key_path = tempdir.path().join("key.pem");
        std::fs::write(&cert_path, b"dummy").unwrap();
        std::fs::write(&key_path, b"dummy").unwrap();

        let tls = TlsConfig::new(&cert_path, &key_path).with_client_ca(tempdir.path().join("missing-ca.pem"));

        let err = tls
            .validate()
            .expect_err("client CA path must exist when provided");
        assert!(matches!(err, ApiError::Tls(_)));
    }

    #[test]
    fn tls_config_rejects_invalid_certificate_material() {
        let tempdir = tempfile::tempdir().unwrap();
        let cert_path = tempdir.path().join("cert.pem");
        let key_path = tempdir.path().join("key.pem");
        std::fs::write(&cert_path, b"not-a-pem").unwrap();
        std::fs::write(&key_path, b"still-not-a-pem").unwrap();

        let tls = TlsConfig::new(&cert_path, &key_path);
        let err = tls
            .build_server_config()
            .expect_err("invalid PEM material should fail TLS setup");

        assert!(matches!(err, ApiError::Tls(_)));
    }

    #[tokio::test]
    async fn integration_flow_restart_persists() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();

        let (addr, shutdown, handle) = start_test_server(dir.path()).await;
        let client = Client::new();

        let create_body = serde_json::json!({
            "name": "products",
            "dimension": 3,
            "metric": "Cosine"
        });
        client
            .post(format!("http://{}/collections", addr))
            .json(&create_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let insert_body = serde_json::json!({
            "id": 1,
            "vector": [0.0, 1.0, 0.0],
            "payload": {"name": "widget"}
        });
        client
            .post(format!("http://{}/collections/products/documents", addr))
            .json(&insert_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let search_body = serde_json::json!({
            "vector": [0.0, 1.0, 0.0],
            "top_k": 1
        });
        let response: SearchResponse = client
            .post(format!("http://{}/collections/products/search", addr))
            .json(&search_body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.results.len(), 1);

        // Shutdown and restart to trigger WAL replay
        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let (addr, shutdown, handle) = start_test_server(dir.path()).await;
        let response: SearchResponse = client
            .post(format!("http://{}/collections/products/search", addr))
            .json(&search_body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.results.len(), 1);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tenant_admin_endpoints_reject_ops_role() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let tenant = TenantId::new("secure-tenant");
        let auth = ApiAuth::new().require_keys();
        auth.insert("ops-key", tenant.clone(), ApiRole::Ops);

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let quota_body = serde_json::json!({
            "max_collections": 1,
            "max_disk_bytes": 1,
            "max_memory_bytes": 1,
            "max_qps": 1
        });

        let response = client
            .put(format!("http://{}/tenants/{}/quota", addr, tenant.as_str()))
            .header("x-api-key", "ops-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&quota_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn jwt_roles_enforced_for_admin_endpoints() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let tenant = TenantId::new("jwt-tenant");
        let claims = HashMap::from([(
            "writer-token".to_string(),
            (tenant.clone(), ApiRole::Writer),
        )]);
        let auth = ApiAuth::new()
            .require_keys()
            .with_jwt_verifier(Arc::new(MapJwtVerifier { claims }));

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let quota_body = serde_json::json!({
            "max_collections": 1,
            "max_disk_bytes": 1,
            "max_memory_bytes": 1,
            "max_qps": 1
        });

        let response = client
            .put(format!("http://{}/tenants/{}/quota", addr, tenant.as_str()))
            .header("Authorization", "Bearer writer-token")
            .json(&quota_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn writer_cannot_create_collection() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let tenant = TenantId::new("writer-tenant");
        let auth = ApiAuth::new().require_keys();
        auth.insert("writer-key", tenant.clone(), ApiRole::Writer);

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let create_body = serde_json::json!({
            "name": "blocked",
            "dimension": 3,
            "metric": "Cosine"
        });

        let response = client
            .post(format!("http://{}/collections", addr))
            .header("x-api-key", "writer-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&create_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn read_routes_follow_configured_preference() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();

        let cluster_config = ClusterConfig {
            node_id: NodeId::new("node-2"),
            nodes: vec![
                NodeConfig {
                    id: NodeId::new("node-0"),
                    address: "http://node-0".into(),
                },
                NodeConfig {
                    id: NodeId::new("node-1"),
                    address: "http://node-1".into(),
                },
                NodeConfig {
                    id: NodeId::new("node-2"),
                    address: "http://node-2".into(),
                },
            ],
            shard_count: 1,
            replication_factor: 2,
            read_preference: ReadPreference::Followers,
            placements: HashMap::from([(
                ShardId(0),
                ShardPlacement {
                    shard: ShardId(0),
                    primary: NodeId::new("node-0"),
                    replicas: vec![NodeId::new("node-1")],
                },
            )]),
        };
        let cluster = ClusterRouter::from_config(cluster_config).unwrap();

        let (addr, shutdown, handle) = start_test_server_with_cluster(dir.path(), cluster).await;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        let response = client
            .get(format!("http://{}/info", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .expect("location header");
        assert_eq!(location.to_str().unwrap(), "http://node-1");

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn multi_tenant_isolation_and_usage_reporting() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let auth = ApiAuth::new().require_keys();
        auth.insert("key-a", TenantId::new("tenant-a"), ApiRole::Admin);
        auth.insert("key-b", TenantId::new("tenant-b"), ApiRole::Admin);

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let create_body = serde_json::json!({
            "name": "products",
            "dimension": 3,
            "metric": "Cosine"
        });

        for (key, tenant) in [("key-a", "tenant-a"), ("key-b", "tenant-b")] {
            client
                .post(format!("http://{}/collections", addr))
                .header("x-api-key", key)
                .header("x-tenant-id", tenant)
                .json(&create_body)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
        }

        let insert_a = serde_json::json!({
            "id": 1,
            "vector": [0.0, 1.0, 0.0],
            "payload": {"name": "widget a"}
        });
        client
            .post(format!("http://{}/collections/products/documents", addr))
            .header("x-api-key", "key-a")
            .header("x-tenant-id", "tenant-a")
            .json(&insert_a)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let insert_b = serde_json::json!({
            "id": 2,
            "vector": [1.0, 0.0, 0.0],
            "payload": {"name": "widget b"}
        });
        client
            .post(format!("http://{}/collections/products/documents", addr))
            .header("x-api-key", "key-b")
            .header("x-tenant-id", "tenant-b")
            .json(&insert_b)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let search_body_a = serde_json::json!({"vector": [0.0, 1.0, 0.0], "top_k": 1});
        let search_a: SearchResponse = client
            .post(format!("http://{}/collections/products/search", addr))
            .header("x-api-key", "key-a")
            .header("x-tenant-id", "tenant-a")
            .json(&search_body_a)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(search_a.results.len(), 1);
        assert_eq!(search_a.results[0].id, DocumentId::U64(1));

        let search_body_b = serde_json::json!({"vector": [0.0, 1.0, 0.0], "top_k": 1});
        let search_b: SearchResponse = client
            .post(format!("http://{}/collections/products/search", addr))
            .header("x-api-key", "key-b")
            .header("x-tenant-id", "tenant-b")
            .json(&search_body_b)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(search_b.results.len(), 1);
        assert_eq!(search_b.results[0].id, DocumentId::U64(2));

        let usage: TenantUsageReport = client
            .get(format!("http://{}/tenants/tenant-a/usage", addr))
            .header("x-api-key", "key-a")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(usage.documents, 1);
        assert_eq!(usage.tenant.as_str(), "tenant-a");

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn metrics_expose_per_tenant_usage() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let auth = ApiAuth::new().require_keys();
        let tenant = TenantId::new("metrics-tenant");
        auth.insert("metrics-key", tenant.clone(), ApiRole::Admin);

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let create_body = serde_json::json!({
            "name": "metrics-coll",
            "dimension": 3,
            "metric": "Cosine"
        });

        client
            .post(format!("http://{}/collections", addr))
            .header("x-api-key", "metrics-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&create_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let insert_body = serde_json::json!({
            "id": 42,
            "vector": [0.1, 0.2, 0.3],
            "payload": {"tag": "test"}
        });

        client
            .post(format!(
                "http://{}/collections/metrics-coll/documents",
                addr
            ))
            .header("x-api-key", "metrics-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&insert_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        // Touch usage reporting to ensure gauges are updated.
        let _: TenantUsageReport = client
            .get(format!("http://{}/tenants/{}/usage", addr, tenant.as_str()))
            .header("x-api-key", "metrics-key")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let metrics_text = client
            .get(format!("http://{}/metrics", addr))
            .header("x-api-key", "metrics-key")
            .header("x-tenant-id", tenant.as_str())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(metrics_text.contains("tenant_usage_documents{tenant=\"metrics-tenant\"}"));
        assert!(metrics_text.contains("tenant_requests_total{tenant=\"metrics-tenant\"}"));

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quota_limits_enforced_per_tenant() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let auth = ApiAuth::new().require_keys();
        let tenant = TenantId::new("quota-tenant");
        auth.insert("quota-key", tenant.clone(), ApiRole::Admin);

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let quota_body = serde_json::json!({
            "max_collections": 1,
            "max_disk_bytes": 128,
            "max_memory_bytes": 128,
            "max_qps": 10
        });

        client
            .put(format!("http://{}/tenants/{}/quota", addr, tenant.as_str()))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&quota_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let create_body = serde_json::json!({
            "name": "limited", "dimension": 3, "metric": "Cosine"
        });

        client
            .post(format!("http://{}/collections", addr))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&create_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let second = client
            .post(format!("http://{}/collections", addr))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&serde_json::json!({"name": "too-many", "dimension": 3, "metric": "Cosine"}))
            .send()
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

        let insert_body = serde_json::json!({
            "id": 1,
            "vector": [0.0, 1.0, 0.0],
            "payload": {"blob": "ok"}
        });
        client
            .post(format!("http://{}/collections/limited/documents", addr))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&insert_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        sleep(Duration::from_secs(1)).await;
        let qps_quota = serde_json::json!({
            "max_collections": 1,
            "max_disk_bytes": 128,
            "max_memory_bytes": 128,
            "max_qps": 1
        });
        client
            .put(format!("http://{}/tenants/{}/quota", addr, tenant.as_str()))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&qps_quota)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let allowed = client
            .post(format!("http://{}/collections/limited/documents", addr))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&serde_json::json!({"id": 2, "vector": [0.1, 0.1, 0.1], "payload": {"blob": "qps"}}))
            .send()
            .await
            .unwrap();
        assert!(allowed.status().is_success());

        let burst = client
            .post(format!("http://{}/collections/limited/documents", addr))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&serde_json::json!({"id": 3, "vector": [0.2, 0.2, 0.2], "payload": {"blob": "second"}}))
            .send()
            .await
            .unwrap();
        assert_eq!(burst.status(), StatusCode::TOO_MANY_REQUESTS);

        sleep(Duration::from_secs(1)).await;
        let tighter_quota = serde_json::json!({
            "max_collections": 1,
            "max_disk_bytes": 16,
            "max_memory_bytes": 16,
            "max_qps": 5
        });
        client
            .put(format!("http://{}/tenants/{}/quota", addr, tenant.as_str()))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&tighter_quota)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let oversized = client
            .post(format!("http://{}/collections/limited/documents", addr))
            .header("x-api-key", "quota-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&serde_json::json!({
                "id": 4,
                "vector": [0.3, 0.3, 0.3],
                "payload": {"blob": "this payload is intentionally oversized for quotas"},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::TOO_MANY_REQUESTS);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn writer_role_cannot_manage_collections() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let tenant = TenantId::new("rbac-tenant");
        let auth = ApiAuth::new().require_keys();
        auth.insert("admin-key", tenant.clone(), ApiRole::TenantAdmin);
        auth.insert("writer-key", tenant.clone(), ApiRole::Writer);

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let create_body = serde_json::json!({
            "name": "managed",
            "dimension": 3,
            "metric": "Cosine"
        });

        client
            .post(format!("http://{}/collections", addr))
            .header("x-api-key", "admin-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&create_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let forbidden = client
            .post(format!("http://{}/collections", addr))
            .header("x-api-key", "writer-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&create_body)
            .send()
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let insert_body = serde_json::json!({
            "id": 42,
            "vector": [0.3, 0.1, 0.2],
            "payload": {"mode": "writer"}
        });

        client
            .post(format!("http://{}/collections/managed/documents", addr))
            .header("x-api-key", "writer-key")
            .header("x-tenant-id", tenant.as_str())
            .json(&insert_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn jwt_tokens_respect_roles() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let tenant = TenantId::new("jwt-tenant");
        let mut claims = HashMap::new();
        claims.insert(
            "tenant-admin".into(),
            (tenant.clone(), ApiRole::TenantAdmin),
        );
        claims.insert("reader-token".into(), (tenant.clone(), ApiRole::Reader));

        let verifier = MapJwtVerifier { claims };
        let auth = ApiAuth::new()
            .require_keys()
            .with_jwt_verifier(Arc::new(verifier));

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let create_body = serde_json::json!({
            "name": "jwt-coll",
            "dimension": 3,
            "metric": "Cosine"
        });

        client
            .post(format!("http://{}/collections", addr))
            .header("Authorization", "Bearer tenant-admin")
            .json(&create_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let insert_body = serde_json::json!({
            "id": 5,
            "vector": [0.2, 0.4, 0.6],
            "payload": {"tag": "jwt"}
        });

        client
            .post(format!("http://{}/collections/jwt-coll/documents", addr))
            .header("Authorization", "Bearer tenant-admin")
            .json(&insert_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let search_body = serde_json::json!({
            "vector": [0.2, 0.4, 0.6],
            "top_k": 1
        });

        let search = client
            .post(format!("http://{}/collections/jwt-coll/search", addr))
            .header("Authorization", "Bearer reader-token")
            .json(&search_body)
            .send()
            .await
            .unwrap();
        assert_eq!(search.status(), StatusCode::OK);

        let metrics = client
            .get(format!("http://{}/metrics", addr))
            .header("Authorization", "Bearer reader-token")
            .send()
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::FORBIDDEN);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn missing_credentials_denied_when_required() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let auth = ApiAuth::new().require_keys();

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let response = client
            .get(format!("http://{}/info", addr))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tenant_mismatch_results_in_forbidden() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let tenant_a = TenantId::new("tenant-a");
        let tenant_b = TenantId::new("tenant-b");
        let auth = ApiAuth::new().require_keys();
        auth.insert("tenant-a-key", tenant_a.clone(), ApiRole::TenantAdmin);

        let (addr, shutdown, handle) = start_test_server_with_auth(dir.path(), auth).await;
        let client = Client::new();

        let forbidden = client
            .get(format!("http://{}/tenants/{}/usage", addr, tenant_b.as_str()))
            .header("x-api-key", "tenant-a-key")
            .send()
            .await
            .unwrap();

        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_server_rejects_missing_material() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let storage = sample_storage(dir.path());
        let tls = TlsConfig::new("/missing/cert.pem", "/missing/key.pem");

        let err = start_tls_server(
            "127.0.0.1:0".parse().unwrap(),
            storage,
            ApiAuth::new(),
            tls,
            async {},
        )
        .await
        .expect_err("server startup should fail when TLS material is missing");

        assert!(matches!(err, ApiError::Tls(_)));
    }

    #[test]
    fn tls_config_validation_catches_missing_material() {
        let config = TlsConfig::new("/no/such/cert.pem", "/no/such/key.pem");
        let err = config.validate().unwrap_err();
        assert!(matches!(err, ApiError::Tls(_)));
    }

    #[tokio::test]
    async fn text_and_hybrid_search() {
        init_tracing();
        let dir = tempfile::tempdir().unwrap();
        let (addr, shutdown, handle) = start_test_server(dir.path()).await;
        let client = Client::new();

        let create_body = serde_json::json!({
            "name": "docs",
            "dimension": 3,
            "metric": "Cosine",
            "text_fields": [
                {"name": "body", "indexed": true, "required": true}
            ]
        });

        client
            .post(format!("http://{}/collections", addr))
            .json(&create_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let insert_body = serde_json::json!({
            "id": 1,
            "vector": [0.0, 1.0, 0.0],
            "payload": {"body": "Rust systems programming"}
        });
        client
            .post(format!("http://{}/collections/docs/documents", addr))
            .json(&insert_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let insert_body2 = serde_json::json!({
            "id": 2,
            "vector": [1.0, 0.0, 0.0],
            "payload": {"body": "Database systems"}
        });
        client
            .post(format!("http://{}/collections/docs/documents", addr))
            .json(&insert_body2)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let search_body = serde_json::json!({"query": "rust systems", "top_k": 2});
        let text_response: TextSearchResponse = client
            .post(format!("http://{}/collections/docs/search/text", addr))
            .json(&search_body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(text_response.results[0].id, DocumentId::U64(1));

        let hybrid_body = serde_json::json!({
            "vector": [0.0, 1.0, 0.0],
            "query": "rust systems",
            "top_k": 2
        });
        let hybrid_response: HybridSearchResponse = client
            .post(format!("http://{}/collections/docs/search/hybrid", addr))
            .json(&hybrid_body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(hybrid_response.results.len(), 2);
        assert!(hybrid_response.results[0].bm25_score.is_some());
        assert!(hybrid_response.results[0].vector_score.is_some());

        let explain_body = serde_json::json!({
            "id": 1,
            "vector": [0.0, 1.0, 0.0],
            "query": "rust systems",
            "top_k": 2
        });
        let explain_response: ExplainResponse = client
            .post(format!(
                "http://{}/collections/docs/search/hybrid/explain",
                addr
            ))
            .json(&explain_body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(explain_response.result.is_some());

        shutdown.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }
}
