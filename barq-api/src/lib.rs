use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};

use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use barq_bm25::Bm25Config;
use barq_cluster::{ClusterConfig, ClusterError, ClusterRouter};
use barq_core::{
    CollectionSchema, Document, FieldSchema, FieldType, Filter, HybridSearchResult, HybridWeights,
    PayloadValue, TenantId,
};
use barq_index::{DistanceMetric, DocumentId, DocumentIdError, IndexType};
use barq_storage::{Storage, TenantQuota, TenantUsageReport};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{self, RootCertStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    storage: Arc<Mutex<Storage>>,
    auth: ApiAuth,
    metrics: PrometheusHandle,
    cluster: ClusterRouter,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,
}

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

impl TlsConfig {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            client_ca_path: None,
        }
    }

    pub fn with_client_ca(mut self, ca_path: impl Into<PathBuf>) -> Self {
        self.client_ca_path = Some(ca_path.into());
        self
    }

    pub fn validate(&self) -> Result<(), ApiError> {
        if !self.cert_path.exists() {
            return Err(ApiError::Tls("certificate path does not exist".into()));
        }
        if !self.key_path.exists() {
            return Err(ApiError::Tls("private key path does not exist".into()));
        }
        if let Some(ca) = &self.client_ca_path {
            if !ca.exists() {
                return Err(ApiError::Tls("client CA path does not exist".into()));
            }
        }
        Ok(())
    }

    fn load_certificates(&self) -> Result<Vec<CertificateDer<'static>>, ApiError> {
        let cert_file = File::open(&self.cert_path)
            .map_err(|err| ApiError::Tls(format!("failed to open cert: {}", err)))?;
        let mut reader = BufReader::new(cert_file);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| ApiError::Tls(format!("failed to parse cert: {}", err)))?;
        if certs.is_empty() {
            return Err(ApiError::Tls("no certificates found".into()));
        }
        Ok(certs)
    }

    fn load_private_key(&self) -> Result<PrivateKeyDer<'static>, ApiError> {
        let key_file = File::open(&self.key_path)
            .map_err(|err| ApiError::Tls(format!("failed to open key: {}", err)))?;
        let mut reader = BufReader::new(key_file);
        rustls_pemfile::private_key(&mut reader)
            .map_err(|err| ApiError::Tls(format!("failed to parse key: {}", err)))?
            .ok_or_else(|| ApiError::Tls("no private key found".into()))
    }

    fn load_client_ca(&self) -> Result<RootCertStore, ApiError> {
        let mut store = RootCertStore::empty();
        if let Some(path) = &self.client_ca_path {
            let ca_file = File::open(path)
                .map_err(|err| ApiError::Tls(format!("failed to open client CA: {}", err)))?;
            let mut reader = BufReader::new(ca_file);
            let certs = rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| ApiError::Tls(format!("failed to parse client CA: {}", err)))?;
            for cert in certs {
                store
                    .add(cert)
                    .map_err(|err| ApiError::Tls(format!("invalid client CA: {}", err)))?;
            }
        }
        Ok(store)
    }

    pub fn build_server_config(&self) -> Result<rustls::ServerConfig, ApiError> {
        self.validate()?;
        let certs = self.load_certificates()?;
        let key = self.load_private_key()?;

        let builder = rustls::ServerConfig::builder();
        let server_config = if self.client_ca_path.is_some() {
            let client_ca = self.load_client_ca()?;
            let verifier = WebPkiClientVerifier::builder(client_ca.into())
                .build()
                .map_err(|err| ApiError::Tls(format!("invalid client verifier: {}", err)))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|err| ApiError::Tls(format!("invalid tls config: {}", err)))?
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|err| ApiError::Tls(format!("invalid tls config: {}", err)))?
        };

        Ok(server_config)
    }

    pub async fn into_rustls_config(&self) -> Result<RustlsConfig, ApiError> {
        let server_config = self.build_server_config()?;
        Ok(RustlsConfig::from_config(Arc::new(server_config)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ApiRole {
    Admin,
    Ops,
    TenantAdmin,
    Writer,
    Reader,
}

impl ApiRole {
    fn allows(&self, required: &ApiPermission) -> bool {
        match (self, required) {
            (ApiRole::Admin, _) => true,
            (ApiRole::Ops, ApiPermission::Ops) => true,
            (ApiRole::TenantAdmin, ApiPermission::TenantAdmin)
            | (ApiRole::TenantAdmin, ApiPermission::Write)
            | (ApiRole::TenantAdmin, ApiPermission::Read) => true,
            (ApiRole::Writer, ApiPermission::Write) | (ApiRole::Writer, ApiPermission::Read) => {
                true
            }
            (ApiRole::Reader, ApiPermission::Read) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiPermission {
    Admin,
    Ops,
    TenantAdmin,
    Write,
    Read,
}

#[derive(Debug, Clone)]
struct ApiKey {
    tenant: TenantId,
    role: ApiRole,
}

#[derive(Clone, Default)]
pub struct ApiAuth {
    keys: Arc<RwLock<HashMap<String, ApiKey>>>,
    require_keys: bool,
    jwt_verifier: Option<Arc<dyn JwtVerifier>>,
}

impl std::fmt::Debug for ApiAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiAuth")
            .field("keys", &"<redacted>")
            .field("require_keys", &self.require_keys)
            .field("jwt_verifier", &self.jwt_verifier.is_some())
            .finish()
    }
}

impl ApiAuth {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn require_keys(mut self) -> Self {
        self.require_keys = true;
        self
    }

    pub fn insert(&self, key: impl Into<String>, tenant: TenantId, role: ApiRole) {
        let mut guard = self.keys.write().expect("auth lock poisoned");
        guard.insert(key.into(), ApiKey { tenant, role });
    }

    pub fn with_jwt_verifier(mut self, verifier: Arc<dyn JwtVerifier>) -> Self {
        self.jwt_verifier = Some(verifier);
        self
    }

    fn authenticate(
        &self,
        headers: &HeaderMap,
        required: ApiPermission,
        path_tenant: Option<&TenantId>,
    ) -> Result<ApiIdentity, ApiError> {
        let guard = self.keys.read().expect("auth lock poisoned");
        let fallback_allowed =
            !self.require_keys && guard.is_empty() && self.jwt_verifier.is_none();
        let requested_header_tenant = tenant_header(headers);
        if fallback_allowed {
            let tenant = path_tenant
                .cloned()
                .or_else(|| requested_header_tenant.clone())
                .unwrap_or_default();
            return Ok(ApiIdentity {
                tenant,
                role: ApiRole::Admin,
                actor: Some("anonymous".to_string()),
                method: AuthMethod::Anonymous,
            });
        }

        if let Some(token) = bearer_token(headers) {
            let verifier = self
                .jwt_verifier
                .as_ref()
                .ok_or_else(|| ApiError::Unauthorized("jwt auth not configured".into()))?;
            let claims = verifier.verify(token)?;
            enforce_tenant_constraints(
                path_tenant,
                requested_header_tenant.as_ref(),
                &claims.tenant,
            )?;
            if !claims.role.allows(&required) {
                return Err(ApiError::Forbidden("insufficient role".into()));
            }
            return Ok(ApiIdentity {
                tenant: claims.tenant,
                role: claims.role,
                actor: claims.subject,
                method: AuthMethod::Jwt,
            });
        }

        let api_key = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .ok_or(ApiError::Unauthorized("missing api key".into()))?;

        let record = guard
            .get(api_key)
            .ok_or_else(|| ApiError::Unauthorized("invalid api key".into()))?;
        enforce_tenant_constraints(
            path_tenant,
            requested_header_tenant.as_ref(),
            &record.tenant,
        )?;
        if !record.role.allows(&required) {
            return Err(ApiError::Forbidden("insufficient role".into()));
        }

        Ok(ApiIdentity {
            tenant: record.tenant.clone(),
            role: record.role.clone(),
            actor: Some(redact_key(api_key)),
            method: AuthMethod::ApiKey,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JwtClaims {
    pub tenant: TenantId,
    pub role: ApiRole,
    pub subject: Option<String>,
}

pub trait JwtVerifier: Send + Sync {
    fn verify(&self, token: &str) -> Result<JwtClaims, ApiError>;
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

#[derive(Debug, Clone)]
struct ApiIdentity {
    tenant: TenantId,
    role: ApiRole,
    actor: Option<String>,
    method: AuthMethod,
}

fn tenant_header(headers: &HeaderMap) -> Option<TenantId> {
    headers
        .get("x-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(TenantId::new)
}

#[derive(Debug, Clone, Copy)]
enum AuthMethod {
    Anonymous,
    ApiKey,
    Jwt,
}

impl AuthMethod {
    fn as_str(&self) -> &'static str {
        match self {
            AuthMethod::Anonymous => "anonymous",
            AuthMethod::ApiKey => "api-key",
            AuthMethod::Jwt => "jwt",
        }
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn enforce_tenant_constraints(
    path_tenant: Option<&TenantId>,
    header_tenant: Option<&TenantId>,
    identity_tenant: &TenantId,
) -> Result<(), ApiError> {
    if let Some(path) = path_tenant {
        if path != identity_tenant {
            return Err(ApiError::Forbidden("tenant mismatch".into()));
        }
    }

    if let Some(header) = header_tenant {
        if header != identity_tenant {
            return Err(ApiError::Forbidden("tenant header mismatch".into()));
        }
    }

    Ok(())
}

fn redact_key(key: &str) -> String {
    let len = key.chars().count();
    if len <= 4 {
        return "****".to_string();
    }
    let prefix: String = key.chars().take(4).collect();
    format!("{}***", prefix)
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

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("storage error: {0}")]
    Storage(#[from] barq_storage::StorageError),

    #[error("cluster error: {0}")]
    Cluster(#[from] ClusterError),

    #[error("document id error: {0}")]
    DocumentId(#[from] DocumentIdError),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("tls configuration error: {0}")]
    Tls(String),

    #[error("redirecting to leader at {0}")]
    Redirect(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::Storage(barq_storage::StorageError::Catalog(_))
            | ApiError::DocumentId(_)
            | ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::Redirect(_) => StatusCode::TEMPORARY_REDIRECT,
            ApiError::Cluster(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Tls(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Storage(barq_storage::StorageError::QuotaExceeded { .. }) => {
                StatusCode::TOO_MANY_REQUESTS
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut response = (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response();

        if let ApiError::Redirect(address) = &self {
            if let Ok(header_value) = axum::http::HeaderValue::from_str(address) {
                response
                    .headers_mut()
                    .insert(axum::http::header::LOCATION, header_value);
            }
        }

        response
    }
}

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
    let metrics = init_metrics_recorder();
    let state = AppState {
        storage: Arc::new(Mutex::new(storage)),
        auth,
        metrics,
        cluster,
    };

    Router::new()
        .route("/health", get(health))
        .route("/info", get(info))
        .route("/metrics", get(render_metrics))
        .route("/collections", post(create_collection))
        .route("/collections/:name", delete(drop_collection))
        .route("/collections/:name/documents", post(insert_document))
        .route("/collections/:name/documents/:id", delete(delete_document))
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
        .with_state(state)
}

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
    let mut storage = state.storage.lock().await;
    let results = storage.search_for_tenant(
        &tenant,
        &name,
        &payload.vector,
        payload.top_k,
        payload.filter.as_ref(),
    )?;
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
    let mut storage = state.storage.lock().await;
    let results = storage.search_text_for_tenant(
        &tenant,
        &name,
        &payload.query,
        payload.top_k,
        payload.filter.as_ref(),
    )?;
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
            .header(axum::http::header::AUTHORIZATION, "Bearer tenant-admin")
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
            .header(axum::http::header::AUTHORIZATION, "Bearer tenant-admin")
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
            .header(axum::http::header::AUTHORIZATION, "Bearer reader-token")
            .json(&search_body)
            .send()
            .await
            .unwrap();
        assert_eq!(search.status(), StatusCode::OK);

        let metrics = client
            .get(format!("http://{}/metrics", addr))
            .header(axum::http::header::AUTHORIZATION, "Bearer reader-token")
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
