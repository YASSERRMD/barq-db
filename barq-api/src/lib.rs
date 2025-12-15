use std::future::Future;
use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use barq_bm25::Bm25Config;
use barq_core::{
    CollectionSchema, Document, FieldSchema, FieldType, Filter, HybridSearchResult, HybridWeights,
    PayloadValue, TenantId,
};
use barq_index::{DistanceMetric, DocumentId, DocumentIdError, IndexType};
use barq_storage::Storage;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    storage: Arc<Mutex<Storage>>,
}

fn tenant_from_headers(headers: &HeaderMap) -> TenantId {
    headers
        .get("x-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(TenantId::new)
        .unwrap_or_default()
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("storage error: {0}")]
    Storage(#[from] barq_storage::StorageError),

    #[error("document id error: {0}")]
    DocumentId(#[from] DocumentIdError),

    #[error("bad request: {0}")]
    BadRequest(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::Storage(barq_storage::StorageError::Catalog(_))
            | ApiError::DocumentId(_)
            | ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response()
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

#[derive(Debug, Deserialize)]
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

pub fn build_router(storage: Storage) -> Router {
    let state = AppState {
        storage: Arc::new(Mutex::new(storage)),
    };

    Router::new()
        .route("/health", get(health))
        .route("/info", get(info))
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
        .with_state(state)
}

pub async fn start_server(
    listener: TcpListener,
    storage: Storage,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<Result<(), std::io::Error>> {
    let app = build_router(storage);
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
    })
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn info(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = tenant_from_headers(&headers);
    let storage = state.storage.lock().await;
    let count = storage.collection_names_for_tenant(&tenant)?.len();
    Ok(Json(
        serde_json::json!({ "collections": count, "tenant": tenant.as_str() }),
    ))
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

    let tenant = tenant_from_headers(&headers);
    let schema = CollectionSchema {
        name: payload.name.clone(),
        fields,
        bm25_config: payload.bm25_config,
        tenant_id: tenant.clone(),
    };

    let mut storage = state.storage.lock().await;
    storage.create_collection_for_tenant(tenant, schema)?;
    Ok(StatusCode::CREATED)
}

async fn drop_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_from_headers(&headers);
    let mut storage = state.storage.lock().await;
    storage.drop_collection_for_tenant(&tenant, &name)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn insert_document(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<InsertDocumentRequest>,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_from_headers(&headers);
    let document_id: DocumentId = payload.id.try_into()?;
    let document = Document {
        id: document_id,
        vector: payload.vector,
        payload: payload.payload,
    };
    let mut storage = state.storage.lock().await;
    storage.insert_for_tenant(&tenant, &name, document, payload.upsert)?;
    Ok(StatusCode::CREATED)
}

async fn delete_document(
    AxumPath((name, id)): AxumPath<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let document_id: DocumentId = id.parse()?;
    let mut storage = state.storage.lock().await;
    let tenant = tenant_from_headers(&headers);
    let existed = storage.delete_for_tenant(&tenant, &name, document_id)?;
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
    let tenant = tenant_from_headers(&headers);
    {
        let storage = state.storage.lock().await;
        storage.collection_schema_for_tenant(&tenant, &name)?;
    }

    let storage = state.storage.clone();
    let requested_index = payload.index.clone();
    tokio::spawn(async move {
        let mut storage = storage.lock().await;
        if let Err(err) = storage.rebuild_index_for_tenant(&tenant, &name, requested_index) {
            eprintln!("failed to rebuild index for {}: {}", name, err);
        }
    });

    Ok(StatusCode::ACCEPTED)
}

async fn search_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers);
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let storage = state.storage.lock().await;
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
    let tenant = tenant_from_headers(&headers);
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let storage = state.storage.lock().await;
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
    let tenant = tenant_from_headers(&headers);
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let storage = state.storage.lock().await;
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
    let tenant = tenant_from_headers(&headers);
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let id: DocumentId = payload.id.try_into()?;
    let storage = state.storage.lock().await;
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

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use barq_index::DocumentId;
    use reqwest::Client;
    use std::net::SocketAddr;
    use std::path::Path;
    use tokio::sync::oneshot;

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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let storage = sample_storage(dir);
        let (tx, rx) = oneshot::channel();
        let handle = start_server(listener, storage, async move {
            let _ = rx.await;
        })
        .await;
        (addr, tx, handle)
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
