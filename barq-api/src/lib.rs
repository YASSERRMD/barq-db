use std::future::Future;
use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use barq_core::{CollectionSchema, Document, FieldSchema, FieldType, PayloadValue};
use barq_index::{DistanceMetric, DocumentId, DocumentIdError};
use barq_storage::Storage;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    storage: Arc<Mutex<Storage>>,
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
    pub filter: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<barq_index::SearchResult>,
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
        .route("/collections/:name/search", post(search_collection))
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

async fn info(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let storage = state.storage.lock().await;
    let count = storage.collection_names()?.len();
    Ok(Json(serde_json::json!({ "collections": count })))
}

async fn create_collection(
    State(state): State<AppState>,
    Json(payload): Json<CreateCollectionRequest>,
) -> Result<StatusCode, ApiError> {
    if payload.dimension == 0 {
        return Err(ApiError::BadRequest("dimension must be positive".into()));
    }
    let schema = CollectionSchema {
        name: payload.name.clone(),
        fields: vec![FieldSchema {
            name: "vector".to_string(),
            field_type: FieldType::Vector {
                dimension: payload.dimension,
                metric: payload.metric,
            },
            required: true,
        }],
    };

    let mut storage = state.storage.lock().await;
    storage.create_collection(schema)?;
    Ok(StatusCode::CREATED)
}

async fn drop_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    let mut storage = state.storage.lock().await;
    storage.drop_collection(&name)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn insert_document(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    Json(payload): Json<InsertDocumentRequest>,
) -> Result<StatusCode, ApiError> {
    let document_id: DocumentId = payload.id.try_into()?;
    let document = Document {
        id: document_id,
        vector: payload.vector,
        payload: payload.payload,
    };
    let mut storage = state.storage.lock().await;
    storage.insert(&name, document, payload.upsert)?;
    Ok(StatusCode::CREATED)
}

async fn delete_document(
    AxumPath((name, id)): AxumPath<(String, String)>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    let document_id: DocumentId = id.parse()?;
    let mut storage = state.storage.lock().await;
    let existed = storage.delete(&name, document_id)?;
    Ok(if existed {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    })
}

async fn search_collection(
    AxumPath(name): AxumPath<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    if payload.top_k == 0 {
        return Err(ApiError::BadRequest("top_k must be positive".into()));
    }
    let _ = payload.filter;
    let storage = state.storage.lock().await;
    let results = storage.search(&name, &payload.vector, payload.top_k)?;
    Ok(Json(SearchResponse { results }))
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
