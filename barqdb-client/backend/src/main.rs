use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::info;
use axum::{
    Router,
    routing::{get, post},
    extract::State as AxumState,
    Json,
    http::StatusCode,
};
use tower_http::cors::{CorsLayer, Any};
use serde::{Deserialize, Serialize};

// Generated code from proto files
pub mod barq {
    tonic::include_proto!("barq");
}

pub mod barqclient {
    tonic::include_proto!("barqclient");
}

use barq::barq_client::BarqClient;

type GrpcClient = Arc<Mutex<BarqClient<tonic::transport::Channel>>>;

#[derive(Clone)]
struct AppState {
    barq_client: GrpcClient,
    start_time: Instant,
}

// REST API Types
#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    version: String,
    barq_status: String,
}

#[derive(Deserialize)]
struct CreateCollectionReq {
    name: String,
    dimension: u32,
    metric: String,
}

#[derive(Serialize)]
struct CreateCollectionRes {
    success: bool,
    message: String,
}

#[derive(Deserialize)]
struct InsertDocReq {
    collection: String,
    id: String,
    vector: Vec<f32>,
    payload: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct SearchReq {
    collection: String,
    vector: Vec<f32>,
    top_k: u32,
}

#[derive(Serialize)]
struct SearchResult {
    id: String,
    score: f32,
    payload: serde_json::Value,
}

#[derive(Serialize)]
struct StatsResponse {
    connected: bool,
    grpc_addr: String,
    uptime_secs: u64,
}

// REST Handlers
async fn health_handler(AxumState(state): AxumState<AppState>) -> Json<HealthResponse> {
    let mut client = state.barq_client.lock().await;
    let barq_status = match client.health(barq::HealthRequest {}).await {
        Ok(resp) => format!("OK (v{})", resp.into_inner().version),
        Err(e) => format!("Error: {}", e.message()),
    };
    
    Json(HealthResponse {
        ok: true,
        version: "0.1.0".to_string(),
        barq_status,
    })
}

async fn create_collection_handler(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<CreateCollectionReq>,
) -> Json<CreateCollectionRes> {
    let mut client = state.barq_client.lock().await;
    
    match client.create_collection(barq::CreateCollectionRequest {
        name: req.name.clone(),
        dimension: req.dimension,
        metric: req.metric,
    }).await {
        Ok(_) => Json(CreateCollectionRes {
            success: true,
            message: format!("Collection '{}' created", req.name),
        }),
        Err(e) => Json(CreateCollectionRes {
            success: false,
            message: e.message().to_string(),
        }),
    }
}

async fn insert_document_handler(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<InsertDocReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut client = state.barq_client.lock().await;
    let payload_json = req.payload.map(|p| p.to_string()).unwrap_or_else(|| "{}".into());
    
    match client.insert_document(barq::InsertDocumentRequest {
        collection: req.collection,
        id: req.id,
        vector: req.vector,
        payload_json,
    }).await {
        Ok(_) => Ok(StatusCode::CREATED),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string())),
    }
}

async fn search_handler(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<SearchReq>,
) -> Result<Json<Vec<SearchResult>>, (StatusCode, String)> {
    let mut client = state.barq_client.lock().await;
    
    match client.search(barq::SearchRequest {
        collection: req.collection,
        vector: req.vector,
        top_k: req.top_k,
    }).await {
        Ok(resp) => {
            let results: Vec<SearchResult> = resp.into_inner().results.into_iter().map(|r| {
                SearchResult {
                    id: r.id,
                    score: r.score,
                    payload: serde_json::from_str(&r.payload_json).unwrap_or(serde_json::json!({})),
                }
            }).collect();
            Ok(Json(results))
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string())),
    }
}

async fn stats_handler(AxumState(state): AxumState<AppState>) -> Json<StatsResponse> {
    let grpc_addr = std::env::var("BARQ_GRPC_ADDR").unwrap_or_else(|_| "http://localhost:50051".into());
    let mut client = state.barq_client.lock().await;
    let connected = client.health(barq::HealthRequest {}).await.is_ok();
    
    Json(StatsResponse {
        connected,
        grpc_addr,
        uptime_secs: state.start_time.elapsed().as_secs(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let grpc_addr = std::env::var("BARQ_GRPC_ADDR").unwrap_or_else(|_| "http://localhost:50051".into());
    info!("Connecting to Barq gRPC at {}", grpc_addr);
    
    let barq_client = BarqClient::connect(grpc_addr).await?;
    let state = AppState {
        barq_client: Arc::new(Mutex::new(barq_client)),
        start_time: Instant::now(),
    };

    // CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // REST API Router
    let app = Router::new()
        .route("/api/health", get(health_handler))
        .route("/api/collections", post(create_collection_handler))
        .route("/api/documents", post(insert_document_handler))
        .route("/api/search", post(search_handler))
        .route("/api/stats", get(stats_handler))
        .layer(cors)
        .with_state(state);

    let addr = "0.0.0.0:3001";
    info!("REST API server listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
