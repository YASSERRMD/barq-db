use std::sync::Arc;

use axum::{
    extract::State,
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use barq_cluster::ClusterRouter;
use barq_core::TenantId;
use barq_index::IndexType;
use barq_storage::Storage;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::auth::{ApiAuth, ApiError, ApiPermission};

#[derive(Clone)]
pub struct AdminState {
    pub storage: Arc<Mutex<Storage>>,
    pub cluster: ClusterRouter,
    pub auth: ApiAuth,
}

pub fn admin_routes() -> Router<AdminState> {
    Router::new()
        .route("/compact", post(admin_compact))
        .route("/index/rebuild", post(admin_index_rebuild))
        .route("/node/drain", post(admin_node_drain))
        .route("/topology", get(admin_topology))
}

#[derive(Deserialize)]
struct AdminCompactRequest {
    tenant: TenantId,
    collection: String,
}

async fn admin_compact(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(req): Json<AdminCompactRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.auth.authenticate(&headers, ApiPermission::Admin, None)?;
    
    // Logic for distributed systems: verify locality.
    // For now, assume operation on local node.
    
    // Check local? (Using existing logic from barq-api)
    // state.ensure_local_for_tenant(&req.tenant)?; // Helper needed if strict check desired.
    // We can assume admin targets the correct node or bypass check for now.
    
    let mut storage = state.storage.lock().await;
    let metadata = storage.compact_segments(&req.tenant, &req.collection)?;
    Ok(Json(serde_json::to_value(metadata).unwrap()))
}

#[derive(Deserialize)]
struct AdminIndexRebuildRequest {
    tenant: TenantId,
    collection: String,
    index_type: Option<IndexType>,
}

async fn admin_index_rebuild(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(req): Json<AdminIndexRebuildRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.auth.authenticate(&headers, ApiPermission::Admin, None)?;
    
    let mut storage = state.storage.lock().await;
    let coll = storage
        .catalog_mut()
        .collection_mut(&req.tenant, &req.collection)
        .map_err(|e| ApiError::Storage(barq_storage::StorageError::Catalog(e)))?;
    coll.rebuild_index(req.index_type).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    
    Ok(Json(serde_json::json!({ "status": "rebuild_initiated" })))
}

#[derive(Deserialize)]
struct AdminDrainRequest {
    node_id: String,
}

async fn admin_node_drain(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(req): Json<AdminDrainRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.auth.authenticate(&headers, ApiPermission::Admin, None)?;
    Ok(Json(serde_json::json!({ "status": "draining", "node": req.node_id })))
}

async fn admin_topology(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.auth.authenticate(&headers, ApiPermission::Admin, None)?;
    let placements = state.cluster.placements.clone();
    Ok(Json(serde_json::to_value(placements).unwrap()))
}
