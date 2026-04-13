pub mod compat;

use crate::{ApiError, AppState};
use barq_core::{
    CatalogError, CollectionSchema, DistanceMetric, Document, DocumentId, FieldSchema, FieldType,
    Filter, PayloadValue, TenantId,
};
use barq_proto::barq::barq_server::Barq;
use barq_proto::barq::{
    BatchSearchRequest, BatchSearchResponse, CreateCollectionRequest, CreateCollectionResponse,
    HealthRequest, HealthResponse, InsertDocumentRequest, InsertDocumentResponse, InsertRequest,
    InsertResponse, QueryResults, SearchRequest, SearchResponse, SearchResult, StatusRequest,
    StatusResponse,
};
use barq_storage::StorageError;
use tonic::{Request, Response, Status};

pub struct GrpcService {
    pub(crate) state: AppState,
}

impl GrpcService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    fn default_tenant() -> TenantId {
        TenantId::default()
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

    async fn insert_internal(
        &self,
        collection: String,
        id: String,
        vector: Vec<f32>,
        payload_json: String,
    ) -> Result<InsertResponse, Status> {
        Self::require_non_empty(&collection, "collection")?;
        Self::require_non_empty(&id, "id")?;
        if vector.is_empty() {
            return Err(Status::invalid_argument("vector must not be empty"));
        }

        let tenant = Self::default_tenant();
        let document = Document {
            id: Self::parse_document_id(&id),
            vector,
            payload: Self::parse_insert_payload(&payload_json)?,
        };

        self.state
            .ensure_primary_for_document(&tenant, &document.id)
            .map_err(api_error_to_status)?;
        self.state
            .enqueue_insert_for_tenant(&tenant, &collection, document, false)
            .await
            .map_err(api_error_to_status)?;

        Ok(InsertResponse { success: true })
    }

    async fn search_internal(&self, request: SearchRequest) -> Result<SearchResponse, Status> {
        Self::require_non_empty(&request.collection, "collection")?;
        if request.vector.is_empty() {
            return Err(Status::invalid_argument("vector must not be empty"));
        }
        if request.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be positive"));
        }

        let tenant = Self::default_tenant();
        self.state
            .ensure_local_for_tenant(&tenant)
            .map_err(api_error_to_status)?;

        let mut storage = self.state.storage.lock().await;
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
        let response = self
            .insert_internal(
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
            tenant_id: barq_core::TenantId::new("default"),
        };

        let tenant = schema.tenant_id.clone();

        let mut storage = self.state.storage.lock().await;

        match storage.create_collection_for_tenant(tenant, schema) {
            Ok(_) => Ok(Response::new(CreateCollectionResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn insert_document(
        &self,
        request: Request<InsertDocumentRequest>,
    ) -> Result<Response<InsertDocumentResponse>, Status> {
        let req = request.into_inner();
        let response = self
            .insert_internal(req.collection, req.id, req.vector, req.payload_json)
            .await?;
        Ok(Response::new(InsertDocumentResponse {
            success: response.success,
        }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let response = self.search_internal(request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn batch_search(
        &self,
        request: Request<BatchSearchRequest>,
    ) -> Result<Response<BatchSearchResponse>, Status> {
        let req = request.into_inner();
        let collection_name = req.collection;
        let tenant = barq_core::TenantId::from("default");

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
    use crate::{ApiAuth, ClusterConfig, ClusterRouter};
    use tonic::Code;

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
                }))
                .await
                .unwrap();
        }

        let response = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 1,
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
            }))
            .await
            .unwrap_err();
        assert_eq!(invalid_insert.code(), Code::InvalidArgument);

        let invalid_search = service
            .search(Request::new(SearchRequest {
                collection: "docs".to_string(),
                vector: vec![1.0, 0.0],
                top_k: 0,
            }))
            .await
            .unwrap_err();
        assert_eq!(invalid_search.code(), Code::InvalidArgument);
    }
}
