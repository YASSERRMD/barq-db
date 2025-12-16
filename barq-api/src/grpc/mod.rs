use std::sync::Arc;
use tonic::{Request, Response, Status};
use tokio::sync::Mutex;
use crate::AppState;
use barq_proto::barq::barq_server::Barq;
use barq_proto::barq::{
    CreateCollectionRequest, CreateCollectionResponse, 
    HealthRequest, HealthResponse, 
    InsertDocumentRequest, InsertDocumentResponse, 
    SearchRequest, SearchResponse, SearchResult
};
use barq_core::{CollectionSchema, DistanceMetric, Document, DocumentId, FieldSchema, FieldType, PayloadValue};
use barq_storage::Storage;

pub struct GrpcService {
    pub(crate) state: AppState,
}

impl GrpcService {
    pub fn new(state: AppState) -> Self {
        Self { state }
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
        },
        serde_json::Value::String(s) => PayloadValue::String(s),
        serde_json::Value::Array(arr) => PayloadValue::Array(arr.into_iter().map(json_to_payload).collect()),
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
    async fn health(&self, _request: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ok: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
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
            fields: vec![
                FieldSchema {
                    name: "vector".to_string(),
                    field_type: FieldType::Vector {
                        dimension: req.dimension as usize,
                        metric,
                        index: None,
                    },
                    required: true,
                }
            ],
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
        
        let payload_json: serde_json::Value = serde_json::from_str(&req.payload_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON payload: {}", e)))?;
        
        let doc_id = if let Ok(u) = req.id.parse::<u64>() {
            DocumentId::U64(u)
        } else {
            DocumentId::Str(req.id.clone())
        };

        let collection_name = req.collection;
        let tenant = barq_core::TenantId::from("default");
        
        let payload = json_to_payload(payload_json);

        let doc = Document {
            id: doc_id,
            vector: req.vector,
            payload: Some(payload),
        };

        self.state.ensure_primary_for_document(&tenant, &doc.id).map_err(|e| Status::failed_precondition(e.to_string()))?;

        let mut storage = self.state.storage.lock().await;
        match storage.insert_for_tenant(&tenant, &collection_name, doc, false) {
            Ok(_) => Ok(Response::new(InsertDocumentResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let collection_name = req.collection;
        let tenant = barq_core::TenantId::from("default");
        
        let storage = self.state.storage.lock().await; 
        
        let collection = storage.catalog().collection(&tenant, &collection_name)
            .map_err(|e| Status::not_found(e.to_string()))?;
        
        let results = collection.search(&req.vector, req.top_k as usize)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut proto_results = Vec::new();
        for res in results {
            let id_str = match res.id {
                DocumentId::U64(v) => v.to_string(),
                DocumentId::Str(s) => s,
            };
            
            proto_results.push(SearchResult {
                id: id_str,
                score: res.score,
                payload_json: "{}".to_string(), 
            });
        }

        Ok(Response::new(SearchResponse { results: proto_results }))
    }
}
