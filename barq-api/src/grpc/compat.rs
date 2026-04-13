use crate::{
    CreateCollectionRequest as RestCreateCollectionRequest,
    DocumentIdInput,
    InsertDocumentRequest as RestInsertDocumentRequest,
    SearchRequest as RestSearchRequest,
    SearchResponse as RestSearchResponse,
};
use barq_core::PayloadValue;
use barq_index::{DistanceMetric, DocumentId, SearchResult as RestSearchResult};
use barq_proto::barq::{
    CreateCollectionRequest as GrpcCreateCollectionRequest, InsertRequest as GrpcInsertRequest,
    SearchRequest as GrpcSearchRequest, SearchResponse as GrpcSearchResponse,
};

/// Maps the existing REST request/response shapes onto the canonical gRPC contract.
pub struct RestGrpcAdapter;

impl RestGrpcAdapter {
    pub fn create_collection(
        payload: RestCreateCollectionRequest,
    ) -> GrpcCreateCollectionRequest {
        GrpcCreateCollectionRequest {
            name: payload.name,
            dimension: payload.dimension as u32,
            metric: metric_name(payload.metric).to_string(),
        }
    }

    pub fn insert(
        collection: String,
        payload: RestInsertDocumentRequest,
    ) -> Result<GrpcInsertRequest, serde_json::Error> {
        Ok(GrpcInsertRequest {
            collection,
            id: document_id_string(payload.id),
            vector: payload.vector,
            payload_json: payload
                .payload
                .map(payload_json)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    pub fn search(
        collection: String,
        payload: RestSearchRequest,
    ) -> Result<GrpcSearchRequest, &'static str> {
        if payload.filter.is_some() {
            return Err("filtered vector search is not supported by the canonical gRPC request");
        }

        Ok(GrpcSearchRequest {
            collection,
            vector: payload.vector,
            top_k: payload.top_k as u32,
        })
    }

    pub fn search_response(payload: GrpcSearchResponse) -> RestSearchResponse {
        RestSearchResponse {
            results: payload
                .results
                .into_iter()
                .map(|result| RestSearchResult {
                    id: parse_document_id(&result.id),
                    score: result.score,
                })
                .collect(),
        }
    }
}

fn metric_name(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::L2 => "L2",
        DistanceMetric::Cosine => "Cosine",
        DistanceMetric::Dot => "Dot",
    }
}

fn document_id_string(id: DocumentIdInput) -> String {
    match id {
        DocumentIdInput::U64(value) => value.to_string(),
        DocumentIdInput::Str(value) => value,
    }
}

fn payload_json(payload: PayloadValue) -> Result<String, serde_json::Error> {
    serde_json::to_string(&payload)
}

fn parse_document_id(id: &str) -> DocumentId {
    id.parse().unwrap_or_else(|_| DocumentId::Str(id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::RestGrpcAdapter;
    use crate::{
        CreateCollectionRequest, DocumentIdInput, InsertDocumentRequest, SearchRequest,
    };
    use barq_core::PayloadValue;
    use barq_index::{DistanceMetric, DocumentId, Filter};
    use barq_proto::barq::{SearchResponse, SearchResult};
    use std::collections::HashMap;

    #[test]
    fn rest_create_collection_maps_to_grpc_request() {
        let request = RestGrpcAdapter::create_collection(CreateCollectionRequest {
            name: "docs".to_string(),
            dimension: 384,
            metric: DistanceMetric::Cosine,
            index: None,
            text_fields: Vec::new(),
            bm25_config: None,
        });

        assert_eq!(request.name, "docs");
        assert_eq!(request.dimension, 384);
        assert_eq!(request.metric, "Cosine");
    }

    #[test]
    fn rest_insert_maps_to_grpc_request() {
        let mut payload = HashMap::new();
        payload.insert("kind".to_string(), PayloadValue::String("compat".to_string()));

        let request = RestGrpcAdapter::insert(
            "docs".to_string(),
            InsertDocumentRequest {
                id: DocumentIdInput::U64(7),
                vector: vec![1.0, 0.0],
                payload: Some(PayloadValue::Object(payload)),
                upsert: false,
            },
        )
        .expect("insert mapping should succeed");

        assert_eq!(request.collection, "docs");
        assert_eq!(request.id, "7");
        assert_eq!(request.vector, vec![1.0, 0.0]);
        assert_eq!(request.payload_json, "{\"kind\":\"compat\"}");
    }

    #[test]
    fn rest_search_maps_to_grpc_request() {
        let request = RestGrpcAdapter::search(
            "docs".to_string(),
            SearchRequest {
                vector: vec![0.5, 0.5],
                top_k: 3,
                filter: None,
            },
        )
        .expect("search mapping should succeed");

        assert_eq!(request.collection, "docs");
        assert_eq!(request.vector, vec![0.5, 0.5]);
        assert_eq!(request.top_k, 3);
    }

    #[test]
    fn filtered_rest_search_is_rejected_by_canonical_grpc_mapping() {
        let error = RestGrpcAdapter::search(
            "docs".to_string(),
            SearchRequest {
                vector: vec![0.5, 0.5],
                top_k: 3,
                filter: Some(Filter::Exists {
                    field: "kind".to_string(),
                }),
            },
        )
        .expect_err("filtered search should not map to canonical grpc request");

        assert_eq!(
            error,
            "filtered vector search is not supported by the canonical gRPC request"
        );
    }

    #[test]
    fn grpc_search_response_maps_back_to_rest_shape() {
        let response = RestGrpcAdapter::search_response(SearchResponse {
            results: vec![
                SearchResult {
                    id: "42".to_string(),
                    score: 1.0,
                    payload_json: "{}".to_string(),
                },
                SearchResult {
                    id: "doc-2".to_string(),
                    score: 0.5,
                    payload_json: "{}".to_string(),
                },
            ],
        });

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].id, DocumentId::U64(42));
        assert_eq!(response.results[1].id, DocumentId::Str("doc-2".to_string()));
    }
}
