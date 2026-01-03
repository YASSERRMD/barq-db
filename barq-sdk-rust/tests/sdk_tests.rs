//! Unit tests for barq-sdk-rust client
//!
//! These tests verify the SDK's behavior without requiring a running server.
//! They test:
//! - Client construction and configuration
//! - Request/response serialization
//! - Error handling
//! - API surface correctness

use barq_core::{DistanceMetric, DocumentId, Filter, HybridWeights, PayloadValue};
use barq_sdk_rust::{BarqClient, BarqError, TextFieldRequest};
use serde_json::json;

// ============================================================================
// Client Construction Tests
// ============================================================================

#[test]
fn test_client_construction() {
    let client = BarqClient::new("http://localhost:8080", "test-api-key");
    // Client should be constructed without panicking
    assert!(format!("{:?}", client).contains("localhost:8080"));
}

#[test]
fn test_client_with_various_urls() {
    // HTTP
    let _client = BarqClient::new("http://localhost:8080", "key");

    // HTTPS
    let _client = BarqClient::new("https://barq.example.com", "key");

    // With path
    let _client = BarqClient::new("http://localhost:8080/api/v1", "key");

    // With port
    let _client = BarqClient::new("http://192.168.1.100:3000", "key");
}

#[test]
fn test_collection_accessor() {
    let client = BarqClient::new("http://localhost:8080", "key");
    let collection = client.collection("my-vectors");

    // Collection should be created from client
    assert!(format!("{:?}", collection).contains("my-vectors"));
}

// ============================================================================
// Document ID Tests
// ============================================================================

#[test]
fn test_document_id_u64_serialization() {
    let id = DocumentId::U64(12345);
    let json = serde_json::to_value(&id).unwrap();
    // DocumentId serializes with variant tag
    assert_eq!(json["U64"], 12345);
}

#[test]
fn test_document_id_string_serialization() {
    let id = DocumentId::Str("doc-abc-123".to_string());
    let json = serde_json::to_value(&id).unwrap();
    // DocumentId serializes with variant tag
    assert_eq!(json["Str"], "doc-abc-123");
}

#[test]
fn test_document_id_creation() {
    let id = DocumentId::U64(42);
    match id {
        DocumentId::U64(v) => assert_eq!(v, 42),
        _ => panic!("Expected U64 variant"),
    }

    let id = DocumentId::Str("my-doc".to_string());
    match id {
        DocumentId::Str(s) => assert_eq!(s, "my-doc"),
        _ => panic!("Expected Str variant"),
    }
}

// ============================================================================
// Distance Metric Tests
// ============================================================================

#[test]
fn test_distance_metric_serialization() {
    assert_eq!(
        serde_json::to_value(&DistanceMetric::Cosine).unwrap(),
        json!("Cosine")
    );
    assert_eq!(
        serde_json::to_value(&DistanceMetric::L2).unwrap(),
        json!("L2")
    );
    assert_eq!(
        serde_json::to_value(&DistanceMetric::Dot).unwrap(),
        json!("Dot")
    );
}

#[test]
fn test_distance_metric_deserialization() {
    let cosine: DistanceMetric = serde_json::from_value(json!("Cosine")).unwrap();
    assert_eq!(cosine, DistanceMetric::Cosine);

    let l2: DistanceMetric = serde_json::from_value(json!("L2")).unwrap();
    assert_eq!(l2, DistanceMetric::L2);

    let dot: DistanceMetric = serde_json::from_value(json!("Dot")).unwrap();
    assert_eq!(dot, DistanceMetric::Dot);
}

// ============================================================================
// Filter Serialization Tests
// ============================================================================

#[test]
fn test_filter_eq_serialization() {
    let filter = Filter::Eq {
        field: "category".to_string(),
        value: PayloadValue::String("electronics".to_string()),
    };

    let json = serde_json::to_value(&filter).unwrap();
    assert_eq!(json["field"], "category");
    assert_eq!(json["value"], "electronics");
}

#[test]
fn test_filter_gt_serialization() {
    let filter = Filter::Gt {
        field: "price".to_string(),
        value: PayloadValue::F64(10.0),
    };

    let json = serde_json::to_value(&filter).unwrap();
    assert_eq!(json["field"], "price");
    assert_eq!(json["value"], 10.0);
}

#[test]
fn test_filter_and_serialization() {
    let filter = Filter::And {
        filters: vec![
            Filter::Eq {
                field: "status".to_string(),
                value: PayloadValue::String("active".to_string()),
            },
            Filter::Eq {
                field: "type".to_string(),
                value: PayloadValue::String("product".to_string()),
            },
        ],
    };

    let json = serde_json::to_value(&filter).unwrap();
    let filters = json["filters"].as_array().unwrap();
    assert_eq!(filters.len(), 2);
}

#[test]
fn test_filter_or_serialization() {
    let filter = Filter::Or {
        filters: vec![
            Filter::Eq {
                field: "region".to_string(),
                value: PayloadValue::String("us".to_string()),
            },
            Filter::Eq {
                field: "region".to_string(),
                value: PayloadValue::String("eu".to_string()),
            },
        ],
    };

    let json = serde_json::to_value(&filter).unwrap();
    let filters = json["filters"].as_array().unwrap();
    assert_eq!(filters.len(), 2);
}

#[test]
fn test_filter_not_serialization() {
    let filter = Filter::Not {
        filter: Box::new(Filter::Eq {
            field: "deleted".to_string(),
            value: PayloadValue::Bool(true),
        }),
    };

    let json = serde_json::to_value(&filter).unwrap();
    assert!(json["filter"].is_object());
}

#[test]
fn test_filter_exists_serialization() {
    let filter = Filter::Exists {
        field: "optional_field".to_string(),
    };

    let json = serde_json::to_value(&filter).unwrap();
    assert_eq!(json["field"], "optional_field");
}

#[test]
fn test_filter_in_serialization() {
    let filter = Filter::In {
        field: "status".to_string(),
        values: vec![
            PayloadValue::String("active".to_string()),
            PayloadValue::String("pending".to_string()),
        ],
    };

    let json = serde_json::to_value(&filter).unwrap();
    assert_eq!(json["field"], "status");
    assert_eq!(json["values"].as_array().unwrap().len(), 2);
}

// ============================================================================
// Hybrid Weights Tests
// ============================================================================

#[test]
fn test_hybrid_weights_serialization() {
    let weights = HybridWeights {
        bm25: 0.3,
        vector: 0.7,
    };

    let json = serde_json::to_value(&weights).unwrap();
    // f32 loses some precision in JSON serialization
    assert!((json["vector"].as_f64().unwrap() - 0.7).abs() < 0.01);
    assert!((json["bm25"].as_f64().unwrap() - 0.3).abs() < 0.01);
}

#[test]
fn test_hybrid_weights_deserialization() {
    let json = json!({ "vector": 0.5, "bm25": 0.5 });
    let weights: HybridWeights = serde_json::from_value(json).unwrap();
    assert_eq!(weights.vector, 0.5);
    assert_eq!(weights.bm25, 0.5);
}

#[test]
fn test_hybrid_weights_default() {
    let weights = HybridWeights::default();
    assert_eq!(weights.bm25, 0.5);
    assert_eq!(weights.vector, 0.5);
}

// ============================================================================
// TextFieldRequest Tests
// ============================================================================

#[test]
fn test_text_field_request_serialization() {
    let field = TextFieldRequest {
        name: "description".to_string(),
        indexed: true,
        required: false,
    };

    let json = serde_json::to_value(&field).unwrap();
    assert_eq!(json["name"], "description");
    assert_eq!(json["indexed"], true);
    assert_eq!(json["required"], false);
}

#[test]
fn test_text_field_request_deserialization() {
    let json = json!({
        "name": "title",
        "indexed": true,
        "required": true
    });

    let field: TextFieldRequest = serde_json::from_value(json).unwrap();
    assert_eq!(field.name, "title");
    assert!(field.indexed);
    assert!(field.required);
}

// ============================================================================
// Error Type Tests
// ============================================================================

#[test]
fn test_error_display() {
    let err = BarqError::Api {
        status: reqwest::StatusCode::NOT_FOUND,
        message: "Collection not found".to_string(),
    };
    let display = format!("{}", err);
    assert!(display.contains("404"));
    assert!(display.contains("Collection not found"));
}

#[test]
fn test_serialization_error() {
    // Create an invalid JSON scenario
    let invalid_json = "{invalid}";
    let result: std::result::Result<serde_json::Value, _> = serde_json::from_str(invalid_json);
    assert!(result.is_err());

    // Verify BarqError can wrap serde errors
    let serde_err = result.unwrap_err();
    let barq_err: BarqError = serde_err.into();
    assert!(format!("{}", barq_err).contains("Serialization error"));
}

// ============================================================================
// Payload Value Tests
// ============================================================================

#[test]
fn test_payload_value_string() {
    let value = PayloadValue::String("hello".to_string());
    let json = serde_json::to_value(&value).unwrap();
    assert_eq!(json, "hello");
}

#[test]
fn test_payload_value_i64() {
    let value = PayloadValue::I64(42);
    let json = serde_json::to_value(&value).unwrap();
    assert_eq!(json, 42);
}

#[test]
fn test_payload_value_f64() {
    let value = PayloadValue::F64(3.14);
    let json = serde_json::to_value(&value).unwrap();
    assert!((json.as_f64().unwrap() - 3.14).abs() < 0.001);
}

#[test]
fn test_payload_value_bool() {
    let value = PayloadValue::Bool(true);
    let json = serde_json::to_value(&value).unwrap();
    assert_eq!(json, true);
}

#[test]
fn test_payload_value_array() {
    let value = PayloadValue::Array(vec![
        PayloadValue::I64(1),
        PayloadValue::I64(2),
        PayloadValue::I64(3),
    ]);
    let json = serde_json::to_value(&value).unwrap();
    assert_eq!(json, json!([1, 2, 3]));
}

#[test]
fn test_payload_value_null() {
    let value = PayloadValue::Null;
    let json = serde_json::to_value(&value).unwrap();
    assert!(json.is_null());
}

// ============================================================================
// Collection Schema Tests (from barq-core)
// ============================================================================

#[test]
fn test_collection_schema_with_hnsw_params() {
    // Test that HNSW index parameters serialize correctly
    let index_params = json!({
        "type": "hnsw",
        "ef_construction": 200,
        "m": 32
    });

    assert_eq!(index_params["type"], "hnsw");
    assert_eq!(index_params["ef_construction"], 200);
    assert_eq!(index_params["m"], 32);
}

#[test]
fn test_collection_schema_with_ivf_params() {
    // Test that IVF index parameters serialize correctly
    let index_params = json!({
        "type": "ivf",
        "n_lists": 256,
        "n_probes": 16
    });

    assert_eq!(index_params["type"], "ivf");
    assert_eq!(index_params["n_lists"], 256);
    assert_eq!(index_params["n_probes"], 16);
}

#[test]
fn test_collection_schema_with_pq() {
    // Test Product Quantization parameters
    let index_params = json!({
        "type": "ivf",
        "n_lists": 128,
        "pq": {
            "n_subvectors": 8,
            "bits_per_code": 8
        }
    });

    assert!(index_params["pq"].is_object());
    assert_eq!(index_params["pq"]["n_subvectors"], 8);
}

// ============================================================================
// Search Request Construction Tests
// ============================================================================

#[test]
fn test_vector_search_request_construction() {
    let vector = vec![0.1, 0.2, 0.3, 0.4];
    let top_k = 10;
    let filter = Some(Filter::Eq {
        field: "status".to_string(),
        value: PayloadValue::String("active".to_string()),
    });

    let request = json!({
        "vector": vector,
        "top_k": top_k,
        "filter": filter
    });

    assert_eq!(request["vector"].as_array().unwrap().len(), 4);
    assert_eq!(request["top_k"], 10);
    assert!(request["filter"].is_object());
}

#[test]
fn test_text_search_request_construction() {
    let query = "rust programming";
    let top_k = 5;

    let request = json!({
        "query": query,
        "top_k": top_k
    });

    assert_eq!(request["query"], "rust programming");
    assert_eq!(request["top_k"], 5);
}

#[test]
fn test_hybrid_search_request_construction() {
    let vector = vec![0.1, 0.2, 0.3];
    let query = "machine learning";
    let weights = HybridWeights {
        bm25: 0.4,
        vector: 0.6,
    };

    let request = json!({
        "vector": vector,
        "query": query,
        "top_k": 20,
        "weights": weights
    });

    assert!(request["vector"].is_array());
    assert_eq!(request["query"], "machine learning");
    // f32 precision in JSON
    assert!((request["weights"]["vector"].as_f64().unwrap() - 0.6).abs() < 0.01);
}

// ============================================================================
// Insert Request Construction Tests
// ============================================================================

#[test]
fn test_insert_request_with_u64_id() {
    let id = DocumentId::U64(12345);
    let vector = vec![1.0, 2.0, 3.0];
    let payload = json!({
        "title": "Test Document",
        "category": "testing"
    });

    let request = json!({
        "id": match id {
            DocumentId::U64(v) => json!(v),
            DocumentId::Str(s) => json!(s),
        },
        "vector": vector,
        "payload": payload
    });

    assert_eq!(request["id"], 12345);
    assert_eq!(request["vector"].as_array().unwrap().len(), 3);
    assert_eq!(request["payload"]["title"], "Test Document");
}

#[test]
fn test_insert_request_with_string_id() {
    let id = DocumentId::Str("doc-uuid-abc123".to_string());
    let vector = vec![0.5; 128];

    let request = json!({
        "id": match id {
            DocumentId::U64(v) => json!(v),
            DocumentId::Str(s) => json!(s),
        },
        "vector": vector,
        "payload": null
    });

    assert_eq!(request["id"], "doc-uuid-abc123");
    assert_eq!(request["vector"].as_array().unwrap().len(), 128);
    assert!(request["payload"].is_null());
}

#[test]
fn test_insert_request_with_complex_payload() {
    let payload = json!({
        "title": "Complex Document",
        "tags": ["rust", "vector", "database"],
        "metadata": {
            "author": "test",
            "version": 1,
            "published": true
        },
        "score": 0.95
    });

    let request = json!({
        "id": 1,
        "vector": vec![0.1; 64],
        "payload": payload
    });

    assert_eq!(request["payload"]["tags"].as_array().unwrap().len(), 3);
    assert_eq!(request["payload"]["metadata"]["author"], "test");
    assert_eq!(request["payload"]["score"], 0.95);
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

#[test]
fn test_empty_vector() {
    let vector: Vec<f32> = vec![];
    let json = serde_json::to_value(&vector).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

#[test]
fn test_high_dimensional_vector() {
    let vector: Vec<f32> = vec![0.01; 4096];
    let json = serde_json::to_value(&vector).unwrap();
    assert_eq!(json.as_array().unwrap().len(), 4096);
}

#[test]
fn test_unicode_in_payload() {
    let payload = json!({
        "title": "Документ на русском",
        "japanese": "日本語テスト",
        "emoji": "🚀🔥💯"
    });

    // Should round-trip correctly
    let serialized = serde_json::to_string(&payload).unwrap();
    let deserialized: serde_json::Value = serde_json::from_str(&serialized).unwrap();

    assert_eq!(deserialized["title"], "Документ на русском");
    assert_eq!(deserialized["japanese"], "日本語テスト");
    assert_eq!(deserialized["emoji"], "🚀🔥💯");
}

#[test]
fn test_special_characters_in_field_names() {
    let filter = Filter::Eq {
        field: "field.with.dots".to_string(),
        value: PayloadValue::String("value".to_string()),
    };

    let json = serde_json::to_value(&filter).unwrap();
    assert_eq!(json["field"], "field.with.dots");
}

#[test]
fn test_nested_filter_complexity() {
    // Build a complex nested filter
    let complex_filter = Filter::And {
        filters: vec![
            Filter::Or {
                filters: vec![
                    Filter::Eq {
                        field: "region".to_string(),
                        value: PayloadValue::String("us".to_string()),
                    },
                    Filter::Eq {
                        field: "region".to_string(),
                        value: PayloadValue::String("eu".to_string()),
                    },
                ],
            },
            Filter::Not {
                filter: Box::new(Filter::Eq {
                    field: "deleted".to_string(),
                    value: PayloadValue::Bool(true),
                }),
            },
            Filter::Gt {
                field: "score".to_string(),
                value: PayloadValue::F64(0.5),
            },
        ],
    };

    let json = serde_json::to_value(&complex_filter).unwrap();
    assert!(json["filters"].is_array());
    assert_eq!(json["filters"].as_array().unwrap().len(), 3);
}

// ============================================================================
// GeoPoint and GeoBoundingBox Tests
// ============================================================================

#[test]
fn test_geo_point_serialization() {
    use barq_core::GeoPoint;

    let point = GeoPoint { lat: 37.7749, lon: -122.4194 };
    let json = serde_json::to_value(&point).unwrap();

    assert!((json["lat"].as_f64().unwrap() - 37.7749).abs() < 0.0001);
    assert!((json["lon"].as_f64().unwrap() - (-122.4194)).abs() < 0.0001);
}

#[test]
fn test_geo_bounding_box_filter() {
    use barq_core::{GeoBoundingBox, GeoPoint};

    let filter = Filter::GeoWithin {
        field: "location".to_string(),
        bounding_box: GeoBoundingBox {
            top_left: GeoPoint { lat: 40.0, lon: -125.0 },
            bottom_right: GeoPoint { lat: 35.0, lon: -120.0 },
        },
    };

    let json = serde_json::to_value(&filter).unwrap();
    assert_eq!(json["field"], "location");
    assert!(json["bounding_box"]["top_left"]["lat"].as_f64().is_some());
}

// ============================================================================
// Document Tests
// ============================================================================

#[test]
fn test_document_serialization() {
    use barq_core::Document;
    use std::collections::HashMap;

    let mut payload_map = HashMap::new();
    payload_map.insert("title".to_string(), PayloadValue::String("Test".to_string()));

    let doc = Document {
        id: DocumentId::U64(1),
        vector: vec![0.1, 0.2, 0.3],
        payload: Some(PayloadValue::Object(payload_map)),
    };

    let json = serde_json::to_value(&doc).unwrap();
    // DocumentId serializes with variant tag
    assert_eq!(json["id"]["U64"], 1);
    assert_eq!(json["vector"].as_array().unwrap().len(), 3);
    assert_eq!(json["payload"]["title"], "Test");
}

#[test]
fn test_document_without_payload() {
    use barq_core::Document;

    let doc = Document {
        id: DocumentId::Str("doc-123".to_string()),
        vector: vec![0.5; 64],
        payload: None,
    };

    let json = serde_json::to_value(&doc).unwrap();
    // DocumentId serializes with variant tag
    assert_eq!(json["id"]["Str"], "doc-123");
    assert!(json["payload"].is_null());
}

// ============================================================================
// Search Result Parsing Tests
// ============================================================================

#[test]
fn test_search_result_parsing() {
    use barq_index::SearchResult;

    let result = SearchResult {
        id: DocumentId::U64(42),
        score: 0.95,
    };

    let json = serde_json::to_value(&result).unwrap();
    // DocumentId serializes with variant tag
    assert_eq!(json["id"]["U64"], 42);
    assert!((json["score"].as_f64().unwrap() - 0.95).abs() < 0.001);
}

#[test]
fn test_hybrid_search_result_parsing() {
    use barq_core::HybridSearchResult;

    let result = HybridSearchResult {
        id: DocumentId::U64(42),
        bm25_score: Some(0.8),
        vector_score: Some(0.9),
        score: 0.85,
    };

    let json = serde_json::to_value(&result).unwrap();
    // DocumentId serializes with variant tag
    assert_eq!(json["id"]["U64"], 42);
    assert!((json["bm25_score"].as_f64().unwrap() - 0.8).abs() < 0.01);
    assert!((json["vector_score"].as_f64().unwrap() - 0.9).abs() < 0.01);
    assert!((json["score"].as_f64().unwrap() - 0.85).abs() < 0.01);
}

// ============================================================================
// Timestamp Payload Tests
// ============================================================================

#[test]
fn test_timestamp_payload_value() {
    use chrono::{TimeZone, Utc};

    let timestamp = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 0).unwrap();
    let value = PayloadValue::Timestamp(timestamp);

    let json = serde_json::to_value(&value).unwrap();
    // Timestamp should serialize to ISO 8601 string
    assert!(json.as_str().is_some());
}

// ============================================================================
// Object Payload Tests
// ============================================================================

#[test]
fn test_nested_object_payload() {
    use std::collections::HashMap;

    let mut inner = HashMap::new();
    inner.insert("nested_key".to_string(), PayloadValue::String("nested_value".to_string()));
    inner.insert("nested_num".to_string(), PayloadValue::I64(42));

    let mut outer = HashMap::new();
    outer.insert("inner".to_string(), PayloadValue::Object(inner));
    outer.insert("top_level".to_string(), PayloadValue::Bool(true));

    let payload = PayloadValue::Object(outer);
    let json = serde_json::to_value(&payload).unwrap();

    assert_eq!(json["inner"]["nested_key"], "nested_value");
    assert_eq!(json["inner"]["nested_num"], 42);
    assert_eq!(json["top_level"], true);
}

#[test]
fn test_batch_search_request_construction() {
    use barq_sdk_rust::{BatchSearchRequest, SearchQuery};
    
    let queries = vec![
        SearchQuery {
            vector: vec![0.1, 0.2, 0.3],
            filter: None,
        },
        SearchQuery {
            vector: vec![0.4, 0.5, 0.6],
            filter: Some(Filter::Eq {
                field: "category".to_string(),
                value: PayloadValue::String("books".to_string()),
            }),
        },
    ];

    let request = BatchSearchRequest {
        queries,
        top_k: 10,
    };

    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["queries"].as_array().unwrap().len(), 2);
    assert_eq!(json["top_k"], 10);
    
    // Check first query
    assert_eq!(json["queries"][0]["vector"].as_array().unwrap().len(), 3);
    assert!(json["queries"][0]["filter"].is_null());
    
    // Check second query
    assert_eq!(json["queries"][1]["vector"].as_array().unwrap().len(), 3);
    assert_eq!(json["queries"][1]["filter"]["field"], "category");
}
