use axum::http::{HeaderName, HeaderValue, StatusCode};
use barq_api::{build_router_with_auth, ApiAuth, ApiRole};
use barq_storage::Storage;
use tempfile::TempDir;
use axum_test::TestServer;
use serde_json::json;

#[tokio::test]
async fn test_phase2_hybrid_search() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let storage = Storage::open(temp_dir.path())?;
    let auth = ApiAuth::new().require_keys();
    auth.insert("key1", barq_core::TenantId::new("tenant-a"), ApiRole::TenantAdmin);

    let app = build_router_with_auth(storage, auth);
    let server = TestServer::new(app)?;

    // 1. Create Collection with Text Fields
    let res = server.post("/collections")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "name": "hybrid-col",
            "dimension": 2,
            "metric": "Cosine",
            "index": "Flat",
            "text_fields": [
                {"name": "description", "indexed": true, "required": true}
            ]
        }))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 2. Insert Documents
    // Doc 1: "apple" (vec [1,0])
    let res = server.post("/collections/hybrid-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "id": 1,
            "vector": [1.0, 0.0],
            "payload": {"description": "fresh red apple", "category": "fruit"}
        }))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // Doc 2: "banana" (vec [0,1])
    let res = server.post("/collections/hybrid-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "id": 2,
            "vector": [0.0, 1.0],
            "payload": {"description": "yellow sweet banana", "category": "fruit"}
        }))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 3. Text Search (BM25) for "apple"
    let text_res = server.post("/collections/hybrid-col/search/text")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "query": "apple",
            "top_k": 5
        }))
        .await;
    assert_eq!(text_res.status_code(), StatusCode::OK);
    let text_json: serde_json::Value = text_res.json();
    let text_results = text_json["results"].as_array().unwrap();
    assert_eq!(text_results.len(), 1);
    assert_eq!(text_results[0]["id"], json!({"U64": 1}));

    // 4. Hybrid Search (Vector + Text)
    // Query: "banana" text, but vector closer to Doc 1? No, let's align them.
    // Vector [0, 1] matches Doc 2. Text "banana" matches Doc 2.
    let hybrid_res = server.post("/collections/hybrid-col/search/hybrid")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "query": "banana",
            "vector": [0.0, 1.0], 
            "top_k": 5,
            "weights": {"vector": 0.5, "bm25": 0.5}
        }))
        .await;
    assert_eq!(hybrid_res.status_code(), StatusCode::OK);
    let hybrid_json: serde_json::Value = hybrid_res.json();
    let hybrid_results = hybrid_json["results"].as_array().unwrap();
    assert!(hybrid_results.len() >= 1);
    assert_eq!(hybrid_results[0]["id"], json!({"U64": 2})); // Should match Doc 2 best

    // 5. Filtering (Phase 4 scope)
    // Add Doc 3: "green apple" but category "vegetable" (incorrectly) to test filter
    let res = server.post("/collections/hybrid-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "id": 3,
            "vector": [1.0, 0.0],
            "payload": {"description": "green apple", "category": "vegetable"}
        }))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);
    
    // Search "apple" with filter category="fruit". Should exclude Doc 3.
    let filter_res = server.post("/collections/hybrid-col/search/text")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "query": "apple",
            "top_k": 5,
            "filter": {
                "field": "description",
                "op": "eq",
                "value": "fresh red apple"
            }
        }))
        .await;
    assert_eq!(filter_res.status_code(), StatusCode::OK);
    let filter_json: serde_json::Value = filter_res.json();
    let filter_results = filter_json["results"].as_array().unwrap();
    assert_eq!(filter_results.len(), 1);
    assert_eq!(filter_results[0]["id"], json!({"U64": 1})); // Doc 1 only. Doc 3 filtered out.

    Ok(())
}
