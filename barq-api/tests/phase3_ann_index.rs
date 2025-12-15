use axum::http::{HeaderName, HeaderValue, StatusCode};
use barq_api::{build_router_with_auth, ApiAuth, ApiRole};
use barq_storage::Storage;
use tempfile::TempDir;
use axum_test::TestServer;
use serde_json::json;

#[tokio::test]
async fn test_phase3_ann_indexes() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let storage = Storage::open(temp_dir.path())?;
    let auth = ApiAuth::new().require_keys();
    auth.insert("key1", barq_core::TenantId::new("tenant-a"), ApiRole::TenantAdmin);

    let app = build_router_with_auth(storage, auth);
    let server = TestServer::new(app)?;

    // 1. Test HNSW Index
    let res = server.post("/collections")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "name": "hnsw-col",
            "dimension": 2,
            "metric": "L2",
            "index": {
                "Hnsw": {
                    "m": 16,
                    "ef_construction": 64,
                    "ef_search": 64
                }
            } 
        }))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // Insert generic data
    for i in 1..=5 {
        let res = server.post("/collections/hnsw-col/documents")
            .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
            .json(&json!({
                "id": i,
                "vector": [i as f32, 0.0]
            }))
            .await;
        assert_eq!(res.status_code(), StatusCode::CREATED);
    }

    // Search HNSW
    let res = server.post("/collections/hnsw-col/search")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "vector": [1.0, 0.0],
            "top_k": 1
        }))
        .await;
    assert_eq!(res.status_code(), StatusCode::OK);
    let body: serde_json::Value = res.json();
    let results = body["results"].as_array().expect("results");
    assert_eq!(results[0]["id"], json!({"U64": 1}));


    // 2. Test IVF Index
    let res = server.post("/collections")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({
            "name": "ivf-col",
            "dimension": 2,
            "metric": "Cosine",
            "index": {
                "Ivf": {
                    "nlist": 8,
                    "nprobe": 2
                }
            }
        }))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // Insert data
    let res = server.post("/collections/ivf-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({ "id": 10, "vector": [0.0, 1.0] }))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // Search IVF
    let res = server.post("/collections/ivf-col/search")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
        .json(&json!({ "vector": [0.0, 1.0], "top_k": 1 }))
        .await;
    assert_eq!(res.status_code(), StatusCode::OK);
    let body: serde_json::Value = res.json();
    let results = body["results"].as_array().expect("results");
    assert_eq!(results[0]["id"], json!({"U64": 10}));

    Ok(())
}
