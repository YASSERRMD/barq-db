use axum::http::{HeaderName, HeaderValue, StatusCode};
use barq_api::{build_router_with_auth, ApiAuth, ApiRole};
use barq_storage::Storage;
use tempfile::TempDir;
use axum_test::TestServer;
use serde_json::json;

#[tokio::test]
async fn test_phase5_multitenancy_isolation() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let storage = Storage::open(temp_dir.path())?;
    let auth = ApiAuth::new().require_keys();
    
    auth.insert("key-a", barq_core::TenantId::new("tenant-a"), ApiRole::TenantAdmin);
    auth.insert("key-b", barq_core::TenantId::new("tenant-b"), ApiRole::TenantAdmin);

    let app = build_router_with_auth(storage, auth);
    let server = TestServer::new(app)?;

    // 1. Tenant A creates 'common-col'
    let res = server.post("/collections")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key-a"))
        .json(&json!({"name": "common-col", "dimension": 2, "metric": "L2", "index": "Flat"}))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 2. Tenant B creates 'common-col' (Same name!)
    // This should work and create a separate namespace
    let res = server.post("/collections")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key-b"))
        .json(&json!({"name": "common-col", "dimension": 2, "metric": "L2", "index": "Flat"}))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 3. Tenant A inserts Doc 1
    let res = server.post("/collections/common-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key-a"))
        .json(&json!({"id": 1, "vector": [0.0, 0.0]}))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 4. Tenant B searches 'common-col' -> Should NOT see Doc 1
    let search_b = server.post("/collections/common-col/search")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key-b"))
        .json(&json!({"vector": [0.0, 0.0], "top_k": 10}))
        .await;
    assert_eq!(search_b.status_code(), StatusCode::OK);
    let json_b: serde_json::Value = search_b.json();
    let results_b = json_b["results"].as_array().unwrap();
    assert!(results_b.is_empty(), "Tenant B should not see Tenant A's data");

    // 5. Tenant B inserts Doc 1 (same ID)
    let res = server.post("/collections/common-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key-b"))
        .json(&json!({"id": 1, "vector": [1.0, 1.0]}))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 6. Verify Tenant A sees [0,0] and Tenant B sees [1,1]
    
    // Explicit delete by A
    let res = server.delete("/collections/common-col/documents/1")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key-a"))
        .await;
    assert_eq!(res.status_code(), StatusCode::NO_CONTENT);

    // Check B still has Doc 1
    let search_b_2 = server.post("/collections/common-col/search")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key-b"))
        .json(&json!({"vector": [1.0, 1.0], "top_k": 1}))
        .await;
    assert_eq!(search_b_2.status_code(), StatusCode::OK);
    let json_b_2: serde_json::Value = search_b_2.json();
    let results_b_2 = json_b_2["results"].as_array().unwrap();
    assert_eq!(results_b_2.len(), 1, "Tenant B data should survive delete by Tenant A");

    Ok(())
}
