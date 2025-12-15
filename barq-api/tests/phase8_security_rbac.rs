use axum::http::{HeaderName, HeaderValue, StatusCode};
use barq_api::{build_router_with_auth, ApiAuth, ApiRole};
use barq_storage::Storage;
use tempfile::TempDir;
use axum_test::TestServer;
use serde_json::json;

#[tokio::test]
async fn test_phase8_security_rbac() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let storage = Storage::open(temp_dir.path())?;
    let auth = ApiAuth::new().require_keys();
    
    // Setup roles
    auth.insert("admin-key", barq_core::TenantId::new("tenant-a"), ApiRole::TenantAdmin);
    auth.insert("writer-key", barq_core::TenantId::new("tenant-a"), ApiRole::Writer);
    auth.insert("reader-key", barq_core::TenantId::new("tenant-a"), ApiRole::Reader);

    let app = build_router_with_auth(storage, auth);
    let server = TestServer::new(app)?;

    // 1. TenantAdmin creates collection (Allowed)
    let res = server.post("/collections")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("admin-key"))
        .json(&json!({"name": "secure-col", "dimension": 2, "metric": "L2", "index": "Flat"}))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 2. Writer tries to create collection (Forbidden)
    let res = server.post("/collections")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("writer-key"))
        .json(&json!({"name": "hacker-col", "dimension": 2, "metric": "L2", "index": "Flat"}))
        .await;
    assert_eq!(res.status_code(), StatusCode::FORBIDDEN);

    // 3. Writer inserts document (Allowed)
    let res = server.post("/collections/secure-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("writer-key"))
        .json(&json!({"id": 1, "vector": [1.0, 0.0]}))
        .await;
    assert_eq!(res.status_code(), StatusCode::CREATED);

    // 4. Reader tries to insert document (Forbidden)
    let res = server.post("/collections/secure-col/documents")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("reader-key"))
        .json(&json!({"id": 2, "vector": [0.0, 1.0]}))
        .await;
    assert_eq!(res.status_code(), StatusCode::FORBIDDEN);

    // 5. Reader searches (Allowed)
    let res = server.post("/collections/secure-col/search")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("reader-key"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .await;
    assert_eq!(res.status_code(), StatusCode::OK);
    let body: serde_json::Value = res.json();
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);

    Ok(())
}
