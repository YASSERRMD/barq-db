use axum::http::{HeaderName, HeaderValue, StatusCode};
use barq_api::{build_router_with_auth, ApiAuth, ApiRole, ApiPermission};
use barq_storage::Storage;
use tempfile::TempDir;
use axum_test::TestServer;
use serde_json::json;

#[tokio::test]
async fn test_phase1_persistence_and_crud() -> anyhow::Result<()> {
    // 1. Setup persistent dir
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().to_owned();

    // --- SESSION 1: Insert Data ---
    {
        let storage = Storage::open(&db_path)?;
        let auth = ApiAuth::new().require_keys();
        auth.insert("key1", barq_core::TenantId::new("tenant-a"), ApiRole::TenantAdmin);

        let app = build_router_with_auth(storage, auth);
        let server = TestServer::new(app)?;

        // Create Collection
        let res = server.post("/collections")
            .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
            .json(&json!({
                "name": "data-v1",
                "dimension": 2,
                "metric": "L2",
                "index": "Flat"
            }))
            .await;
        assert_eq!(res.status_code(), StatusCode::CREATED);

        // Insert Doc
        let res = server.post("/collections/data-v1/documents")
            .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
            .json(&json!({
                "id": 1,
                "vector": [1.0, 0.0],
                "payload": {"info": "persistent"}
            }))
            .await;
        assert_eq!(res.status_code(), StatusCode::CREATED);
    } // Storage is dropped here, simulating shutdown

    // --- SESSION 2: Recovery ---
    {
        // Re-open storage from same path
        let storage = Storage::open(&db_path)?;
        let auth = ApiAuth::new().require_keys();
        auth.insert("key1", barq_core::TenantId::new("tenant-a"), ApiRole::TenantAdmin);

        let app = build_router_with_auth(storage, auth);
        let server = TestServer::new(app)?;

        // Search to verify persistence
        let res = server.post("/collections/data-v1/search")
            .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static("key1"))
            .json(&json!({
                "vector": [1.0, 0.0],
                "top_k": 1
            }))
            .await;
        
        assert_eq!(res.status_code(), StatusCode::OK);
        let body: serde_json::Value = res.json();
        let results = body["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1, "Should find 1 doc after restart");
        assert_eq!(results[0]["id"], json!({"U64": 1}));
    }

    Ok(())
}
