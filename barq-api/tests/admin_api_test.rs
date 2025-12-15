use axum::http::{HeaderName, HeaderValue, StatusCode};
use barq_api::{build_router_with_auth, ApiAuth, ApiRole};
use barq_core::TenantId;
use barq_storage::Storage;
use tempfile::TempDir;
use axum_test::TestServer;

#[tokio::test]
async fn test_admin_flow_integration() -> anyhow::Result<()> {
    // 1. Setup
    let temp_dir = TempDir::new()?;
    let storage = Storage::open(temp_dir.path())?;
    
    // Configure Auth with an Admin Key
    let auth = ApiAuth::new().require_keys();
    // ApiRole::Admin is re-exported via barq-api
    auth.insert("secret-admin-key", TenantId::new("admin"), ApiRole::Admin);
    auth.insert("tenant-key", TenantId::new("default"), ApiRole::TenantAdmin);

    let app = build_router_with_auth(storage, auth);
    let server = TestServer::new(app)?;

    // 2. Create a collection (Tenant Admin)
    let create_resp = server.post("/collections")
        .add_header(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("tenant-key")
        )
        .json(&serde_json::json!({
            "name": "test-collection",
            "dimension": 4,
            "metric": "Cosine",
            "index": "Flat"
        }))
        .await;
    assert_eq!(create_resp.status_code(), StatusCode::CREATED);

    // 3. Admin: Check Topology (Admin)
    let topology_resp = server.get("/admin/topology")
        .add_header(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("secret-admin-key")
        )
        .await;
    assert_eq!(topology_resp.status_code(), StatusCode::OK);
    // Should return JSON with placements
    let topology: serde_json::Value = topology_resp.json();
    assert!(topology.is_object()); 

    // 4. Admin: Trigger Compaction (Admin)
    let compact_resp = server.post("/admin/compact")
        .add_header(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("secret-admin-key")
        )
        .json(&serde_json::json!({
            "tenant": "default",
            "collection": "test-collection"
        }))
        .await;
    // Compaction might return OK or error if empty, but we check access is allowed
    assert!(compact_resp.status_code() == StatusCode::OK || compact_resp.status_code() == StatusCode::BAD_REQUEST);

    // 5. Admin: Trigger Index Rebuild (Admin)
    let rebuild_resp = server.post("/admin/index/rebuild")
        .add_header(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("secret-admin-key")
        )
        .json(&serde_json::json!({
            "tenant": "default",
            "collection": "test-collection",
            "index_type": "Flat"
        }))
        .await;
    // Should be OK (Accepted or JSON status)
    assert!(rebuild_resp.status_code().is_success());

    // 6. Admin without permission (Fail)
    let fail_resp = server.get("/admin/topology")
        .add_header(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("tenant-key")
        )
        .await;
    assert_eq!(fail_resp.status_code(), StatusCode::FORBIDDEN);

    Ok(())
}
