use axum::http::{HeaderName, HeaderValue, StatusCode};
use barq_api::{build_router_with_auth, ApiAuth, ApiRole};
use barq_storage::Storage;
use axum_test::TestServer;
use serde_json::json;

#[tokio::test]
async fn test_chaos_restarts_and_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().to_str().unwrap().to_string();
    let api_key = "chaos-key";

    let total_iterations = 5;
    let docs_per_batch = 100;

    for i in 0..total_iterations {
        println!("Chaos Iteration {}: Starting Server...", i);
        
        let storage = Storage::open(&db_path).unwrap();
        let auth = ApiAuth::new();
        // auth.insert() requires mut? If so, I need to keep mut. 
        // Checking previous error: "help: remove this mut". So insert must be interior mutability or consuming?
        // Let's check `barq-admin/src/auth.rs`.
        // If ApiAuth uses Arc<RwLock>, insert might be &self.
        // Assuming the compiler knows best, I will remove mut.
        auth.insert(api_key, barq_core::TenantId::new("chaos-tenant"), ApiRole::TenantAdmin);

        let app = build_router_with_auth(storage, auth);
        let server = TestServer::new(app).unwrap();

        if i == 0 {
            let res = server.post("/collections")
                .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static(api_key))
                .json(&json!({
                    "name": "chaos_col",
                    "dimension": 4,
                    "metric": "L2"
                }))
                .await;
            
            // It might fail if directory reuse causes issues?
            // "Storage::open" should load existing state.
            // If collection exists, this might return 409 Conflict or 200 OK depending on implementation.
            // Barq API `create_collection` usually fails if exists.
            // So on i > 0, we expect failure or skip. 
            // In my loop, I only run this if i == 0. So it's fine.
            assert_eq!(res.status_code(), StatusCode::CREATED);
        }

        let start_id = i * docs_per_batch + 1;
        for j in 0..docs_per_batch {
            let id = start_id + j;
            let vec = vec![(id as f32) * 0.1; 4];
            let res = server.post("/collections/chaos_col/documents")
                .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static(api_key))
                .json(&json!({
                    "id": id,
                    "vector": vec,
                    "payload": {"batch": i}
                }))
                .await;
            if res.status_code() != StatusCode::CREATED {
                println!("Insert Failed: {} - {}", res.status_code(), res.text());
                panic!("Insert failed");
            }
        }

        drop(server);
        println!("Chaos Iteration {}: Server stopped.", i);
    }

    println!("Final Verification: Restarting Server...");
    let storage = Storage::open(&db_path).unwrap();
    let auth = ApiAuth::new();
    auth.insert(api_key, barq_core::TenantId::new("chaos-tenant"), ApiRole::TenantAdmin);
    let app = build_router_with_auth(storage, auth);
    let server = TestServer::new(app).unwrap();

    let res = server.get("/collections/chaos_col/documents/10")
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static(api_key))
        .await;
    assert_eq!(res.status_code(), StatusCode::OK);

    let last_id = (total_iterations - 1) * docs_per_batch + 10;
    let res = server.get(&format!("/collections/chaos_col/documents/{}", last_id))
        .add_header(HeaderName::from_static("x-api-key"), HeaderValue::from_static(api_key))
        .await;
    assert_eq!(res.status_code(), StatusCode::OK);
    let json: serde_json::Value = res.json();
    // Response is { "document": { ... } } or { "document": null }?
    // If not found, it returns { "document": null }?
    // Wait, get_document returns document: Option<Document>. 
    // And if Option is None, it serializes to null.
    // So "document": null.
    // But we expect it to exist.
    assert!(json["document"].is_object(), "Expected document object, got {:?}", json);
    assert_eq!(json["document"]["id"]["U64"], last_id);

    println!("Chaos Test Passed.");
}
