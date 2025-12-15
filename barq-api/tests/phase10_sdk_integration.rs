use barq_api::{build_router_with_auth, ApiAuth, ApiRole};
use barq_storage::Storage;
use tempfile::TempDir;
use tokio::net::TcpListener;
use std::net::SocketAddr;
use barq_sdk_rust::BarqClient;
use barq_core::{DistanceMetric, TenantId};

#[tokio::test]
async fn test_phase10_sdk_integration() -> anyhow::Result<()> {
    // 1. Setup Server
    let temp_dir = TempDir::new()?;
    let storage = Storage::open(temp_dir.path())?;
    let auth = ApiAuth::new().require_keys();
    auth.insert("sdk-key", TenantId::new("tenant-sdk"), ApiRole::TenantAdmin);

    let app = build_router_with_auth(storage, auth);

    // 2. Bind to random port
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("Server running on {}", addr);

    // 3. Spawn server in background
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // 4. Initialize SDK Client
    let base_url = format!("http://{}", addr);
    let client = BarqClient::new(base_url, "sdk-key");

    // 5. Run SDK operations
    // Health check
    client.health().await?;

    // Create Collection
    let col_name = "sdk-test-col";
    client.create_collection(
        col_name, 
        2, 
        DistanceMetric::L2, 
        None, 
        None
    ).await?;

    let collection = client.collection(col_name);

    // Insert Document
    collection.insert(
        barq_core::DocumentId::U64(100), 
        vec![1.0, 1.0], 
        Some(serde_json::json!({"source": "sdk"}))
    ).await?;

    // Search
    let results = collection.search(
        Some(vec![1.0, 1.0]), 
        None, 
        5, 
        None, 
        None
    ).await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["id"], serde_json::json!({"U64": 100}));
    // Note: Search API currently returns only ID and Score, not payload.
    // assert_eq!(results[0]["payload"]["source"], "sdk");

    Ok(())
}
