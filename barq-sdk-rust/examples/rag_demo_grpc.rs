use barq_sdk_rust::{BarqGrpcClient, DistanceMetric, DocumentId};
use serde_json::json;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connect to Barq using gRPC (port 50051)
    let mut client = BarqGrpcClient::connect("http://localhost:50051".to_string()).await?;
    println!("Connected to Barq gRPC at localhost:50051");

    // 2. Health check
    let health = client.health().await?;
    println!("Health check: {}", health);

    // 3. Create Collection
    println!("Creating collection 'grpc_rag_demo'...");
    client.create_collection("grpc_rag_demo", 4, DistanceMetric::Cosine).await?;

    // 4. Insert Document
    println!("Inserting documents...");
    let doc_id = DocumentId::Str("doc_grpc_1".to_string());
    let vector = vec![0.1, 0.2, 0.3, 0.4];
    let payload = json!({"text": "Hello form gRPC", "category": "demo"});
    
    client.insert_document("grpc_rag_demo", doc_id, vector.clone(), payload).await?;

    // Wait for indexing (simple consistency)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 5. Search
    println!("Searching...");
    let results = client.search("grpc_rag_demo", vector, 3).await?;
    println!("Found {} results:", results.len());
    for res in results {
        println!(" - {:?}", res);
    }

    Ok(())
}
