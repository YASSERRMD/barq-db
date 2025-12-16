use barq_sdk_rust::{BarqClient, DistanceMetric, DocumentId};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize Client
    let client = BarqClient::new("http://localhost:8080", "rust-key");
    println!("🔌 Connected to Barq DB at http://localhost:8080");

    // 2. Health Check
    match client.health().await {
        Ok(_) => println!("✅ Health check passed"),
        Err(e) => {
            eprintln!("❌ Health check failed: {}", e);
            eprintln!("Make sure 'barq-server' is running on port 8080.");
            return Ok(());
        }
    }

    // 3. Create Collection
    let collection_name = "rust_rag_demo";
    println!("📦 Creating collection '{}'...", collection_name);
    // Create with L2 metric, no special index config (None), no text fields (None)
    let _ = client.create_collection(
        collection_name, 
        4, 
        DistanceMetric::L2, 
        None, 
        None
    ).await;

    let collection = client.collection(collection_name);

    // 4. Ingest Documents (Simulated Vector Embeddings)
    let documents = vec![
        (1, "Rust is a systems programming language.", vec![0.1, 0.2, 0.3, 0.4]),
        (2, "Python is great for data science.", vec![0.9, 0.8, 0.7, 0.6]),
        (3, "Memory safety is a key feature of Rust.", vec![0.11, 0.21, 0.31, 0.41]),
        (4, "Machine learning models need lots of data.", vec![0.88, 0.77, 0.66, 0.55]),
    ];

    println!("📥 Ingesting {} documents...", documents.len());
    for (id, text, vector) in documents {
        let payload = json!({
            "text": text,
            "source": "rust_example"
        });
        
        // Wrap ID in DocumentId::U64
        collection.insert(DocumentId::U64(id), vector, Some(payload)).await?;
    }

    // Allow some time for async indexing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 5. Semantic Search
    let query_vector = vec![0.1, 0.2, 0.3, 0.4]; // Close to Rust docs
    println!("🔍 Searching for documents related to 'Rust' (vector sim)...");
    
    // search(vector, query, top_k, filter, weights)
    let results = collection.search(
        Some(query_vector), 
        None, 
        3, 
        None, 
        None
    ).await?;
    
    println!("📊 Found {} results:", results.len());
    for (i, result) in results.iter().enumerate() {
        println!("  {}. ID: {:?}, Score: {:.4}", i + 1, result["id"], result["score"]);
    }

    Ok(())
}
