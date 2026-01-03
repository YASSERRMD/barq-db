use barq_sdk_rust::{BarqClient, Document, DistanceMetric};
use chrono::Utc;
use rand::Rng;
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    let client = BarqClient::new("http://localhost:8080");
    
    // Check health
    if client.health().await.is_err() {
        eprintln!("Failed to connect to Barq DB");
        return Ok(());
    }
    
    let collection = "system_logs";
    
    // Create Collection
    println!("Creating collection '{}'...", collection);
    // Ignore error if exists
    let _ = client.create_collection(
        collection,
        128, // Small vector for log embedding
        DistanceMetric::L2,
    ).await;
    
    println!("Simulating log ingestion...");
    
    let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    let components = ["auth", "payment", "ui", "database"];
    
    for i in 0..100 {
        let mut rng = rand::thread_rng();
        let level = levels[rng.gen_range(0..4)];
        let component = components[rng.gen_range(0..4)];
        let timestamp = Utc::now();
        let message = format!("{} event in component {}", level, component);
        
        // Mock embedding
        let vector: Vec<f32> = (0..128).map(|_| rng.gen()).collect();
        
        let doc = Document {
            id: format!("log_{}", i).parse()?, // Assuming DocumentId parsing
            vector,
            payload: Some(json!({
                "level": level,
                "component": component,
                "timestamp": timestamp.to_rfc3339(),
                "message": message
            })),
        };
        
        client.insert_document(collection, doc, false).await?;
        
        if i % 10 == 0 {
            print!(".");
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }
    println!("\nIngestion complete.");
    
    // Search for Errors
    println!("Searching for ERROR logs...");
    // Create a vector representing "error" (mock)
    let query_vector: Vec<f32> = (0..128).map(|_| 0.9).collect();
    
    let results = client.search(collection, query_vector, 5, None).await?;
    
    for hit in results {
        println!("- [Score: {:.4}] {:?}", hit.score, hit.payload);
    }
    
    Ok(())
}
