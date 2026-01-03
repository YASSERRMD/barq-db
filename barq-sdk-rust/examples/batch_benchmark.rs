use barq_sdk_rust::{BarqClient, DistanceMetric, SearchQuery};
use serde_json::json;
use std::time::Instant;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = BarqClient::new("http://localhost:8080", "test-key");

    // 1. Setup Collection
    let collection_name = "benchmark_test";
    println!("Creating collection '{}'...", collection_name);
    // Ignore error if exists (or explicit drop?)
    let _ = client.create_collection(collection_name, 128, DistanceMetric::L2, None, None).await;
    
    let collection = client.collection(collection_name);

    // 2. Insert Documents (Background Setup)
    println!("Inserting 1000 documents...");
    let mut futures = Vec::new();
    for i in 0..1000 {
        let vector: Vec<f32> = (0..128).map(|x| (x as f32 + i as f32) / 1000.0).collect();
        futures.push(collection.insert(i as u64, vector, Some(json!({"id": i}))));
    }
    
    // Simple serial insert for setup (can be optimized but not the focus)
    for f in futures {
        f.await?;
    }
    
    // Allow indexing to catch up slightly (though it is synchronous usually)
    sleep(std::time::Duration::from_millis(100)).await;

    // 3. Prepare Queries
    let num_queries = 100;
    let queries: Vec<Vec<f32>> = (0..num_queries)
        .map(|i| (0..128).map(|x| (x as f32 + i as f32) / 1000.0).collect())
        .collect();

    // 4. Sequential Search Benchmark
    println!("\nRunning {} sequential searches...", num_queries);
    let start_seq = Instant::now();
    for q in &queries {
        collection.search(Some(q.clone()), None, 10, None, None).await?;
    }
    let duration_seq = start_seq.elapsed();
    println!("Sequential: {:?} (Average: {:?}/req)", duration_seq, duration_seq / num_queries as u32);

    // 5. Batch Search Benchmark
    println!("\nRunning batch search with {} queries...", num_queries);
    let batch_queries: Vec<SearchQuery> = queries.iter()
        .map(|v| SearchQuery { vector: v.clone(), filter: None })
        .collect();

    let start_batch = Instant::now();
    collection.batch_search(batch_queries, 10).await?;
    let duration_batch = start_batch.elapsed();
    println!("Batch:      {:?} (Speedup: {:.2}x)", duration_batch, duration_seq.as_secs_f64() / duration_batch.as_secs_f64());

    Ok(())
}
