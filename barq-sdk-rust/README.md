# Barq SDK for Rust

<p align="center">
  <a href="https://crates.io/crates/barq-sdk-rust"><img src="https://img.shields.io/crates/v/barq-sdk-rust.svg" alt="crates.io"></a>
  <a href="https://docs.rs/barq-sdk-rust"><img src="https://docs.rs/barq-sdk-rust/badge.svg" alt="docs.rs"></a>
  <a href="https://github.com/YASSERRMD/barq-db/blob/main/LICENSE"><img src="https://img.shields.io/github/license/YASSERRMD/barq-db" alt="License"></a>
</p>

The official Rust SDK for [Barq DB](https://github.com/YASSERRMD/barq-db) - a high-performance vector database built in Rust.

---

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
barq-sdk-rust = { path = "../barq-sdk-rust" }
# Or from crates.io when published:
# barq-sdk-rust = "0.1"
```

---

## Quick Start

```rust
use barq_sdk_rust::{BarqClient, DistanceMetric};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize client
    let client = BarqClient::new("http://localhost:8080", "your-api-key");

    // Health check
    client.health().await?;

    // Create collection
    client.create_collection(
        "products",
        384,
        DistanceMetric::Cosine,
        None,
        None
    ).await?;

    // Insert document
    let vector: Vec<f32> = vec![0.1; 384];
    let collection = client.collection("products");
    collection.insert(
        1u64,
        vector.clone(),
        Some(json!({"name": "Widget", "price": 29.99}))
    ).await?;

    // Search
    let results = collection.search(
        Some(vector),
        None,
        10,
        None,
        None
    ).await?;

    for r in results {
        println!("ID: {}, Score: {}", r["id"], r["score"]);
    }

    Ok(())
}
```

---

## HTTP Client

### Initialization

```rust
use barq_sdk_rust::BarqClient;

let client = BarqClient::new("http://localhost:8080", "your-api-key");
```

### Health Check

```rust
client.health().await?;
println!("Server is healthy");
```

### Create Collection

```rust
use barq_sdk_rust::{BarqClient, DistanceMetric, TextFieldRequest};
use serde_json::json;

// Basic collection
client.create_collection(
    "embeddings",
    768,
    DistanceMetric::L2,
    None,
    None
).await?;

// With text fields for hybrid search
client.create_collection(
    "articles",
    384,
    DistanceMetric::Cosine,
    None,
    Some(vec![
        TextFieldRequest { name: "title".into(), indexed: true, required: true },
        TextFieldRequest { name: "content".into(), indexed: true, required: false },
    ])
).await?;

// With custom index
client.create_collection(
    "products",
    256,
    DistanceMetric::Cosine,
    Some(json!({"type": "hnsw", "m": 16, "ef_construction": 200})),
    None
).await?;
```

### Insert Documents

```rust
use serde_json::json;

let collection = client.collection("products");

// With u64 ID
collection.insert(
    1u64,
    vector.clone(),
    Some(json!({"name": "Widget", "price": 29.99}))
).await?;

// With string ID
collection.insert(
    "doc-001",
    vector.clone(),
    Some(json!({"category": "electronics"}))
).await?;

// Without payload
collection.insert(42u64, vector.clone(), None).await?;
```

### Vector Search

```rust
let results = collection.search(
    Some(query_vector),  // vector
    None,                // query (text)
    10,                  // top_k
    None,                // filter
    None                 // weights
).await?;

for result in results {
    println!("ID: {}", result["id"]);
    println!("Score: {}", result["score"]);
    println!("Payload: {}", result["payload"]);
}
```

### Text Search (BM25)

```rust
let results = collection.search(
    None,
    Some("machine learning tutorial".into()),
    10,
    None,
    None
).await?;
```

### Hybrid Search

```rust
use barq_core::HybridWeights;

let results = collection.search(
    Some(query_vector),
    Some("neural networks".into()),
    10,
    None,
    Some(HybridWeights { vector: 0.7, text: 0.3 })
).await?;
```

### Filtered Search

```rust
use barq_core::Filter;

let filter = Filter::And(vec![
    Filter::Eq("category".into(), "electronics".into()),
    Filter::Lte("price".into(), 100.0.into()),
]);

let results = collection.search(
    Some(query_vector),
    None,
    10,
    Some(filter),
    None
).await?;
```

### Batch Search

For high-throughput applications, you can search multiple vectors in parallel:

```rust
use barq_sdk_rust::SearchQuery;

let queries = vec![
    SearchQuery { 
        vector: vec![0.1; 384], 
        filter: None 
    },
    SearchQuery { 
        vector: vec![0.2; 384], 
        filter: Some(Filter::Eq("category".into(), "books".into())) 
    },
];

// Returns Vec<Vec<serde_json::Value>>
// Each inner Vec corresponds to a query in the batch
let batch_results = collection.batch_search(queries, 10).await?;

for (i, results) in batch_results.iter().enumerate() {
    println!("Query {} matched {} documents", i, results.len());
}
```

---

## gRPC Client

For high-throughput applications:

```rust
use barq_sdk_rust::{BarqGrpcClient, DistanceMetric};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect
    let mut client = BarqGrpcClient::connect("http://localhost:50051".into()).await?;

    // Health check
    let ok = client.health().await?;
    println!("Healthy: {}", ok);

    // Create collection
    client.create_collection("vectors", 384, DistanceMetric::L2).await?;

    // Insert document
    client.insert_document(
        "vectors",
        "doc-001",
        vec![0.1; 384],
        json!({"label": "example"})
    ).await?;

    // Search
    let results = client.search("vectors", vec![0.1; 384], 10).await?;
    for r in results {
        println!("{}: {}", r["id"], r["score"]);
    }

    Ok(())
}
```

---

## API Reference

### Types

```rust
pub enum DistanceMetric {
    L2,
    Cosine,
    Dot,
}

pub struct TextFieldRequest {
    pub name: String,
    pub indexed: bool,
    pub required: bool,
}

pub enum BarqError {
    Http(reqwest::Error),
    Api { status: StatusCode, message: String },
    Serialization(serde_json::Error),
    Grpc(tonic::Status),
    Transport(tonic::transport::Error),
}
```

### `BarqClient` (HTTP)

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `(base_url, api_key) -> Self` | Create client |
| `health` | `(&self) -> Result<()>` | Health check |
| `create_collection` | `(&self, name, dimension, metric, index, text_fields) -> Result<()>` | Create collection |
| `collection` | `(&self, name) -> Collection` | Get collection reference |

### `Collection`

| Method | Signature | Description |
|--------|-----------|-------------|
| `insert` | `(&self, id, vector, payload) -> Result<()>` | Insert document |
| `search` | `(&self, vector, query, top_k, filter, weights) -> Result<Vec<Value>>` | Search |

### `BarqGrpcClient`

| Method | Signature | Description |
|--------|-----------|-------------|
| `connect` | `(dst) -> Result<Self>` | Connect to server |
| `health` | `(&mut self) -> Result<bool>` | Health check |
| `create_collection` | `(&mut self, name, dimension, metric) -> Result<()>` | Create collection |
| `insert_document` | `(&mut self, collection, id, vector, payload) -> Result<()>` | Insert |
| `search` | `(&mut self, collection, vector, top_k) -> Result<Vec<Value>>` | Search |

---

## Examples

### RAG Application

```rust
use barq_sdk_rust::{BarqClient, DistanceMetric};
use serde_json::json;

async fn rag_example() -> Result<(), Box<dyn std::error::Error>> {
    let client = BarqClient::new("http://localhost:8080", "api-key");

    // Create knowledge base
    client.create_collection("knowledge", 384, DistanceMetric::Cosine, None, None).await?;

    let collection = client.collection("knowledge");

    // Index documents (embeddings from your model)
    let documents = vec![
        ("Python is a programming language", vec![0.1; 384]),
        ("Rust is fast and safe", vec![0.2; 384]),
        ("Vector databases store embeddings", vec![0.3; 384]),
    ];

    for (i, (text, embedding)) in documents.iter().enumerate() {
        collection.insert(
            i as u64,
            embedding.clone(),
            Some(json!({"text": text}))
        ).await?;
    }

    // Query
    let query_embedding = vec![0.15; 384];
    let results = collection.search(Some(query_embedding), None, 3, None, None).await?;

    for r in results {
        println!("[{}] {}", r["score"], r["payload"]["text"]);
    }

    Ok(())
}
```

### Async Batch Insert

```rust
use futures::future::join_all;

async fn batch_insert(
    collection: &Collection,
    documents: Vec<(u64, Vec<f32>, serde_json::Value)>
) -> Result<(), BarqError> {
    let futures: Vec<_> = documents
        .into_iter()
        .map(|(id, vector, payload)| {
            collection.insert(id, vector, Some(payload))
        })
        .collect();

    let results = join_all(futures).await;

    for result in results {
        result?;
    }

    Ok(())
}
```

---

## Requirements

- Rust 1.70+
- Tokio runtime

### Dependencies

```toml
[dependencies]
reqwest = { version = "0.11", features = ["json"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tonic = "0.9"
thiserror = "1"
```

---

## Contributing

We welcome contributions! See the [main repository](https://github.com/YASSERRMD/barq-db) for guidelines.

### Areas for Improvement

- Connection pooling configuration
- Retry policies with backoff
- Streaming responses for large datasets
- Builder pattern for requests
- More comprehensive error types
- Integration tests

---

## License

MIT License - see [LICENSE](https://github.com/YASSERRMD/barq-db/blob/main/LICENSE)

---

<p align="center">
  <a href="https://github.com/YASSERRMD/barq-db">Barq DB</a> - Vector search at lightning speed
</p>
