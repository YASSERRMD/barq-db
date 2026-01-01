# Barq SDK for Python

<p align="center">
  <a href="https://pypi.org/project/barq-sdk-python/"><img src="https://img.shields.io/pypi/v/barq-sdk-python.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/barq-sdk-python/"><img src="https://img.shields.io/pypi/pyversions/barq-sdk-python.svg" alt="Python versions"></a>
  <a href="https://github.com/YASSERRMD/barq-db/blob/main/LICENSE"><img src="https://img.shields.io/github/license/YASSERRMD/barq-db" alt="License"></a>
</p>

The official Python SDK for [Barq DB](https://github.com/YASSERRMD/barq-db) — a blazing-fast, lightweight vector database built in Rust.

## Installation

```bash
pip install barq-sdk-python
```

## Quick Start

```python
from barq import BarqClient

client = BarqClient("http://localhost:8080", api_key="your-api-key")

# Create a collection
client.create_collection(name="products", dimension=384, metric="L2")

# Insert vectors
client.insert_document(
    collection="products",
    id=1,
    vector=[0.12, 0.34, ...],  # 384-dim vector
    payload={"name": "Widget", "category": "electronics"}
)

# Search
results = client.search(collection="products", vector=query_vector, top_k=10)
```

## Features

- **HTTP & gRPC Support** — Choose the protocol that fits your infrastructure
- **Hybrid Search** — Combine vector similarity with text-based queries
- **Filtering** — Apply metadata filters to narrow search results
- **Async-Ready** — Built on `httpx` for modern async applications

## Usage

### HTTP Client

```python
from barq import BarqClient

client = BarqClient(base_url="http://localhost:8080", api_key="your-key")

# Health check
assert client.health()

# Create collection with text fields for hybrid search
client.create_collection(
    name="documents",
    dimension=768,
    metric="Cosine",
    text_fields=["title", "content"]
)

# Insert with payload
client.insert_document(
    collection="documents",
    id="doc-001",
    vector=embedding,
    payload={"title": "Introduction", "content": "..."}
)

# Vector search
results = client.search(collection="documents", vector=query_embedding, top_k=5)

# Hybrid search (vector + text)
results = client.search(
    collection="documents",
    vector=query_embedding,
    query="machine learning",
    top_k=5
)

client.close()
```

### gRPC Client

```python
from barq import GrpcClient

client = GrpcClient(target="localhost:50051")

client.create_collection(name="embeddings", dimension=384, metric="L2")
client.insert_document(collection="embeddings", id="1", vector=embedding, payload={"label": "A"})

results = client.search(collection="embeddings", vector=query, top_k=10)
```

## API Reference

### `BarqClient`

| Method | Parameters | Description |
|--------|------------|-------------|
| `health()` | — | Returns `True` if server is healthy |
| `create_collection()` | `name`, `dimension`, `metric`, `index`, `text_fields` | Create a new collection |
| `insert_document()` | `collection`, `id`, `vector`, `payload` | Insert a document with vector and metadata |
| `search()` | `collection`, `vector`, `query`, `top_k`, `filter` | Search by vector, text, or hybrid |
| `close()` | — | Close the HTTP connection |

### `GrpcClient`

| Method | Parameters | Description |
|--------|------------|-------------|
| `health()` | — | Returns `True` if server is healthy |
| `create_collection()` | `name`, `dimension`, `metric` | Create a new collection |
| `insert_document()` | `collection`, `id`, `vector`, `payload` | Insert a document |
| `search()` | `collection`, `vector`, `top_k` | Vector similarity search |

## Requirements

- Python 3.8+
- Dependencies: `httpx`, `grpcio`, `protobuf`

## Contributing

We welcome contributions! Here's how you can help:

1. **Report bugs** — Open an issue with reproduction steps
2. **Suggest features** — Describe your use case and proposed solution
3. **Submit PRs** — Fork the repo, create a branch, and submit a pull request

### Development Setup

```bash
git clone https://github.com/YASSERRMD/barq-db.git
cd barq-db/barq-sdk-python
pip install -e ".[dev]"
```

### Areas for Contribution

- Async client implementation (`httpx` async support)
- Connection pooling and retry logic
- Additional index configuration options
- Batch insert/upsert operations
- Improved error handling and custom exceptions
- Unit and integration tests

## License

MIT License — see [LICENSE](https://github.com/YASSERRMD/barq-db/blob/main/LICENSE) for details.

---

<p align="center">
  <b>Barq DB</b> — Vector search at lightning speed ⚡
</p>
