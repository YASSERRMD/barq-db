# Barq SDK for Python

<p align="center">
  <a href="https://pypi.org/project/barq-sdk-python/"><img src="https://img.shields.io/pypi/v/barq-sdk-python.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/barq-sdk-python/"><img src="https://img.shields.io/pypi/pyversions/barq-sdk-python.svg" alt="Python"></a>
  <a href="https://github.com/YASSERRMD/barq-db/blob/main/LICENSE"><img src="https://img.shields.io/github/license/YASSERRMD/barq-db" alt="License"></a>
</p>

The official Python SDK for [Barq DB](https://github.com/YASSERRMD/barq-db) - a high-performance vector database built in Rust.

---

## Installation

```bash
pip install barq-sdk-python
```

---

## Quick Start

```python
from barq import BarqClient

client = BarqClient("http://localhost:8080", api_key="your-api-key")

# Create a collection
client.create_collection(name="documents", dimension=384, metric="Cosine")

# Insert a document
client.insert_document(
    collection="documents",
    id=1,
    vector=[0.1] * 384,
    payload={"title": "Hello World", "category": "greeting"}
)

# Search
results = client.search(collection="documents", vector=[0.1] * 384, top_k=5)
for r in results:
    print(f"ID: {r['id']}, Score: {r['score']:.4f}")

client.close()
```

---

## HTTP Client

The `BarqClient` communicates with Barq DB over HTTP/REST.

### Initialization

```python
from barq import BarqClient

client = BarqClient(
    base_url="http://localhost:8080",
    api_key="your-api-key"
)
```

### Health Check

```python
if client.health():
    print("Server is healthy")
```

### Create Collection

```python
# Basic collection
client.create_collection(
    name="embeddings",
    dimension=768,
    metric="L2"  # Options: "L2", "Cosine", "Dot"
)

# With text fields for hybrid search
client.create_collection(
    name="articles",
    dimension=384,
    metric="Cosine",
    text_fields=[
        {"name": "title", "indexed": True, "required": True},
        {"name": "content", "indexed": True, "required": False}
    ]
)

# With custom index configuration
client.create_collection(
    name="products",
    dimension=256,
    metric="Cosine",
    index={"type": "hnsw", "m": 16, "ef_construction": 200}
)
```

### Insert Documents

```python
# Single document
client.insert_document(
    collection="products",
    id=1,                          # int or string
    vector=[0.12, 0.34, ...],      # list of floats
    payload={"name": "Widget", "price": 29.99, "in_stock": True}
)

# Batch insert (loop)
documents = [
    {"id": i, "vector": embeddings[i], "payload": {"text": texts[i]}}
    for i in range(len(embeddings))
]
for doc in documents:
    client.insert_document(collection="products", **doc)
```

### Vector Search

```python
results = client.search(
    collection="products",
    vector=query_embedding,
    top_k=10
)

for result in results:
    print(f"ID: {result['id']}")
    print(f"Score: {result['score']:.4f}")
    print(f"Payload: {result.get('payload', {})}")
```

### Text Search (BM25)

```python
results = client.search(
    collection="articles",
    query="machine learning tutorial",
    top_k=10
)
```

### Hybrid Search

Combine vector similarity with keyword matching:

```python
results = client.search(
    collection="articles",
    vector=query_embedding,
    query="neural networks",
    top_k=10
)
```

### Filtered Search

```python
results = client.search(
    collection="products",
    vector=query_embedding,
    top_k=10,
    filter={
        "must": [
            {"field": "category", "match": "electronics"},
            {"field": "price", "range": {"lte": 100}}
        ]
    }
)
```

---

## gRPC Client

For high-throughput applications, use the gRPC client:

```python
from barq import GrpcClient

client = GrpcClient(target="localhost:50051")

# Health check
if client.health():
    print("Connected via gRPC")

# Create collection
client.create_collection(
    name="vectors",
    dimension=384,
    metric="L2"
)

# Insert document
client.insert_document(
    collection="vectors",
    id="doc-001",
    vector=[0.1] * 384,
    payload={"label": "example"}
)

# Search
results = client.search(
    collection="vectors",
    vector=[0.1] * 384,
    top_k=5
)

for r in results:
    print(f"{r['id']}: {r['score']}")
```

---

## API Reference

### `BarqClient`

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| `health()` | - | `bool` | Check server health |
| `create_collection()` | `name`, `dimension`, `metric`, `index`, `text_fields` | `dict` | Create collection |
| `insert_document()` | `collection`, `id`, `vector`, `payload` | `dict` | Insert document |
| `search()` | `collection`, `vector`, `query`, `top_k`, `filter` | `list[dict]` | Search documents |
| `close()` | - | - | Close connection |

### `GrpcClient`

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| `health()` | - | `bool` | Check server health |
| `create_collection()` | `name`, `dimension`, `metric` | - | Create collection |
| `insert_document()` | `collection`, `id`, `vector`, `payload` | - | Insert document |
| `search()` | `collection`, `vector`, `top_k` | `list[dict]` | Search documents |

---

## Examples

### RAG Application

```python
from barq import BarqClient
from sentence_transformers import SentenceTransformer

# Initialize
client = BarqClient("http://localhost:8080", "api-key")
model = SentenceTransformer("all-MiniLM-L6-v2")

# Create collection
client.create_collection(name="knowledge", dimension=384, metric="Cosine")

# Index documents
documents = [
    "Python is a programming language",
    "Machine learning uses neural networks",
    "Vector databases store embeddings"
]

for i, doc in enumerate(documents):
    embedding = model.encode(doc).tolist()
    client.insert_document(
        collection="knowledge",
        id=i,
        vector=embedding,
        payload={"text": doc}
    )

# Query
query = "What is machine learning?"
query_vec = model.encode(query).tolist()
results = client.search(collection="knowledge", vector=query_vec, top_k=3)

for r in results:
    print(f"[{r['score']:.3f}] {r['payload']['text']}")
```

---

## Requirements

- Python 3.8+
- `httpx >= 0.23`
- `grpcio >= 1.50.0`
- `protobuf >= 4.21.0`

---

## Contributing

We welcome contributions! See the [main repository](https://github.com/YASSERRMD/barq-db) for guidelines.

### Areas for Improvement

- Async client (`httpx` async support)
- Connection pooling and retry logic
- Batch insert operations
- Custom exception classes
- Comprehensive test suite

---

## License

MIT License - see [LICENSE](https://github.com/YASSERRMD/barq-db/blob/main/LICENSE)

---

<p align="center">
  <a href="https://github.com/YASSERRMD/barq-db">Barq DB</a> - Vector search at lightning speed
</p>
