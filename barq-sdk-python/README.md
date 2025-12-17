# Barq SDK Python

Official Python client for [Barq DB](https://github.com/YASSERRMD/barq-db) - a high-performance vector database.

## Installation

```bash
pip install barq-sdk-python
```

## Quick Start

### HTTP Client

```python
from barq import BarqClient

# Initialize client
client = BarqClient("http://localhost:8080", api_key="your-api-key")

# Check health
if client.health():
    print("Connected!")

# Create a collection
client.create_collection(
    name="documents",
    dimension=384,
    metric="L2"
)

# Insert a document
client.insert_document(
    collection="documents",
    id=1,
    vector=[0.1] * 384,
    payload={"title": "Hello World"}
)

# Search
results = client.search(
    collection="documents",
    vector=[0.1] * 384,
    top_k=5
)

for result in results:
    print(f"ID: {result['id']}, Score: {result['score']}")

client.close()
```

### gRPC Client

```python
from barq import GrpcClient

# Initialize gRPC client
client = GrpcClient("localhost:50051")

# Check health
if client.health():
    print("Connected via gRPC!")

# Create collection
client.create_collection(
    name="documents",
    dimension=384,
    metric="L2"
)

# Insert document
client.insert_document(
    collection="documents",
    id="doc1",
    vector=[0.1] * 384,
    payload={"title": "Hello World"}
)

# Search
results = client.search(
    collection="documents",
    vector=[0.1] * 384,
    top_k=5
)

for result in results:
    print(f"ID: {result['id']}, Score: {result['score']}")
```

## API Reference

### BarqClient (HTTP)

| Method | Description |
|--------|-------------|
| `health()` | Check server health |
| `create_collection(name, dimension, metric, index, text_fields)` | Create a new collection |
| `insert_document(collection, id, vector, payload)` | Insert a document |
| `search(collection, vector, query, top_k, filter)` | Search for similar vectors |
| `close()` | Close the client connection |

### GrpcClient

| Method | Description |
|--------|-------------|
| `health()` | Check server health |
| `create_collection(name, dimension, metric)` | Create a new collection |
| `insert_document(collection, id, vector, payload)` | Insert a document |
| `search(collection, vector, top_k)` | Search for similar vectors |

## Requirements

- Python >= 3.8
- httpx >= 0.23
- grpcio >= 1.50.0
- protobuf >= 4.21.0

## License

MIT License - see [LICENSE](../LICENSE) for details.
