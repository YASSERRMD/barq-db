# Barq DB

**Rust-Native, Cloud-Ready Vector & Hybrid Search Database**

Barq is a lightning-fast vector database and hybrid search engine that unifies dense vectors, BM25-style sparse signals, and rich metadata filters into a single, cloud-native platform for RAG, semantic search, and recommendation workloads. 

Its architecture focuses on three pillars: **extreme performance**, **multi-tenant cloud architecture**, and **first-class hybrid retrieval** tailored for global use cases.

---

## ⚡ Getting Started

### Option 1: Docker (Recommended)
The easiest way to run Barq DB is via Docker Compose.

```bash
# Start Barq DB
docker-compose up -d

# Check logs
docker-compose logs -f
```
The API will be available at `http://localhost:8080`.

### Option 2: Build from Source
If you have Rust installed (1.74+):

```bash
# Run the server
cargo run --bin barq-server
```

---

## 🚀 Usage Example (Python RAG Demo)

Barq comes with a ready-to-run RAG demo using the Python SDK.

1. **Install Dependencies** (or ensure `httpx` is installed):
   ```bash
   pip install httpx
   ```

2. **Run the Demo**:
   ```bash
   python3 examples/rag_demo.py
   ```
   This script creates a collection, ingests document chunks, and performs a hybrid search.

---

## Capabilities & Architecture

### Core Architecture
- **Rust + Tokio**: Async-first core with verifiable safety and performance.
- **Storage**: Pluggable engine architecture (File-based WAL + Snapshots implemented, object-storage hooks designed).
- **Indexing**: Multi-index support including **HNSW**, **IVF**, and **Flat** (exact) search with SIMD-optimized distance kernels (L2, Cosine, Dot Product).
- **Clustering**: Sharded, replicated storage with Raft-based consensus for high availability.

### Hybrid Search & Relevance
- **Native BM25**: Integrated sparse scoring engine with tokenization and term statistics.
- **Hybrid Pipeline**: Single-query execution of Vector + Keyword search with RRF/Weighted fusion.
- **Languages**: Tokenization support for English and others; extensible analyzer pipeline.

### Data Model & APIs
- **Collections**: Typed schema with configurable dimensions, metrics, and index types.
- **Rich Filtering**: Boolean, Numeric Range, and Metadata filters pushed down to the index.
- **APIs**: REST (Axum) and internal RPCs.
- **SDKs**: First-party clients for **Rust**, **Python**, **TypeScript/Node**, and **Go**.

### Multi-Tenancy & Security
- **Hard Multi-Tenancy**: Namespace isolation per tenant.
- **RBAC**: Role-Based Access Control (Admin, Writer, Reader) enforced at the API level.
- **Isolation**: Per-tenant quota tracking and usage metrics.

### Observability
- **Metrics**: Native Prometheus integration (latency, QPS, index stats).
- **Tracing**: OpenTelemetry hooks.
- **Admin**: CLI and API for topology management and compaction.

---

## Implementation Status (Phases 1-10 Complete)

Barq DB development is organized into 10 phases, all of which are currently implemented in the codebase:

| Phase | Feature Set | Status |
|-------|-------------|--------|
| **1** | **Core & Persistence** | ✅ Implemented (WAL, Snapshots, CRUD) |
| **2** | **Hybrid Search** | ✅ Implemented (BM25, RRF, Hybrid Query) |
| **3** | **ANN Indexes** | ✅ Implemented (HNSW, IVF, PQ) |
| **4** | **Filtering** | ✅ Implemented (Metadata filters) |
| **5** | **Multi-Tenancy** | ✅ Implemented (Tenant Isolation) |
| **6** | **Cluster/Sharding** | ✅ Implemented (Sharding, basic Replication) |
| **7** | **Storage Engine v2** | ✅ Implemented (Snapshots, compaction) |
| **8** | **Security** | ✅ Implemented (RBAC, API Keys) |
| **9** | **Observability** | ✅ Implemented (Metrics, Admin API) |
| **10** | **SDKs & Ecosystem** | ✅ Implemented (Rust, Python, TS, Go available) |

### Roadmap / In-Progress
- 🚧 Object Storage Tiering (S3/GCS) - *Architecture Ready*
- 🚧 Helm Charts & K8s Operator - *Planned*
- 🚧 Advanced Arabic Analyzers - *Planned*

---

## Crate Layout

This project is a Cargo Workspace containing the following crates:

- `barq-core`: Core data structures, catalog, and type system.
- `barq-index`: Vector indexes (HNSW, IVF, Flat) and distance kernels.
- `barq-bm25`: Text search engine and analyzers.
- `barq-storage`: Persistence, WAL, and snapshot management.
- `barq-cluster`: Distributed node logic, sharding, and consensus.
- `barq-api`: HTTP REST API, Auth, and Request validation.
- `barq-admin`: Admin tooling and CLI.
- `barq-sdk-rust`: Official Rust client.
- `barq-sdk-ts`: TypeScript client.
- `barq-sdk-go`: Go client.
- `barq-sdk-python`: Python client.

## License
Apache-2.0
