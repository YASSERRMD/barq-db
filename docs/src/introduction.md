# Introduction

**Barq DB** is a high-performance, distributed, cloud-native vector database designed for modern AI applications. It combines the speed of dense vector search with the precision of keyword-based retrieval (BM25), all in a single, unified system.

## Key Features

- **Blazing Fast Vector Search**: Built on HNSW and IVF indexes with SIMD optimizations for maximum throughput.
- **Hybrid Retrieval**: Seamlessly blend vector similarity with BM25 keyword scores using weighted score fusion.
- **Storage Tiering**: Automatically manage data lifecycle by moving cold segments to cheaper object storage (S3, GCS, Azure Blob).
- **Multi-Tenancy**: Native support for tenant isolation, quotas, and role-based access control (RBAC).
- **Cloud Native**: Designed for Kubernetes with a custom operator, stateless query nodes, and decoupled storage.

## Why Barq DB?

While many vector databases exist, Barq focuses on:
1.  **Operational Simplicity**: Easy to deploy and manage with our Kubernetes Operator.
2.  **Cost Efficiency**: Tiering lets you store PB-scale datasets without PB-scale SSD costs.
3.  **Developer Experience**: Typed SDKs for Python, TypeScript, Go, and Rust, aligned to the canonical gRPC contract in `proto/barq.proto`.

## Barq v2 Main Delivery Phases

1. **Phase 1**: Vector store foundation, mmap-backed persistence, memory budgeting, and restart hydration.
2. **Phase 2**: Segment lifecycle, sealing, compaction behavior, persisted lifecycle replay, and lifecycle stress coverage.
3. **Phase 3**: Deterministic benchmark tooling through `barq-bench`.
4. **Phase 4**: Async ingestion with queueing, batching, backpressure, and ingestion metrics.
5. **Phase 5**: Explicit index lifecycle with `Building`, `Ready`, and `Stale` states.
6. **Phase 6**: Honest cluster capability reporting and explicit durability semantics.
7. **Phase 7**: Query planning improvements, explicit hybrid execution, and deterministic merge behavior.
8. **Phase 8**: Observability across ingestion, storage, indexing, query latency, and admin metrics.

## Getting Started

Ready to dive in? Check out the [Installation Guide](getting-started/installation.md) to set up your first cluster in minutes.
