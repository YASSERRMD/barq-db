# Introduction

**Barq DB** is a high-performance, distributed, cloud-native vector database designed for modern AI applications. It combines the speed of dense vector search with the precision of keyword-based retrieval (BM25), all in a single, unified system.

## Key Features

- **Blazing Fast Vector Search**: Built on HNSW and IVF indexes with SIMD optimizations for maximum throughput.
- **Hybrid Retrieval**: Seamlessly blend vector similarity with BM25 keyword scores using Reciprocal Rank Fusion (RRF).
- **Storage Tiering**: Automatically manage data lifecycle by moving cold segments to cheaper object storage (S3, GCS, Azure Blob).
- **Multi-Tenancy**: Native support for tenant isolation, quotas, and role-based access control (RBAC).
- **Cloud Native**: Designed for Kubernetes with a custom operator, stateless query nodes, and decoupled storage.

## Why Barq DB?

While many vector databases exist, Barq focuses on:
1.  **Operational Simplicity**: Easy to deploy and manage with our Kubernetes Operator.
2.  **Cost Efficiency**: Tiering lets you store PB-scale datasets without PB-scale SSD costs.
3.  **Developer Experience**: Typed SDKs for Python, TypeScript, Go, and Rust.

## Getting Started

Ready to dive in? Check out the [Installation Guide](getting-started/installation.md) to set up your first cluster in minutes.
