# Barq DB

Barq is a Rust-native, cloud-ready vector and hybrid search database designed to be fully buildable and maintainable by an automated code agent. This document captures the initial architecture scope and milestones for the project.

## Core Architecture & Crate Layout
- **Language/runtime**: Rust, async-first (Tokio), strictly typed, no `unsafe` except in isolated, audited kernels.
- **Crates**:
  - `barq-core`: core data structures, collection catalog, type system.
  - `barq-index`: vector indexes (HNSW, IVF, flat), quantization, distance kernels.
  - `barq-bm25`: text indexing, term stats, BM25 scoring, analyzers.
  - `barq-storage`: persistence layer, WAL, snapshots, object-store integration.
  - `barq-cluster`: node membership, sharding, replication, Raft/consensus.
  - `barq-api`: REST/gRPC server, auth, request validation.
  - `barq-admin`: admin APIs, schema migration, health checks.
  - `barq-sdk-*`: language SDKs (Rust, Python, TS/Node, Go).
- Each crate exposes a small, well-documented surface with clear trait boundaries so a code agent can generate and test them in isolation.

## Data Model & Collections
- **Core concepts**: collection (named container with schema and one or more indexes), vector field (fixed dimension, metric, index type), payload/metadata (JSON-like with primitives, arrays, nested objects, geo, timestamps), document IDs (opaque string or u64, unique per collection).
- **Operations**: create/update/drop collection with schema; insert/upsert/delete single or batch documents; partial payload updates without re-uploading unchanged vectors.
- **Internal**: catalog of collections in an embedded metadata store; schema validation and migration rules covering compatible vs incompatible changes.

## Vector Indexing & Search
- **Distance metrics**: L2, cosine, dot-product, Hamming (for binary) with SIMD acceleration.
- **Index types**: flat (exact k-NN), HNSW (default ANN for high recall), IVF(+PQ) with optional quantization.
- **Features**: async index building and background maintenance; configurable parameters (`ef_search`, `ef_construction`, `M`, `nlist`, `nprobe`); incremental updates; ability to run exact search on candidate subsets for final rerank.

## BM25 Text Engine & Language Support
- **Text indexing**: per-collection inverted index over selected text fields; tracks term frequency, document frequency, and average document length.
- **Scoring**: BM25 with configurable `k1` and `b`; optional field boosts and query-time boosts.
- **Language pipeline**: tokenization and normalization; optional analyzers for English and Arabic (tokenization, stopwords, simple stemming).
- **APIs**: index text independently or as part of payload; query text fields via BM25-only or hybrid modes.

## Hybrid Search & Ranking
- **Query model**: supports vector-only, keyword-only, and hybrid query objects with `top_k`, optional filters, and modality weights.
- **Execution pipeline**: BM25 search and vector search run in parallel against the same candidate space.
- **Fusion**: score normalization per modality; weighted sum or Reciprocal Rank Fusion (RRF); hooks for custom rerankers and an explain API detailing score composition.

## Filtering & Metadata Indexes
- **Capabilities**: equality/inequality, range (numeric/timestamp), in-list, NOT, AND/OR combinations, geo radius/box queries.
- **Implementation**: per-field bitmap or inverted indexes for selective fields; predicate-pushdown scans for low-selectivity fields.
- **Integration**: pre-filtering before ANN search and post-filtering with fallback behavior (e.g., expand search radius/`ef` when candidate pool is too small).

## Storage Layer & Durability
- **Write path**: append-only WAL per shard/collection; batched writes to on-disk structures (LSM-style).
- **Data files**: segment files for vectors, payloads, and indexes with compaction/merge logic.
- **Snapshots & recovery**: point-in-time snapshots (API-triggered); startup recovery via WAL replay plus latest snapshot.
- **Optional cloud storage**: pluggable backends (local FS, S3-compatible, GCS, Azure Blob) with background upload of cold segments.

## Cluster, Sharding & Replication
- **Cluster model**: logical cluster with N nodes hosting shards assigned to primary + replica nodes.
- **Sharding**: hash-based on document or tenant ID; rebalance tool/API for resharding.
- **Replication/consistency**: Raft-based or primary/replica per shard; configurable read preference (primary-only or leader+followers).
- **Node discovery**: initial config files; optional etcd/Consul/ZooKeeper-based membership for advanced deployments.

## Multi-Tenancy
- **Tenant model**: tenant ID as first-class concept; namespaces per tenant with quotas.
- **Isolation**: per-tenant limits (collections, disk, memory, QPS); API auth tokens scoped to tenant/roles.
- **Operational UX**: admin APIs for tenant lifecycle and quotas; usage metrics per tenant.

## APIs & SDKs
- **Endpoints** (REST and gRPC): health/info/metrics; tenant/collection/schema management; insert/upsert/delete; search (vector, keyword, hybrid); snapshot/backup/restore.
- **Definitions**: OpenAPI and Protobuf as single sources of truth.
- **SDKs**: Rust (async, type-safe builders), Python (sync/async for notebooks and RAG), Node/TS (Promise-based), Go (infra-friendly client).

## Security & Access Control
- **Authentication**: API keys scoped to tenant/collection/read-write; optional JWT with pluggable verifier.
- **Transport**: TLS for external endpoints.
- **Authorization**: role-based (admin, read-only, read-write) at tenant and collection level; admin-only endpoints clearly separated.

## Observability & Operations
- **Metrics**: per-node and per-collection (QPS, p50/p95/p99 latency, error rates, index build times, cache hit/miss, memory usage).
- **Logs**: structured JSON for requests, major events, and errors.
- **Tracing**: optional OpenTelemetry integration for distributed query tracing.
- **Admin tools**: CLI/HTTP endpoints for cluster state, shard placements, manual compaction/index rebuild, and draining nodes for maintenance.

## Performance Targets
- **Ingestion**: 100k+ vectors/sec per node with batch inserts on commodity hardware.
- **Query**: p95 < 50 ms for `top_k=10` hybrid queries on medium collections.
- **Scalability**: linear QPS scaling with node count for read-heavy workloads.
- **Resource efficiency**: host 100M+ vectors per node via quantization and cold storage.

## Testing & Validation Plan
- **Unit tests**: core logic across crates (index ops, scoring, filters).
- **Integration**: single-node bring-up covering CRUD + search flows; correctness tests for hybrid scoring and filters.
- **Performance**: synthetic workload generator for bulk load/search; benchmark scripts for reproducible runs.
- **Compatibility**: adapters/scripts to import from JSON/CSV and known vector DB formats.

## Implementation Phases
1. **Minimal single-node core**: `barq-core`, flat index, basic CRUD + vector search, file-based storage.
2. **Add BM25 and hybrid**: `barq-bm25`, analyzers, hybrid pipeline, basic explain.
3. **Add ANN and quantization**: HNSW, IVF, simple PQ, tuning parameters.
4. **Add multi-tenancy and richer filters**: tenant model, quotas, payload indexes.
5. **Add clustering and replication**: sharding, Raft or primary/replica, rebalance tools.
6. **Optimize and harden**: SIMD, caches, observability, SDKs, admin tooling.


## Development Quickstart
This repository is now a Cargo workspace. To work with the in-memory core prototype:

1. Install Rust (1.74+ recommended).
2. Run tests for all crates:
   ```bash
   cargo test
   ```
3. Explore the crates:
   - `barq-index`: foundational vector primitives with a serializable `DocumentId`, distance metrics (`L2`, cosine, dot), and a parallel flat index implementing the `VectorIndex` trait.
   - `barq-core`: collection schema types, a `Catalog` for registering collections, and an in-memory `Collection` that wraps the flat index for basic insert/delete/search flows.

This skeleton focuses on the minimal single-node core (Phase 1) and is designed to be extended with ANN indexes, storage, clustering, and API layers outlined above.
