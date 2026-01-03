# Phase 5: Benchmarking & Performance

## Overview
This phase focuses on creating comprehensive benchmarks to measure and optimize Barq DB's performance across vector search, text search, hybrid search, and storage tiering operations.

**Branch**: `phase-5-benchmarking`
**Priority**: Medium
**Dependencies**: `barq-core`, `barq-index`, `barq-storage`

---

## Task 5.1: Micro-Benchmarks for Index Operations

### Description
Create benchmarks for core index operations using the `criterion` crate.

### Benchmarks to Implement
- HNSW index construction (varying dimensions: 128, 384, 768, 1536)
- HNSW search (varying ef_search: 50, 100, 200)
- IVF index construction
- IVF search (varying nprobe)
- Flat index brute-force search
- Distance calculations (L2, Cosine, Dot) with SIMD

### Acceptance Criteria
- [ ] All benchmarks run with `cargo bench`
- [ ] Results exported to JSON for CI comparison
- [ ] Baseline established for future regression detection

---

## Task 5.2: End-to-End API Benchmarks

### Description
Benchmark full request/response cycle through the HTTP and gRPC APIs.

### Benchmarks to Implement
- Single search latency (p50, p95, p99)
- Batch search throughput (queries/sec)
- Insert throughput (docs/sec)
- Hybrid search latency
- Concurrent search (varying thread count)

### Acceptance Criteria
- [ ] Benchmarks simulate realistic workloads
- [ ] Results include latency histograms
- [ ] Can run against local or remote Barq instance

---

## Task 5.3: Storage Tiering Benchmarks

### Description
Measure the performance impact of storage tiering operations.

### Benchmarks to Implement
- Segment upload to S3/GCS (LocalObjectStore mock)
- Segment download/hydration time
- Policy enforcement overhead
- Cold data access latency vs hot data

### Acceptance Criteria
- [ ] Benchmarks use mock object store (LocalObjectStore)
- [ ] Measure both latency and throughput
- [ ] Document expected performance characteristics

---

## Task 5.4: Benchmark Documentation & CI Integration

### Description
Document benchmark results and integrate into CI for regression detection.

### Implementation
- Add `benchmarks/` directory with scripts and baselines
- Create GitHub Action for nightly benchmark runs
- Generate performance reports

### Acceptance Criteria
- [ ] README with instructions for running benchmarks
- [ ] CI workflow for automated benchmarking
- [ ] Historical comparison available

---

## Future Phase
- Memory profiling with heaptrack/valgrind
- Flamegraph generation for hot path analysis
- Load testing with k6 or wrk
