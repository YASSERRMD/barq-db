# Phase 2: Client SDK Updates (Rust)

## Overview
This phase focuses on updating the Rust SDK (`barq-sdk-rust`) to support the newly implemented batch search capabilities exposed by the Barq DB server. This ensures developers can leverage the performance improvements from Phase 1.

**Branch**: `phase-2-client-sdk`
**Priority**: High
**Dependencies**: Phase 1 (completed)

---

## Task 2.1: Rust SDK Batch Search Support

### Description
Implement batch search methods in both the HTTP (`BarqClient`) and gRPC (`BarqGrpcClient`) clients within the Rust SDK.

### Implementation Details

#### Files to Modify
- `barq-sdk-rust/src/lib.rs` (or relevant module files)
- `barq-sdk-rust/README.md`

#### Code Changes

1.  **New Structs**:
    ```rust
    #[derive(Debug, Serialize, Deserialize)]
    pub struct SearchQuery {
        pub vector: Vec<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub filter: Option<Filter>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct BatchSearchRequest {
        pub queries: Vec<SearchQuery>,
        pub top_k: usize,
    }
    
    // ... Response structs
    ```

2.  **HTTP Client (`Collection` impl)**:
    ```rust
    impl Collection {
        pub async fn batch_search(
            &self,
            queries: Vec<SearchQuery>,
            top_k: usize,
        ) -> Result<Vec<Vec<SearchResult>>, BarqError>;
    }
    ```

3.  **gRPC Client (`BarqGrpcClient` impl)**:
    ```rust
    impl BarqGrpcClient {
        pub async fn batch_search(
            &mut self,
            collection: &str,
            queries: Vec<SearchQuery>,
            top_k: usize,
        ) -> Result<Vec<Vec<SearchResult>>, BarqError>;
    }
    ```

### Acceptance Criteria
- [ ] `batch_search` method available on HTTP `Collection` struct.
- [ ] `batch_search` method available on `BarqGrpcClient`.
- [ ] Methods correctly serialize request/response to match server API.
- [ ] README updated with batch search examples.

### Test Plan
- Create an integration test in `barq-sdk-rust/tests` (if exists) or within `lib.rs` tests that mocks the server or assumes a running instance (if applicable).
- Verify basic compilation and usage patterns.

---

## Task 2.2: SDK Benchmarking

### Description
Create a benchmark suite using `criterion` (or simple timing) to verify the performance difference between sequential `search` vs `batch_search` using the updated SDK.

### Implementation Details
- Create `benches/sdk_benchmark.rs`.
- scenarios:
    - 100 sequential searches.
    - 1 batch search of 100 queries.
    - compare latencies.

### Acceptance Criteria
- [ ] Benchmark script runnable via `cargo bench`.
- [ ] Results show batch search throughput improvement.
