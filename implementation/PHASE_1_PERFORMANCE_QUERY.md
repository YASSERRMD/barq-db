# Phase 1: Performance & Query Optimization

## Overview
This phase focuses on improving query performance through filtered vector search optimization, batch processing, and SIMD enhancements.

**Branch**: `phase-1-performance-query`  
**Duration**: 1-2 weeks  
**Priority**: High

---

## Task 1.1: Pre/Post Filter Strategy for Vector Search

### Description
Implement intelligent filtering strategies that choose between pre-filtering (apply filter before ANN) and post-filtering (apply filter after ANN) based on selectivity.

### Implementation Details

#### Files to Create/Modify
- `barq-index/src/filtered_search.rs` (NEW)
- `barq-index/src/lib.rs` (MODIFY)
- `barq-core/src/lib.rs` (MODIFY)

#### Code Structure
```rust
// barq-index/src/filtered_search.rs

/// Strategy for applying filters in vector search
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterStrategy {
    /// Apply filter before ANN search - best for highly selective filters
    PreFilter,
    /// Apply filter after ANN search - best for low selectivity
    PostFilter,
    /// Automatically choose based on selectivity estimation
    Auto { selectivity_threshold: f32 },
}

/// Selectivity estimator for filter conditions
pub struct SelectivityEstimator {
    field_cardinalities: HashMap<String, usize>,
    total_documents: usize,
}

impl SelectivityEstimator {
    pub fn estimate(&self, filter: &Filter) -> f32;
}

/// Filtered vector search executor
pub struct FilteredVectorSearch<'a> {
    index: &'a VectorIndex,
    filter: Option<Filter>,
    strategy: FilterStrategy,
    estimator: SelectivityEstimator,
}

impl<'a> FilteredVectorSearch<'a> {
    pub fn new(index: &'a VectorIndex) -> Self;
    pub fn with_filter(self, filter: Filter) -> Self;
    pub fn with_strategy(self, strategy: FilterStrategy) -> Self;
    
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        candidate_multiplier: usize,
    ) -> Result<Vec<SearchResult>, VectorIndexError>;
    
    fn execute_pre_filter(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorIndexError>;
    fn execute_post_filter(&self, query: &[f32], top_k: usize, multiplier: usize) -> Result<Vec<SearchResult>, VectorIndexError>;
    fn choose_strategy(&self) -> FilterStrategy;
}
```

### Acceptance Criteria
- [ ] Pre-filter correctly excludes documents before distance calculation
- [ ] Post-filter correctly excludes documents after distance calculation
- [ ] Auto strategy chooses correctly based on 30% selectivity threshold
- [ ] Performance improvement of 2-5x for selective filters

### Test Suite

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pre_filter_high_selectivity() {
        // Filter that matches <10% of documents
        let index = create_test_index(10000, 128);
        let filter = Filter::Eq { field: "category".into(), value: "rare".into() };
        
        let search = FilteredVectorSearch::new(&index)
            .with_filter(filter)
            .with_strategy(FilterStrategy::PreFilter);
        
        let results = search.search(&query_vec, 10, 1).unwrap();
        assert!(results.iter().all(|r| r.payload["category"] == "rare"));
    }

    #[test]
    fn test_post_filter_low_selectivity() {
        // Filter that matches >50% of documents
        let index = create_test_index(10000, 128);
        let filter = Filter::Eq { field: "active".into(), value: true.into() };
        
        let search = FilteredVectorSearch::new(&index)
            .with_filter(filter)
            .with_strategy(FilterStrategy::PostFilter);
        
        let results = search.search(&query_vec, 10, 3).unwrap();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_auto_strategy_selection() {
        let estimator = SelectivityEstimator::new(cardinalities, 10000);
        
        // High selectivity filter
        let selective = Filter::Eq { field: "unique_id".into(), value: "123".into() };
        assert!(estimator.estimate(&selective) < 0.01);
        
        // Low selectivity filter
        let broad = Filter::Eq { field: "status".into(), value: "active".into() };
        assert!(estimator.estimate(&broad) > 0.5);
    }

    #[test]
    fn test_selectivity_estimator_compound_filters() {
        let estimator = SelectivityEstimator::new(cardinalities, 10000);
        
        let and_filter = Filter::And {
            filters: vec![
                Filter::Eq { field: "a".into(), value: "x".into() },
                Filter::Eq { field: "b".into(), value: "y".into() },
            ],
        };
        
        // AND should multiply selectivities
        let selectivity = estimator.estimate(&and_filter);
        assert!(selectivity < 0.1);
    }

    #[test]
    fn test_filtered_search_preserves_order() {
        let index = create_test_index(1000, 128);
        let filter = Filter::Gte { field: "price".into(), value: 100.0.into() };
        
        let results = FilteredVectorSearch::new(&index)
            .with_filter(filter)
            .search(&query_vec, 10, 3)
            .unwrap();
        
        // Results should be ordered by score descending
        for i in 1..results.len() {
            assert!(results[i-1].score >= results[i].score);
        }
    }

    #[test]
    fn test_empty_filter_returns_all() {
        let index = create_test_index(100, 128);
        
        let with_filter = FilteredVectorSearch::new(&index)
            .search(&query_vec, 10, 1)
            .unwrap();
        
        let without = index.search(&query_vec, 10).unwrap();
        
        assert_eq!(with_filter, without);
    }
}
```

### Commit Message
```
feat(index): implement pre/post filter strategy for vector search

- Add FilterStrategy enum with PreFilter, PostFilter, Auto variants
- Implement SelectivityEstimator for filter cardinality estimation
- Add FilteredVectorSearch executor with strategy selection
- Include comprehensive test suite for all strategies
```

---

## Task 1.2: Batch Vector Search with Prefetching

### Description
Implement batch search operations that process multiple queries in parallel with memory prefetching for improved cache utilization.

### Implementation Details

#### Files to Create/Modify
- `barq-index/src/batch.rs` (NEW)
- `barq-index/src/lib.rs` (MODIFY)
- `barq-api/src/lib.rs` (MODIFY)

#### Code Structure
```rust
// barq-index/src/batch.rs

/// Configuration for batch search operations
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of queries to process in parallel
    pub max_parallelism: usize,
    /// Enable memory prefetching hints
    pub enable_prefetch: bool,
    /// Chunk size for processing
    pub chunk_size: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_parallelism: num_cpus::get(),
            enable_prefetch: true,
            chunk_size: 32,
        }
    }
}

/// Batch search executor for multiple queries
pub struct BatchSearch<'a> {
    index: &'a VectorIndex,
    config: BatchConfig,
}

impl<'a> BatchSearch<'a> {
    pub fn new(index: &'a VectorIndex) -> Self;
    pub fn with_config(self, config: BatchConfig) -> Self;
    
    /// Execute batch search with parallel processing
    pub fn search(
        &self,
        queries: &[Vec<f32>],
        top_k: usize,
    ) -> Result<Vec<Vec<SearchResult>>, VectorIndexError>;
    
    /// Execute batch search with per-query filters
    pub fn search_filtered(
        &self,
        queries: &[(Vec<f32>, Option<Filter>)],
        top_k: usize,
    ) -> Result<Vec<Vec<SearchResult>>, VectorIndexError>;
    
    /// Prefetch vectors for upcoming queries
    fn prefetch_batch(&self, query_indices: &[usize], vectors: &[Vec<f32>]);
}

/// Parallel distance computation using rayon
pub fn compute_distances_parallel(
    query: &[f32],
    vectors: &[Vec<f32>],
    metric: DistanceMetric,
) -> Vec<f32>;
```

### Acceptance Criteria
- [ ] Batch search processes 100 queries faster than 100 sequential searches
- [ ] Prefetching improves cache hit rate by >20%
- [ ] Thread pool is properly utilized (no thread starvation)
- [ ] Memory usage is bounded during batch processing

### Test Suite

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_batch_search_correctness() {
        let index = create_test_index(10000, 128);
        let queries: Vec<Vec<f32>> = (0..10).map(|_| random_vector(128)).collect();
        
        let batch_results = BatchSearch::new(&index)
            .search(&queries, 10)
            .unwrap();
        
        // Compare with sequential results
        for (i, query) in queries.iter().enumerate() {
            let sequential = index.search(query, 10).unwrap();
            assert_eq!(batch_results[i], sequential);
        }
    }

    #[test]
    fn test_batch_search_performance() {
        let index = create_test_index(100000, 128);
        let queries: Vec<Vec<f32>> = (0..100).map(|_| random_vector(128)).collect();
        
        // Sequential timing
        let seq_start = Instant::now();
        for query in &queries {
            index.search(query, 10).unwrap();
        }
        let seq_duration = seq_start.elapsed();
        
        // Batch timing
        let batch_start = Instant::now();
        BatchSearch::new(&index).search(&queries, 10).unwrap();
        let batch_duration = batch_start.elapsed();
        
        // Batch should be at least 2x faster on multi-core
        assert!(batch_duration < seq_duration / 2);
    }

    #[test]
    fn test_batch_with_filters() {
        let index = create_test_index(10000, 128);
        let queries: Vec<(Vec<f32>, Option<Filter>)> = vec![
            (random_vector(128), Some(Filter::Eq { field: "a".into(), value: 1.into() })),
            (random_vector(128), None),
            (random_vector(128), Some(Filter::Gte { field: "b".into(), value: 5.into() })),
        ];
        
        let results = BatchSearch::new(&index)
            .search_filtered(&queries, 10)
            .unwrap();
        
        assert_eq!(results.len(), 3);
        // First result should only contain documents with a=1
        assert!(results[0].iter().all(|r| r.payload["a"] == 1));
    }

    #[test]
    fn test_empty_batch() {
        let index = create_test_index(100, 128);
        let results = BatchSearch::new(&index)
            .search(&[], 10)
            .unwrap();
        
        assert!(results.is_empty());
    }

    #[test]
    fn test_single_query_batch() {
        let index = create_test_index(100, 128);
        let query = random_vector(128);
        
        let batch_result = BatchSearch::new(&index)
            .search(&[query.clone()], 10)
            .unwrap();
        
        let single_result = index.search(&query, 10).unwrap();
        
        assert_eq!(batch_result[0], single_result);
    }

    #[test]
    fn test_config_respects_parallelism() {
        let index = create_test_index(1000, 128);
        let queries: Vec<Vec<f32>> = (0..50).map(|_| random_vector(128)).collect();
        
        let config = BatchConfig {
            max_parallelism: 2,
            enable_prefetch: false,
            chunk_size: 10,
        };
        
        let results = BatchSearch::new(&index)
            .with_config(config)
            .search(&queries, 5)
            .unwrap();
        
        assert_eq!(results.len(), 50);
    }
}
```

### Commit Message
```
feat(index): add batch vector search with parallel processing

- Implement BatchSearch executor for multi-query operations
- Add configurable parallelism and prefetching
- Support per-query filters in batch mode
- Performance tests verify 2x+ speedup over sequential
```

---

## Task 1.3: SIMD Distance Computation Enhancements

### Description
Enhance SIMD distance calculations with AVX-512 support, better fallback paths, and auto-detection of optimal instruction set.

### Implementation Details

#### Files to Modify
- `barq-index/src/distance.rs` (MODIFY)

#### Code Structure
```rust
// barq-index/src/distance.rs

/// Detected SIMD capability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdCapability {
    Avx512,
    Avx2,
    Sse4,
    Neon,
    Scalar,
}

/// Detect the best SIMD capability at runtime
pub fn detect_simd_capability() -> SimdCapability {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return SimdCapability::Avx512;
        }
        if is_x86_feature_detected!("avx2") {
            return SimdCapability::Avx2;
        }
        if is_x86_feature_detected!("sse4.1") {
            return SimdCapability::Sse4;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        return SimdCapability::Neon;
    }
    SimdCapability::Scalar
}

/// SIMD-optimized L2 distance
#[inline]
pub fn l2_distance_simd(a: &[f32], b: &[f32]) -> f32 {
    match detect_simd_capability() {
        SimdCapability::Avx512 => l2_avx512(a, b),
        SimdCapability::Avx2 => l2_avx2(a, b),
        SimdCapability::Sse4 => l2_sse4(a, b),
        SimdCapability::Neon => l2_neon(a, b),
        SimdCapability::Scalar => l2_scalar(a, b),
    }
}

/// SIMD-optimized cosine similarity
#[inline]
pub fn cosine_similarity_simd(a: &[f32], b: &[f32]) -> f32;

/// SIMD-optimized dot product
#[inline]
pub fn dot_product_simd(a: &[f32], b: &[f32]) -> f32;

/// Batch distance computation (single query vs many vectors)
pub fn batch_distances(
    query: &[f32],
    vectors: &[&[f32]],
    metric: DistanceMetric,
) -> Vec<f32>;

// Architecture-specific implementations
#[cfg(target_arch = "x86_64")]
mod avx512 { ... }

#[cfg(target_arch = "x86_64")]
mod avx2 { ... }

#[cfg(target_arch = "aarch64")]
mod neon { ... }

mod scalar { ... }
```

### Acceptance Criteria
- [ ] AVX-512 path is used when available
- [ ] Fallback paths work correctly on all architectures
- [ ] Batch distance is 4x faster than individual calls
- [ ] All distance metrics produce identical results across SIMD paths

### Test Suite

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance_correctness() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 6.0, 7.0, 8.0];
        
        let scalar = l2_scalar(&a, &b);
        let simd = l2_distance_simd(&a, &b);
        
        assert!((scalar - simd).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_correctness() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        
        let similarity = cosine_similarity_simd(&a, &b);
        assert!((similarity - 0.0).abs() < 1e-6); // Orthogonal
        
        let c = vec![1.0, 0.0, 0.0];
        let same = cosine_similarity_simd(&a, &c);
        assert!((same - 1.0).abs() < 1e-6); // Same direction
    }

    #[test]
    fn test_dot_product_correctness() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        
        let expected = 1.0*4.0 + 2.0*5.0 + 3.0*6.0;
        let actual = dot_product_simd(&a, &b);
        
        assert!((expected - actual).abs() < 1e-6);
    }

    #[test]
    fn test_large_vector_distances() {
        let dim = 1536; // OpenAI embedding dimension
        let a: Vec<f32> = (0..dim).map(|i| i as f32 / dim as f32).collect();
        let b: Vec<f32> = (0..dim).map(|i| (dim - i) as f32 / dim as f32).collect();
        
        let scalar = l2_scalar(&a, &b);
        let simd = l2_distance_simd(&a, &b);
        
        assert!((scalar - simd).abs() < 1e-4);
    }

    #[test]
    fn test_batch_distances_performance() {
        let query: Vec<f32> = (0..128).map(|_| rand::random()).collect();
        let vectors: Vec<Vec<f32>> = (0..10000)
            .map(|_| (0..128).map(|_| rand::random()).collect())
            .collect();
        let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();
        
        let start = Instant::now();
        let batch = batch_distances(&query, &refs, DistanceMetric::L2);
        let batch_time = start.elapsed();
        
        let start = Instant::now();
        let individual: Vec<f32> = vectors.iter()
            .map(|v| l2_distance_simd(&query, v))
            .collect();
        let individual_time = start.elapsed();
        
        assert_eq!(batch.len(), individual.len());
        assert!(batch_time < individual_time);
    }

    #[test]
    fn test_simd_detection() {
        let capability = detect_simd_capability();
        // Should detect something
        assert!(matches!(
            capability,
            SimdCapability::Avx512 | SimdCapability::Avx2 | 
            SimdCapability::Sse4 | SimdCapability::Neon | SimdCapability::Scalar
        ));
    }

    #[test]
    fn test_zero_vectors() {
        let zero = vec![0.0; 128];
        let other: Vec<f32> = (0..128).map(|i| i as f32).collect();
        
        let l2 = l2_distance_simd(&zero, &other);
        assert!(l2 > 0.0);
        
        let cosine = cosine_similarity_simd(&zero, &other);
        assert!(cosine.is_nan() || cosine == 0.0); // Handle zero norm
    }

    #[test]
    fn test_normalized_vectors() {
        let a: Vec<f32> = normalize(&random_vector(128));
        let b: Vec<f32> = normalize(&random_vector(128));
        
        let cosine = cosine_similarity_simd(&a, &b);
        assert!(cosine >= -1.0 && cosine <= 1.0);
    }
}
```

### Commit Message
```
perf(index): enhance SIMD distance computations with AVX-512

- Add runtime SIMD capability detection
- Implement AVX-512, AVX2, SSE4, NEON paths
- Add batch distance computation for better cache usage
- Ensure correctness across all architectures
```

---

## Task 1.4: API Batch Endpoints

### Description
Expose batch search capabilities through HTTP and gRPC APIs.

### Implementation Details

#### Files to Modify
- `barq-api/src/lib.rs` (MODIFY)
- `barq-proto/src/barq.proto` (MODIFY)
- `barq-api/src/grpc/mod.rs` (MODIFY)

#### API Specification

**HTTP Endpoint**
```
POST /collections/{name}/batch_search
Content-Type: application/json

{
  "queries": [
    {
      "vector": [0.1, 0.2, ...],
      "filter": { "op": "eq", "field": "category", "value": "electronics" }
    },
    {
      "vector": [0.3, 0.4, ...],
      "filter": null
    }
  ],
  "top_k": 10,
  "include_payload": true
}

Response:
{
  "results": [
    {
      "hits": [
        { "id": "1", "score": 0.95, "payload": {...} },
        ...
      ],
      "took_ms": 5
    },
    ...
  ],
  "total_took_ms": 12
}
```

**gRPC**
```protobuf
message BatchSearchRequest {
  string collection = 1;
  repeated SearchQuery queries = 2;
  uint32 top_k = 3;
  bool include_payload = 4;
}

message BatchSearchResponse {
  repeated QueryResults results = 1;
  uint64 total_took_ns = 2;
}

message QueryResults {
  repeated SearchResult hits = 1;
  uint64 took_ns = 2;
}

service BarqService {
  rpc BatchSearch(BatchSearchRequest) returns (BatchSearchResponse);
}
```

### Acceptance Criteria
- [ ] HTTP batch endpoint accepts up to 100 queries
- [ ] gRPC batch endpoint handles streaming for large batches
- [ ] Response includes per-query timing
- [ ] Errors in individual queries don't fail the entire batch

### Test Suite

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_http_batch_search() {
        let app = create_test_app().await;
        
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/test/batch_search")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{
                        "queries": [
                            {"vector": [0.1, 0.2, 0.3]},
                            {"vector": [0.4, 0.5, 0.6]}
                        ],
                        "top_k": 5
                    }"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
        
        let body: BatchSearchResponse = serde_json::from_slice(
            &hyper::body::to_bytes(response.into_body()).await.unwrap()
        ).unwrap();
        
        assert_eq!(body.results.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_with_partial_filters() {
        let app = create_test_app().await;
        
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/test/batch_search")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{
                        "queries": [
                            {"vector": [0.1, 0.2, 0.3], "filter": {"op": "eq", "field": "a", "value": 1}},
                            {"vector": [0.4, 0.5, 0.6]}
                        ],
                        "top_k": 5
                    }"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_batch_max_queries_limit() {
        let app = create_test_app().await;
        
        let too_many_queries: Vec<_> = (0..101)
            .map(|_| json!({"vector": [0.1, 0.2, 0.3]}))
            .collect();
        
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/collections/test/batch_search")
                    .header("Content-Type", "application/json")
                    .body(Body::from(json!({
                        "queries": too_many_queries,
                        "top_k": 5
                    }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_grpc_batch_search() {
        let mut client = create_grpc_client().await;
        
        let request = BatchSearchRequest {
            collection: "test".into(),
            queries: vec![
                SearchQuery { vector: vec![0.1, 0.2, 0.3], filter: None },
                SearchQuery { vector: vec![0.4, 0.5, 0.6], filter: None },
            ],
            top_k: 5,
            include_payload: true,
        };
        
        let response = client.batch_search(request).await.unwrap();
        
        assert_eq!(response.results.len(), 2);
    }
}
```

### Commit Message
```
feat(api): add batch search endpoints for HTTP and gRPC

- Implement POST /collections/{name}/batch_search
- Add BatchSearch gRPC method to BarqService
- Support per-query filters and timing metrics
- Enforce maximum 100 queries per batch
```

---

## Phase Completion Checklist

- [ ] All tasks implemented and tested
- [ ] Integration tests pass
- [ ] Documentation updated
- [ ] Performance benchmarks show improvement
- [ ] Code review completed
- [ ] Merged to main branch

## Performance Benchmarks

Run the following benchmarks before and after:

```bash
# Single query latency
cargo bench --bench search_latency

# Batch throughput
cargo bench --bench batch_throughput

# Filter performance
cargo bench --bench filtered_search

# SIMD speedup
cargo bench --bench distance_simd
```

Expected improvements:
- Filtered search: 2-5x faster with pre-filtering
- Batch search: 3-5x faster than sequential
- SIMD: 10-20% improvement with AVX-512
