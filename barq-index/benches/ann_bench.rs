use barq_index::{build_index, DistanceMetric, DocumentId, IndexConfig, IndexType, HnswParams};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn random_vector(rng: &mut StdRng, dimension: usize) -> Vec<f32> {
    (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn build_dataset(count: usize, dimension: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| random_vector(&mut rng, dimension))
        .collect()
}

fn build_indexed_dataset(index_type: IndexType, dimension: usize, dataset: &[(DocumentId, Vec<f32>)]) -> Box<dyn barq_index::VectorIndex> {
    let mut index = build_index(IndexConfig::new(DistanceMetric::L2, dimension, index_type));
    for (id, vector) in dataset.iter().cloned() {
        index.insert(id, vector).unwrap();
    }
    index
}

// ============================================================================
// Benchmark: Index Construction
// ============================================================================
fn bench_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_build");
    group.sample_size(20); // Reduce sample size for expensive operations

    for dimension in [128, 384, 768].iter() {
        let dataset: Vec<(DocumentId, Vec<f32>)> = build_dataset(5_000, *dimension, 42)
            .into_iter()
            .enumerate()
            .map(|(idx, vector)| (DocumentId::U64(idx as u64 + 1), vector))
            .collect();

        group.throughput(Throughput::Elements(dataset.len() as u64));

        group.bench_function(BenchmarkId::new("flat", dimension), |b| {
            b.iter(|| {
                let mut index = build_index(IndexConfig::new(DistanceMetric::L2, *dimension, IndexType::Flat));
                for (id, vector) in dataset.iter().cloned() {
                    index.insert(id, vector).unwrap();
                }
            })
        });

        group.bench_function(BenchmarkId::new("hnsw_m16", dimension), |b| {
            b.iter(|| {
                let params = HnswParams { m: 16, ef_construction: 200, ef_search: 100 };
                let mut index = build_index(IndexConfig::new(DistanceMetric::L2, *dimension, IndexType::Hnsw(params)));
                for (id, vector) in dataset.iter().cloned() {
                    index.insert(id, vector).unwrap();
                }
            })
        });

        group.bench_function(BenchmarkId::new("hnsw_m32", dimension), |b| {
            b.iter(|| {
                let params = HnswParams { m: 32, ef_construction: 200, ef_search: 100 };
                let mut index = build_index(IndexConfig::new(DistanceMetric::L2, *dimension, IndexType::Hnsw(params)));
                for (id, vector) in dataset.iter().cloned() {
                    index.insert(id, vector).unwrap();
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark: Search Operations
// ============================================================================
fn bench_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("search");
    group.sample_size(100);

    let dimension = 384;
    let dataset: Vec<(DocumentId, Vec<f32>)> = build_dataset(10_000, dimension, 42)
        .into_iter()
        .enumerate()
        .map(|(idx, vector)| (DocumentId::U64(idx as u64 + 1), vector))
        .collect();

    // Pre-build indexes
    let flat_index = build_indexed_dataset(IndexType::Flat, dimension, &dataset);
    let hnsw_index = build_indexed_dataset(IndexType::Hnsw(HnswParams { m: 16, ef_construction: 200, ef_search: 100 }), dimension, &dataset);

    // Generate query vectors
    let queries: Vec<Vec<f32>> = build_dataset(100, dimension, 123);

    group.throughput(Throughput::Elements(queries.len() as u64));

    for top_k in [10, 50, 100].iter() {
        group.bench_function(BenchmarkId::new("flat", top_k), |b| {
            b.iter(|| {
                for query in &queries {
                    let _ = flat_index.search(query, *top_k);
                }
            })
        });

        group.bench_function(BenchmarkId::new("hnsw", top_k), |b| {
            b.iter(|| {
                for query in &queries {
                    let _ = hnsw_index.search(query, *top_k);
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark: Distance Calculations
// ============================================================================
fn bench_distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance");
    
    for dimension in [128, 384, 768, 1536].iter() {
        let mut rng = StdRng::seed_from_u64(42);
        let v1: Vec<f32> = random_vector(&mut rng, *dimension);
        let v2: Vec<f32> = random_vector(&mut rng, *dimension);

        group.bench_function(BenchmarkId::new("l2", dimension), |b| {
            b.iter(|| {
                let mut sum = 0.0f32;
                for (a, b) in v1.iter().zip(v2.iter()) {
                    let diff = a - b;
                    sum += diff * diff;
                }
                sum.sqrt()
            })
        });

        group.bench_function(BenchmarkId::new("cosine", dimension), |b| {
            b.iter(|| {
                let mut dot = 0.0f32;
                let mut norm1 = 0.0f32;
                let mut norm2 = 0.0f32;
                for (a, b) in v1.iter().zip(v2.iter()) {
                    dot += a * b;
                    norm1 += a * a;
                    norm2 += b * b;
                }
                1.0 - (dot / (norm1.sqrt() * norm2.sqrt()))
            })
        });

        group.bench_function(BenchmarkId::new("dot", dimension), |b| {
            b.iter(|| {
                let mut dot = 0.0f32;
                for (a, b) in v1.iter().zip(v2.iter()) {
                    dot += a * b;
                }
                -dot // Negative for similarity ranking
            })
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark: Batch Insert Operations
// ============================================================================
fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_insert");
    group.sample_size(10);

    let dimension = 384;
    
    for batch_size in [100, 500, 1000].iter() {
        let dataset: Vec<(DocumentId, Vec<f32>)> = build_dataset(*batch_size, dimension, 42)
            .into_iter()
            .enumerate()
            .map(|(idx, vector)| (DocumentId::U64(idx as u64 + 1), vector))
            .collect();

        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_function(BenchmarkId::new("hnsw", batch_size), |b| {
            b.iter(|| {
                let mut index = build_index(IndexConfig::new(
                    DistanceMetric::Cosine,
                    dimension,
                    IndexType::Hnsw(HnswParams::default()),
                ));
                for (id, vector) in dataset.iter().cloned() {
                    index.insert(id, vector).unwrap();
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark: Memory Usage (informational - tracks allocations per op)
// ============================================================================
fn bench_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling");
    group.sample_size(10);

    let dimension = 256;

    for size in [1_000, 5_000, 10_000].iter() {
        let dataset: Vec<(DocumentId, Vec<f32>)> = build_dataset(*size, dimension, 42)
            .into_iter()
            .enumerate()
            .map(|(idx, vector)| (DocumentId::U64(idx as u64 + 1), vector))
            .collect();

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_function(BenchmarkId::new("hnsw_build", size), |b| {
            b.iter(|| {
                build_indexed_dataset(IndexType::Hnsw(HnswParams::default()), dimension, &dataset)
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_index_build,
    bench_search,
    bench_distance,
    bench_batch_insert,
    bench_scaling,
);
criterion_main!(benches);

