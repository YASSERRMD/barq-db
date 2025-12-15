use barq_index::{build_index, DistanceMetric, DocumentId, IndexConfig, IndexType};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn random_vector(rng: &mut StdRng, dimension: usize) -> Vec<f32> {
    (0..dimension).map(|_| rng.gen_range(0.0..1.0)).collect()
}

fn build_dataset(count: usize, dimension: usize) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count).map(|_| random_vector(&mut rng, dimension)).collect()
}

fn load_index(index: IndexType, dimension: usize, dataset: &[(DocumentId, Vec<f32>)]) {
    let mut index = build_index(IndexConfig::new(DistanceMetric::L2, dimension, index));
    for (id, vector) in dataset.iter().cloned() {
        index.insert(id, vector).unwrap();
    }
}

fn bench_ann(c: &mut Criterion) {
    let dimension = 64;
    let dataset: Vec<(DocumentId, Vec<f32>)> = build_dataset(2_000, dimension)
        .into_iter()
        .enumerate()
        .map(|(idx, vector)| (DocumentId::U64(idx as u64 + 1), vector))
        .collect();

    let mut group = c.benchmark_group("ann_search_build");

    group.bench_function(BenchmarkId::new("flat", dataset.len()), |b| {
        b.iter(|| load_index(IndexType::Flat, dimension, &dataset))
    });

    group.bench_function(BenchmarkId::new("hnsw", dataset.len()), |b| {
        b.iter(|| load_index(IndexType::Hnsw(Default::default()), dimension, &dataset))
    });

    group.bench_function(BenchmarkId::new("ivf", dataset.len()), |b| {
        b.iter(|| load_index(IndexType::Ivf(Default::default()), dimension, &dataset))
    });

    group.finish();
}

criterion_group!(benches, bench_ann);
criterion_main!(benches);
