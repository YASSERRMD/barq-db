use barq_bench::dataset::{generate_dataset, DatasetConfig};
use barq_bench::ingest::{run_ingestion_benchmark, IngestionBenchmarkConfig};
use serde_json::Value;

#[test]
fn harness_smoke_test() {
    let dataset = generate_dataset(&DatasetConfig {
        seed: 99,
        count: 32,
        dimension: 16,
    })
    .expect("dataset should be generated");

    let result = run_ingestion_benchmark(
        &IngestionBenchmarkConfig {
            warmup_iterations: 1,
            measured_iterations: 2,
        },
        &dataset,
    );

    assert_eq!(result.benchmark, "ingestion");
    assert_eq!(result.record_count, 32);
    assert_eq!(result.total_vectors_written, 64);
    assert!(result.vectors_per_second.is_finite());
}

#[test]
fn output_format_is_valid_json() {
    let dataset = generate_dataset(&DatasetConfig {
        seed: 21,
        count: 4,
        dimension: 8,
    })
    .expect("dataset should be generated");

    let result = run_ingestion_benchmark(
        &IngestionBenchmarkConfig {
            warmup_iterations: 0,
            measured_iterations: 1,
        },
        &dataset,
    );

    let json = serde_json::to_string(&result).expect("result should serialize");
    let value: Value = serde_json::from_str(&json).expect("json should parse");

    for key in [
        "benchmark",
        "record_count",
        "total_vectors_written",
        "elapsed_millis",
        "vectors_per_second",
    ] {
        assert!(value.get(key).is_some(), "missing key: {key}");
    }
}
