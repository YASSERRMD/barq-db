use barq_bench::dataset::{generate_dataset, DatasetConfig};
use barq_bench::search::{calculate_search_stats, percentile, run_search_benchmark, SearchBenchmarkConfig};
use std::time::Duration;

#[test]
fn stats_calculation_produces_expected_fields() {
    let latencies = vec![
        Duration::from_millis(10),
        Duration::from_millis(20),
        Duration::from_millis(30),
        Duration::from_millis(40),
    ];

    let stats = calculate_search_stats(&latencies, Duration::from_secs(2));

    assert_eq!(stats.samples, 4);
    assert_eq!(stats.p50_millis, 25.0);
    assert!((stats.p95_millis - 38.5).abs() < f64::EPSILON);
    assert!((stats.p99_millis - 39.7).abs() < 1e-9);
    assert_eq!(stats.qps, 2.0);
}

#[test]
fn percentile_known_samples_are_correct() {
    let samples = vec![5.0, 10.0, 15.0, 20.0, 25.0];

    assert_eq!(percentile(&samples, 0.0), 5.0);
    assert_eq!(percentile(&samples, 50.0), 15.0);
    assert_eq!(percentile(&samples, 95.0), 24.0);
    assert_eq!(percentile(&samples, 99.0), 24.8);
    assert_eq!(percentile(&samples, 100.0), 25.0);
}

#[test]
fn live_search_benchmark_executes_queries() {
    let dataset = generate_dataset(&DatasetConfig {
        seed: 11,
        count: 32,
        dimension: 8,
    })
    .expect("dataset should be generated");

    let stats = run_search_benchmark(&SearchBenchmarkConfig { queries: 12 }, &dataset)
        .expect("search benchmark should succeed");

    assert_eq!(stats.samples, 12);
    assert!(stats.p50_millis.is_finite());
    assert!(stats.p95_millis.is_finite());
    assert!(stats.p99_millis.is_finite());
    assert!(stats.qps.is_finite());
}
