use crate::dataset::VectorRecord;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Configuration for ingestion benchmark execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestionBenchmarkConfig {
    pub warmup_iterations: usize,
    pub measured_iterations: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestionBenchmarkResult {
    pub benchmark: String,
    pub record_count: usize,
    pub total_vectors_written: usize,
    pub elapsed_millis: u128,
    pub vectors_per_second: f64,
}

pub fn run_ingestion_benchmark(
    config: &IngestionBenchmarkConfig,
    dataset: &[VectorRecord],
) -> IngestionBenchmarkResult {
    for _ in 0..config.warmup_iterations {
        let mut checksum = 0.0f32;
        for record in dataset {
            checksum += record.values.iter().copied().sum::<f32>();
        }
        std::hint::black_box(checksum);
    }

    let start = Instant::now();
    let mut checksum = 0.0f32;
    for _ in 0..config.measured_iterations {
        for record in dataset {
            checksum += record.values.iter().copied().sum::<f32>();
        }
    }
    std::hint::black_box(checksum);
    let elapsed = start.elapsed();
    let total_vectors_written = dataset.len() * config.measured_iterations;
    let secs = elapsed.as_secs_f64().max(f64::EPSILON);

    IngestionBenchmarkResult {
        benchmark: "ingestion".to_string(),
        record_count: dataset.len(),
        total_vectors_written,
        elapsed_millis: elapsed.as_millis(),
        vectors_per_second: total_vectors_written as f64 / secs,
    }
}
