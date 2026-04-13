use crate::runtime::{benchmark_collection, RuntimeBenchmarkError};
use crate::dataset::VectorRecord;
use barq_core::{Document, DocumentId};
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
) -> Result<IngestionBenchmarkResult, RuntimeBenchmarkError> {
    let first = dataset.first().ok_or(RuntimeBenchmarkError::EmptyDataset)?;
    for _ in 0..config.warmup_iterations {
        let mut collection = benchmark_collection("bench_ingest_warmup", first.values.len())?;
        for record in dataset {
            collection.insert(Document {
                id: DocumentId::U64(record.id as u64 + 1),
                vector: record.values.clone(),
                payload: None,
            })?;
        }
        std::hint::black_box(collection.document_count());
    }

    let start = Instant::now();
    for _ in 0..config.measured_iterations {
        let mut collection = benchmark_collection("bench_ingest_measured", first.values.len())?;
        for record in dataset {
            collection.insert(Document {
                id: DocumentId::U64(record.id as u64 + 1),
                vector: record.values.clone(),
                payload: None,
            })?;
        }
        std::hint::black_box(collection.document_count());
    }
    let elapsed = start.elapsed();
    let total_vectors_written = dataset.len() * config.measured_iterations;
    let secs = elapsed.as_secs_f64().max(f64::EPSILON);

    Ok(IngestionBenchmarkResult {
        benchmark: "ingestion".to_string(),
        record_count: dataset.len(),
        total_vectors_written,
        elapsed_millis: elapsed.as_millis(),
        vectors_per_second: total_vectors_written as f64 / secs,
    })
}
