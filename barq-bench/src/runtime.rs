use std::process::Command;

use barq_core::{
    Collection, CollectionSchema, DistanceMetric, Document, DocumentId, FieldSchema, FieldType,
    IndexType, TenantId,
};

use crate::dataset::VectorRecord;
use crate::report::{build_memory_report, MemoryReport};

/// Errors raised by the live benchmark harness.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeBenchmarkError {
    #[error("dataset must contain at least one record")]
    EmptyDataset,

    #[error("queries must be greater than zero")]
    ZeroQueries,

    #[error("benchmark collection failed: {0}")]
    Catalog(#[from] barq_core::CatalogError),

    #[error("rss sampling failed: {0}")]
    MemorySampling(String),
}

pub fn benchmark_collection(
    name: &str,
    dimension: usize,
) -> Result<Collection, RuntimeBenchmarkError> {
    let schema = CollectionSchema {
        name: name.to_string(),
        fields: vec![FieldSchema {
            name: "vector".to_string(),
            field_type: FieldType::Vector {
                dimension,
                metric: DistanceMetric::Cosine,
                index: Some(IndexType::Flat),
            },
            required: true,
        }],
        bm25_config: None,
        tenant_id: TenantId::default(),
    };

    Ok(Collection::new(schema)?)
}

pub fn load_collection(
    name: &str,
    dataset: &[VectorRecord],
) -> Result<Collection, RuntimeBenchmarkError> {
    let first = dataset.first().ok_or(RuntimeBenchmarkError::EmptyDataset)?;
    let mut collection = benchmark_collection(name, first.values.len())?;

    for record in dataset {
        collection.insert(Document {
            id: DocumentId::U64(record.id as u64 + 1),
            vector: record.values.clone(),
            payload: None,
        })?;
    }

    Ok(collection)
}

pub fn current_rss_bytes() -> Result<u64, RuntimeBenchmarkError> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .map_err(|error| RuntimeBenchmarkError::MemorySampling(error.to_string()))?;

    if !output.status.success() {
        return Err(RuntimeBenchmarkError::MemorySampling(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }

    let rss_kb: u64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .map_err(|error: std::num::ParseIntError| {
            RuntimeBenchmarkError::MemorySampling(error.to_string())
        })?;

    Ok(rss_kb.saturating_mul(1024))
}

pub fn sample_memory_report<F, T>(f: F) -> Result<(T, MemoryReport), RuntimeBenchmarkError>
where
    F: FnOnce() -> Result<T, RuntimeBenchmarkError>,
{
    let before = current_rss_bytes()?;
    let value = f()?;
    let after = current_rss_bytes()?;
    Ok((value, build_memory_report(before, after, before.max(after))))
}
