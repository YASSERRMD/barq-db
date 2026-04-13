use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

/// A deterministic benchmark vector record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorRecord {
    pub id: usize,
    pub values: Vec<f32>,
}

/// Configuration for deterministic dataset generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetConfig {
    pub seed: u64,
    pub count: usize,
    pub dimension: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DatasetError {
    #[error("count must be greater than zero")]
    ZeroCount,
    #[error("dimension must be greater than zero")]
    ZeroDimension,
}

pub fn generate_dataset(config: &DatasetConfig) -> Result<Vec<VectorRecord>, DatasetError> {
    if config.count == 0 {
        return Err(DatasetError::ZeroCount);
    }
    if config.dimension == 0 {
        return Err(DatasetError::ZeroDimension);
    }

    let mut rng = ChaCha8Rng::seed_from_u64(config.seed);
    let mut records = Vec::with_capacity(config.count);

    for id in 0..config.count {
        let mut values = Vec::with_capacity(config.dimension);
        for _ in 0..config.dimension {
            values.push(rng.random_range(-1.0..1.0));
        }
        records.push(VectorRecord { id, values });
    }

    Ok(records)
}
