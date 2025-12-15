use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum VectorIndexError {
    #[error("vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("search requested top_k={top_k}, but a positive value is required")]
    InvalidTopK { top_k: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DocumentId {
    U64(u64),
    Str(String),
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocumentId::U64(v) => write!(f, "{}", v),
            DocumentId::Str(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    L2,
    Cosine,
    Dot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchResult {
    pub id: DocumentId,
    /// Higher is better; for L2 we return the negative distance.
    pub score: f32,
}

pub trait VectorIndex: Send + Sync {
    fn insert(&mut self, id: DocumentId, vector: Vec<f32>) -> Result<(), VectorIndexError>;
    fn remove(&mut self, id: &DocumentId) -> Option<Vec<f32>>;
    fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorIndexError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatIndex {
    metric: DistanceMetric,
    dimension: usize,
    vectors: Vec<(DocumentId, Vec<f32>)>,
}

impl FlatIndex {
    pub fn new(metric: DistanceMetric, dimension: usize) -> Self {
        Self {
            metric,
            dimension,
            vectors: Vec::new(),
        }
    }

    fn validate_dimension(&self, vector: &[f32]) -> Result<(), VectorIndexError> {
        if vector.len() != self.dimension {
            return Err(VectorIndexError::DimensionMismatch {
                expected: self.dimension,
                actual: vector.len(),
            });
        }
        Ok(())
    }

    fn score(&self, lhs: &[f32], rhs: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::L2 => -l2_distance(lhs, rhs),
            DistanceMetric::Cosine => cosine_similarity(lhs, rhs),
            DistanceMetric::Dot => dot_product(lhs, rhs),
        }
    }
}

impl VectorIndex for FlatIndex {
    fn insert(&mut self, id: DocumentId, vector: Vec<f32>) -> Result<(), VectorIndexError> {
        self.validate_dimension(&vector)?;
        self.vectors.push((id, vector));
        Ok(())
    }

    fn remove(&mut self, id: &DocumentId) -> Option<Vec<f32>> {
        if let Some((idx, _)) = self.vectors.iter().enumerate().find(|(_, (doc_id, _))| doc_id == id) {
            let (_, vector) = self.vectors.swap_remove(idx);
            Some(vector)
        } else {
            None
        }
    }

    fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorIndexError> {
        if top_k == 0 {
            return Err(VectorIndexError::InvalidTopK { top_k });
        }
        self.validate_dimension(query)?;

        let mut scored: Vec<SearchResult> = self
            .vectors
            .par_iter()
            .map(|(id, vector)| SearchResult {
                id: id.clone(),
                score: self.score(query, vector),
            })
            .collect();

        scored.par_sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k.min(scored.len()));
        Ok(scored)
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }
}

fn dot_product(lhs: &[f32], rhs: &[f32]) -> f32 {
    lhs.iter().zip(rhs).map(|(a, b)| a * b).sum()
}

fn l2_distance(lhs: &[f32], rhs: &[f32]) -> f32 {
    lhs.iter()
        .zip(rhs)
        .map(|(a, b)| {
            let diff = a - b;
            diff * diff
        })
        .sum::<f32>()
        .sqrt()
}

fn cosine_similarity(lhs: &[f32], rhs: &[f32]) -> f32 {
    let dot = dot_product(lhs, rhs);
    let norm_l = dot_product(lhs, lhs).sqrt();
    let norm_r = dot_product(rhs, rhs).sqrt();
    if norm_l == 0.0 || norm_r == 0.0 {
        return 0.0;
    }
    dot / (norm_l * norm_r)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_and_searches() {
        let mut index = FlatIndex::new(DistanceMetric::L2, 3);
        index.insert(DocumentId::U64(1), vec![0.0, 1.0, 2.0]).unwrap();
        index.insert(DocumentId::U64(2), vec![0.0, 1.5, 2.0]).unwrap();

        let results = index.search(&[0.0, 1.0, 2.1], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));
    }

    #[test]
    fn rejects_bad_dimension() {
        let mut index = FlatIndex::new(DistanceMetric::Cosine, 2);
        let err = index.insert(DocumentId::U64(1), vec![0.0]).unwrap_err();
        matches!(err, VectorIndexError::DimensionMismatch { .. });
    }

    #[test]
    fn invalid_top_k() {
        let index = FlatIndex::new(DistanceMetric::Dot, 2);
        let err = index.search(&[0.0, 1.0], 0).unwrap_err();
        matches!(err, VectorIndexError::InvalidTopK { .. });
    }
}
