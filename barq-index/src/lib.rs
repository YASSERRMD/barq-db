use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

mod distance;
pub mod types;
pub mod filtered_search;

use distance::*;
pub use types::{Filter, GeoBoundingBox, GeoPoint, PayloadValue};
pub use filtered_search::{FilteredVectorSearch, FilterStrategy, SelectivityEstimator, MatchScorer};

#[derive(Debug, Copy, Clone, PartialEq)]
struct OrderedScore(f32);

impl Eq for OrderedScore {}

impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VectorIndexError {
    #[error("vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("search requested top_k={top_k}, but a positive value is required")]
    InvalidTopK { top_k: usize },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DocumentIdError {
    #[error("document id string cannot be empty")]
    EmptyString,

    #[error("document id must be positive, got {0}")]
    NonPositiveU64(u64),
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

impl Ord for DocumentId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_string().cmp(&other.to_string())
    }
}

impl PartialOrd for DocumentId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl DocumentId {
    pub fn validate(&self) -> Result<(), DocumentIdError> {
        match self {
            DocumentId::U64(v) if *v == 0 => Err(DocumentIdError::NonPositiveU64(*v)),
            DocumentId::Str(s) if s.trim().is_empty() => Err(DocumentIdError::EmptyString),
            _ => Ok(()),
        }
    }
}

impl FromStr for DocumentId {
    type Err = DocumentIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(value) = s.parse::<u64>() {
            let id = DocumentId::U64(value);
            id.validate().map(|_| id)
        } else if s.trim().is_empty() {
            Err(DocumentIdError::EmptyString)
        } else {
            Ok(DocumentId::Str(s.to_string()))
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    Flat,
    Hnsw(HnswParams),
    Ivf(IvfParams),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HnswParams {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 64,
            ef_search: 64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IvfParams {
    pub nlist: usize,
    pub nprobe: usize,
    pub pq: Option<PqConfig>,
}

impl Default for IvfParams {
    fn default() -> Self {
        Self {
            nlist: 8,
            nprobe: 2,
            pq: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PqConfig {
    pub segments: usize,
    pub codebook_bits: u8,
}

impl Default for PqConfig {
    fn default() -> Self {
        Self {
            segments: 4,
            codebook_bits: 8,
        }
    }
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

    pub fn dimension(&self) -> usize {
        self.dimension
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
        score_with_metric(self.metric, lhs, rhs)
    }
}

impl VectorIndex for FlatIndex {
    fn insert(&mut self, id: DocumentId, vector: Vec<f32>) -> Result<(), VectorIndexError> {
        self.validate_dimension(&vector)?;
        self.vectors.push((id, vector));
        Ok(())
    }

    fn remove(&mut self, id: &DocumentId) -> Option<Vec<f32>> {
        if let Some((idx, _)) = self
            .vectors
            .iter()
            .enumerate()
            .find(|(_, (doc_id, _))| doc_id == id)
        {
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

        scored.par_sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(top_k.min(scored.len()));
        Ok(scored)
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }
}

#[derive(Debug, Clone)]
pub struct HnswIndex {
    metric: DistanceMetric,
    dimension: usize,
    params: HnswParams,
    vectors: HashMap<DocumentId, Vec<f32>>,
    neighbors: HashMap<DocumentId, Vec<DocumentId>>,
    entry_point: Option<DocumentId>,
}

impl HnswIndex {
    pub fn new(metric: DistanceMetric, dimension: usize, params: HnswParams) -> Self {
        Self {
            metric,
            dimension,
            params,
            vectors: HashMap::new(),
            neighbors: HashMap::new(),
            entry_point: None,
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
        score_with_metric(self.metric, lhs, rhs)
    }

    fn update_neighbors(&mut self, id: &DocumentId, neighbors: Vec<DocumentId>) {
        let mut unique: Vec<DocumentId> = neighbors;
        unique.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        unique.dedup_by(|a, b| a.to_string() == b.to_string());
        if unique.len() > self.params.m {
            unique.truncate(self.params.m);
        }
        self.neighbors.insert(id.clone(), unique);
    }

    fn candidate_neighbors(&self, vector: &[f32], ef: usize) -> Vec<(DocumentId, f32)> {
        let mut scored: Vec<(DocumentId, f32)> = self
            .vectors
            .par_iter()
            .map(|(id, stored)| (id.clone(), self.score(vector, stored)))
            .collect();
        scored.par_sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        scored.truncate(ef.min(scored.len()));
        scored
    }

    fn search_internal(&self, query: &[f32], ef: usize) -> Vec<SearchResult> {
        let entry = match &self.entry_point {
            Some(e) => e,
            None => return Vec::new(),
        };

        let mut visited = HashSet::new();
        let mut candidate = BinaryHeap::new();
        let entry_score = self
            .vectors
            .get(entry)
            .map(|v| self.score(query, v))
            .unwrap_or(f32::MIN);
        candidate.push((OrderedScore(entry_score), entry.clone()));
        let mut results = BinaryHeap::new();

        while let Some((OrderedScore(score), current)) = candidate.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());
            results.push((OrderedScore(score), current.clone()));

            if results.len() > ef {
                results.pop();
            }

            if let Some(neigh) = self.neighbors.get(&current) {
                for n in neigh {
                    if visited.contains(n) {
                        continue;
                    }
                    if let Some(vec) = self.vectors.get(n) {
                        let s = self.score(query, vec);
                        candidate.push((OrderedScore(s), n.clone()));
                    }
                }
            }
        }

        let mut collected: Vec<SearchResult> = results
            .into_sorted_vec()
            .into_iter()
            .rev()
            .map(|(OrderedScore(score), id)| SearchResult { id, score })
            .collect();
        collected.truncate(ef.min(collected.len()));
        collected
    }
}

impl VectorIndex for HnswIndex {
    fn insert(&mut self, id: DocumentId, vector: Vec<f32>) -> Result<(), VectorIndexError> {
        self.validate_dimension(&vector)?;
        if self.entry_point.is_none() {
            self.entry_point = Some(id.clone());
        }

        let neighbors = self
            .candidate_neighbors(&vector, self.params.ef_construction)
            .into_iter()
            .map(|(candidate_id, _)| candidate_id)
            .collect::<Vec<_>>();

        self.vectors.insert(id.clone(), vector);
        self.update_neighbors(&id, neighbors.clone());

        for neighbor in neighbors {
            let mut neighbor_edges = self.neighbors.get(&neighbor).cloned().unwrap_or_default();
            neighbor_edges.push(id.clone());
            neighbor_edges.sort_by(|a, b| {
                let va = self.vectors.get(a).unwrap();
                let vb = self.vectors.get(b).unwrap();
                let sa = self.score(va, self.vectors.get(&neighbor).unwrap());
                let sb = self.score(vb, self.vectors.get(&neighbor).unwrap());
                sb.partial_cmp(&sa).unwrap_or(Ordering::Equal)
            });
            if neighbor_edges.len() > self.params.m {
                neighbor_edges.truncate(self.params.m);
            }
            self.neighbors.insert(neighbor.clone(), neighbor_edges);
        }

        Ok(())
    }

    fn remove(&mut self, id: &DocumentId) -> Option<Vec<f32>> {
        let removed = self.vectors.remove(id);
        self.neighbors.remove(id);
        for edges in self.neighbors.values_mut() {
            edges.retain(|neighbor| neighbor != id);
        }
        if self.entry_point.as_ref() == Some(id) {
            self.entry_point = self.vectors.keys().next().cloned();
        }
        removed
    }

    fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorIndexError> {
        if top_k == 0 {
            return Err(VectorIndexError::InvalidTopK { top_k });
        }
        self.validate_dimension(query)?;
        let ef = self.params.ef_search.max(top_k);
        let mut candidates = self.search_internal(query, ef);
        candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        candidates.truncate(top_k.min(candidates.len()));
        Ok(candidates)
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }
}

#[derive(Debug, Clone)]
enum StoredVector {
    Raw(Vec<f32>),
    Quantized(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct ProductQuantizer {
    config: PqConfig,
    dimension: usize,
    mins: Vec<f32>,
    maxs: Vec<f32>,
}

impl ProductQuantizer {
    pub fn train(config: PqConfig, dimension: usize, samples: &[Vec<f32>]) -> Self {
        let mut mins = vec![f32::INFINITY; dimension];
        let mut maxs = vec![f32::NEG_INFINITY; dimension];

        for sample in samples {
            for (i, value) in sample.iter().enumerate() {
                mins[i] = mins[i].min(*value);
                maxs[i] = maxs[i].max(*value);
            }
        }

        Self {
            config,
            dimension,
            mins,
            maxs,
        }
    }

    fn quantize_value(&self, dim: usize, value: f32) -> u8 {
        let levels = (1 << self.config.codebook_bits) as f32;
        let min = self.mins[dim];
        let max = self.maxs[dim];
        if max <= min {
            return 0;
        }
        let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
        (normalized * (levels - 1.0)).round() as u8
    }

    fn dequantize_value(&self, dim: usize, code: u8) -> f32 {
        let levels = (1 << self.config.codebook_bits) as f32;
        let min = self.mins[dim];
        let max = self.maxs[dim];
        if max <= min {
            return min;
        }
        let step = (max - min) / (levels - 1.0);
        min + step * code as f32
    }

    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dimension);
        vector
            .iter()
            .enumerate()
            .map(|(i, v)| self.quantize_value(i, *v))
            .collect()
    }

    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        debug_assert_eq!(codes.len(), self.dimension);
        codes
            .iter()
            .enumerate()
            .map(|(i, code)| self.dequantize_value(i, *code))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct IvfIndex {
    metric: DistanceMetric,
    dimension: usize,
    params: IvfParams,
    centroids: Vec<Vec<f32>>,
    inverted_lists: Vec<Vec<(DocumentId, StoredVector)>>,
    assignments: HashMap<DocumentId, usize>,
    pq: Option<ProductQuantizer>,
}

impl IvfIndex {
    pub fn new(metric: DistanceMetric, dimension: usize, params: IvfParams) -> Self {
        Self {
            metric,
            dimension,
            params,
            centroids: Vec::new(),
            inverted_lists: Vec::new(),
            assignments: HashMap::new(),
            pq: None,
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
        score_with_metric(self.metric, lhs, rhs)
    }

    fn ensure_lists(&mut self) {
        if self.inverted_lists.len() < self.centroids.len() {
            self.inverted_lists
                .resize_with(self.centroids.len(), Vec::new);
        }
    }

    fn nearest_centroid(&self, vector: &[f32]) -> Option<usize> {
        self.centroids
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                let sa = self.score(vector, a);
                let sb = self.score(vector, b);
                sa.partial_cmp(&sb).unwrap_or(Ordering::Equal)
            })
            .map(|(idx, _)| idx)
    }

    fn ensure_pq(&mut self) {
        if self.params.pq.is_none() || self.pq.is_some() {
            return;
        }
        let samples: Vec<Vec<f32>> = self
            .inverted_lists
            .iter()
            .flat_map(|list| {
                list.iter().filter_map(|(_, stored)| match stored {
                    StoredVector::Raw(v) => Some(v.clone()),
                    StoredVector::Quantized(_) => None,
                })
            })
            .collect();
        if samples.is_empty() {
            return;
        }
        let config = self.params.pq.clone().unwrap_or_default();
        self.pq = Some(ProductQuantizer::train(config, self.dimension, &samples));
    }
}

impl VectorIndex for IvfIndex {
    fn insert(&mut self, id: DocumentId, vector: Vec<f32>) -> Result<(), VectorIndexError> {
        self.validate_dimension(&vector)?;
        if self.centroids.len() < self.params.nlist {
            self.centroids.push(vector.clone());
            self.ensure_lists();
        }

        self.ensure_lists();
        self.ensure_pq();
        let centroid_idx = self.nearest_centroid(&vector).unwrap_or(0);
        let stored = match self.pq.as_ref() {
            Some(pq) => StoredVector::Quantized(pq.encode(&vector)),
            None => StoredVector::Raw(vector.clone()),
        };
        self.inverted_lists[centroid_idx].push((id.clone(), stored));
        self.assignments.insert(id, centroid_idx);
        Ok(())
    }

    fn remove(&mut self, id: &DocumentId) -> Option<Vec<f32>> {
        let list_idx = self.assignments.remove(id)?;
        let list = self.inverted_lists.get_mut(list_idx)?;
        if let Some(pos) = list.iter().position(|(doc_id, _)| doc_id == id) {
            let (_, stored) = list.swap_remove(pos);
            match stored {
                StoredVector::Raw(v) => Some(v),
                StoredVector::Quantized(codes) => self.pq.as_ref().map(|pq| pq.decode(&codes)),
            }
        } else {
            None
        }
    }

    fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorIndexError> {
        if top_k == 0 {
            return Err(VectorIndexError::InvalidTopK { top_k });
        }
        self.validate_dimension(query)?;
        if self.centroids.is_empty() {
            return Ok(Vec::new());
        }

        let mut scored_centroids: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(idx, centroid)| (idx, self.score(query, centroid)))
            .collect();
        scored_centroids.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        scored_centroids.truncate(self.params.nprobe.min(scored_centroids.len()));

        let mut results = Vec::new();
        for (centroid_idx, _) in scored_centroids {
            if let Some(list) = self.inverted_lists.get(centroid_idx) {
                for (doc_id, stored) in list {
                    let candidate = match stored {
                        StoredVector::Raw(v) => v.clone(),
                        StoredVector::Quantized(codes) => match &self.pq {
                            Some(pq) => pq.decode(codes),
                            None => continue,
                        },
                    };
                    let score = self.score(query, &candidate);
                    results.push(SearchResult {
                        id: doc_id.clone(),
                        score,
                    });
                }
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        results.truncate(top_k.min(results.len()));
        Ok(results)
    }

    fn len(&self) -> usize {
        self.assignments.len()
    }
}

#[derive(Debug)]
pub struct BackgroundBuildHandle<T> {
    handle: std::thread::JoinHandle<T>,
}

impl<T> BackgroundBuildHandle<T> {
    pub fn join(self) -> T {
        self.handle
            .join()
            .expect("background build thread panicked")
    }
}

pub fn spawn_background_build<F, T>(task: F) -> BackgroundBuildHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let handle = std::thread::spawn(task);
    BackgroundBuildHandle { handle }
}

#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub metric: DistanceMetric,
    pub dimension: usize,
    pub index: IndexType,
}

impl IndexConfig {
    pub fn new(metric: DistanceMetric, dimension: usize, index: IndexType) -> Self {
        Self {
            metric,
            dimension,
            index,
        }
    }
}

pub fn build_index(config: IndexConfig) -> Box<dyn VectorIndex> {
    match config.index {
        IndexType::Flat => Box::new(FlatIndex::new(config.metric, config.dimension)),
        IndexType::Hnsw(params) => {
            Box::new(HnswIndex::new(config.metric, config.dimension, params))
        }
        IndexType::Ivf(params) => Box::new(IvfIndex::new(config.metric, config.dimension, params)),
    }
}

// moved to distance.rs

// functions moved to distance.rs

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::time::Duration;

    use rand::{rngs::StdRng, Rng, SeedableRng};

    #[test]
    fn hnsw_insert_and_search() {
        let mut index = HnswIndex::new(DistanceMetric::L2, 2, HnswParams::default());
        index.insert(DocumentId::U64(1), vec![0.0, 0.0]).unwrap();
        index.insert(DocumentId::U64(2), vec![1.0, 1.0]).unwrap();
        index.insert(DocumentId::U64(3), vec![2.0, 2.0]).unwrap();

        let results = index.search(&[0.1, 0.1], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, DocumentId::U64(1));
    }

    #[test]
    fn ivf_insert_search_with_pq() {
        let mut index = IvfIndex::new(
            DistanceMetric::L2,
            2,
            IvfParams {
                nlist: 2,
                nprobe: 2,
                pq: Some(PqConfig::default()),
            },
        );

        index.insert(DocumentId::U64(1), vec![0.0, 0.0]).unwrap();
        index.insert(DocumentId::U64(2), vec![1.0, 1.0]).unwrap();
        index.insert(DocumentId::U64(3), vec![0.9, 1.1]).unwrap();

        let results = index.search(&[0.95, 1.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, DocumentId::U64(2));
    }

    #[test]
    fn factory_builds_indexes() {
        let flat = build_index(IndexConfig::new(DistanceMetric::Dot, 2, IndexType::Flat));
        assert_eq!(flat.len(), 0);

        let hnsw = build_index(IndexConfig::new(
            DistanceMetric::Cosine,
            2,
            IndexType::Hnsw(HnswParams::default()),
        ));
        assert_eq!(hnsw.len(), 0);
    }

    #[test]
    fn background_build_runs() {
        let counter = Arc::new(AtomicUsize::new(0));
        let cloned = counter.clone();
        let handle = spawn_background_build(move || {
            cloned.fetch_add(1, AtomicOrdering::SeqCst);
        });
        handle.join();
        assert_eq!(counter.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn inserts_and_searches() {
        let mut index = FlatIndex::new(DistanceMetric::L2, 3);
        index
            .insert(DocumentId::U64(1), vec![0.0, 1.0, 2.0])
            .unwrap();
        index
            .insert(DocumentId::U64(2), vec![0.0, 1.5, 2.0])
            .unwrap();

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

    #[test]
    fn validates_document_ids() {
        assert!(DocumentId::U64(1).validate().is_ok());
        assert!(DocumentId::Str("abc".into()).validate().is_ok());
        assert_eq!(
            DocumentId::U64(0).validate().unwrap_err(),
            DocumentIdError::NonPositiveU64(0)
        );
        assert_eq!(
            DocumentId::from_str("").unwrap_err(),
            DocumentIdError::EmptyString
        );
    }

    #[test]
    fn hnsw_large_collection_latency_budget() {
        let dimension = 32;
        let mut rng = StdRng::seed_from_u64(13);
        let mut index = HnswIndex::new(
            DistanceMetric::L2,
            dimension,
            HnswParams {
                m: 32,
                ef_construction: 100,
                ef_search: 200,
            },
        );

        let mut stored = Vec::new();
        for i in 0..1_200u64 {
            let vector: Vec<f32> = (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let id = DocumentId::U64(i + 1);
            stored.push((id.clone(), vector.clone()));
            index.insert(id, vector).unwrap();
        }

        let mut evaluated = 0;
        let mut empty_results = 0;
        let mut max_elapsed = Duration::ZERO;

        for (_, vector) in stored.iter().step_by(75).take(20) {
            let start = std::time::Instant::now();
            let results = index.search(vector, 10).unwrap();
            let elapsed = start.elapsed();
            max_elapsed = max_elapsed.max(elapsed);
            evaluated += 1;
            if results.is_empty() {
                empty_results += 1;
            }
        }

        // Allow a modest headroom to avoid flakes on slower CI hardware while still
        // ensuring HNSW search remains low latency for reasonably large collections.
        assert!(
            max_elapsed < Duration::from_millis(250),
            "search took {:?}",
            max_elapsed
        );
        assert_eq!(
            empty_results, 0,
            "missing matches for {empty_results} / {evaluated}"
        );
    }
}
