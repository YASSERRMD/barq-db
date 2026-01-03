use crate::{DocumentId, SearchResult, VectorIndex, VectorIndexError};
use crate::types::Filter;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Strategy for applying filters in vector search
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FilterStrategy {
    /// Apply filter before ANN search - best for highly selective filters (< 10% match)
    PreFilter,
    /// Apply filter after ANN search - best for low selectivity
    PostFilter,
    /// Automatically choose based on selectivity estimation
    Auto { selectivity_threshold: f32 },
}

impl Default for FilterStrategy {
    fn default() -> Self {
        FilterStrategy::Auto { selectivity_threshold: 0.1 } // 10%
    }
}

/// Selectivity estimator for filter conditions
#[derive(Debug, Clone)]
pub struct SelectivityEstimator {
    field_cardinalities: HashMap<String, usize>,
    total_documents: usize,
}

impl SelectivityEstimator {
    pub fn new(field_cardinalities: HashMap<String, usize>, total_documents: usize) -> Self {
        Self {
            field_cardinalities,
            total_documents: total_documents.max(1),
        }
    }

    pub fn estimate(&self, filter: &Filter) -> f32 {
        match filter {
            Filter::And { filters } => {
                filters.iter().map(|f| self.estimate(f)).product()
            }
            Filter::Or { filters } => {
                let prob_none_match: f32 = filters.iter().map(|f| 1.0 - self.estimate(f)).product();
                1.0 - prob_none_match
            }
            Filter::Not { filter } => 1.0 - self.estimate(filter),
            Filter::Eq { field, .. } => {
                let card = self.field_cardinalities.get(field).unwrap_or(&10); // Default assumption
                1.0 / (*card as f32).max(1.0)
            }
            Filter::In { field, values } => {
                let card = self.field_cardinalities.get(field).unwrap_or(&10);
                (values.len() as f32) / (*card as f32).max(1.0)
            }
            // Rough heuristic for ranges and others
            Filter::Gt { .. } | Filter::Gte { .. } | Filter::Lt { .. } | Filter::Lte { .. } => 0.5,
            Filter::GeoWithin { .. } => 0.1,
            Filter::Exists { .. } => 0.9,
            _ => 0.5,
        }
    }
}

/// Helper trait/struct to abstract scoring logic (accessing vectors)
pub trait MatchScorer {
    fn score(&self, id: &DocumentId, query: &[f32]) -> Option<f32>;
}

impl<F> MatchScorer for F where F: Fn(&DocumentId, &[f32]) -> Option<f32> {
    fn score(&self, id: &DocumentId, query: &[f32]) -> Option<f32> {
        self(id, query)
    }
}

/// Filtered vector search executor
pub struct FilteredVectorSearch<'a> {
    index: &'a dyn VectorIndex,
    filter: Option<&'a Filter>,
    strategy: FilterStrategy,
    estimator: Option<&'a SelectivityEstimator>,
}

impl<'a> FilteredVectorSearch<'a> {
    pub fn new(index: &'a dyn VectorIndex) -> Self {
        Self {
            index,
            filter: None,
            strategy: FilterStrategy::default(),
            estimator: None,
        }
    }

    pub fn with_filter(mut self, filter: &'a Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_strategy(mut self, strategy: FilterStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_estimator(mut self, estimator: &'a SelectivityEstimator) -> Self {
        self.estimator = Some(estimator);
        self
    }

    fn choose_strategy(&self) -> FilterStrategy {
        match self.strategy {
            FilterStrategy::Auto { selectivity_threshold } => {
                if let (Some(filter), Some(est)) = (self.filter, self.estimator) {
                    if est.estimate(filter) < selectivity_threshold {
                        FilterStrategy::PreFilter
                    } else {
                        FilterStrategy::PostFilter
                    }
                } else {
                    FilterStrategy::PostFilter
                }
            }
            s => s,
        }
    }

    /// Execute the search.
    /// `candidates_fn` returns Option<Vec<DocumentId>>. If Some, typically from Inverted Index (PreFilter).
    /// `check_fn` returns bool for a given ID (PostFilter constraint checking).
    pub fn search<F, C, M>(
        &self,
        query: &[f32],
        top_k: usize,
        match_scorer: &M,
        candidates_fn: C,
        check_fn: F,
    ) -> Result<Vec<SearchResult>, VectorIndexError>
    where
        F: Fn(&DocumentId) -> bool,
        C: Fn() -> Option<Vec<DocumentId>>,
        M: MatchScorer,
    {
        if self.filter.is_none() {
            // No filter, just search
            return self.index.search(query, top_k);
        }

        let strategy = self.choose_strategy();

        match strategy {
            FilterStrategy::PreFilter | FilterStrategy::Auto { .. } => {
                // Try to get candidates first
                if let Some(candidates) = candidates_fn() {
                    // We have specific candidates. Calculate scores for them and pick Top K
                    Ok(self.score_candidates(candidates, query, top_k, match_scorer)?)
                } else {
                    // Fallback to post filtering if candidates cannot be retrieved
                    self.execute_post_filter(query, top_k, check_fn)
                }
            }
            FilterStrategy::PostFilter => {
                self.execute_post_filter(query, top_k, check_fn)
            }
        }
    }

    fn execute_post_filter<F>(
        &self,
        query: &[f32],
        top_k: usize,
        check_fn: F,
    ) -> Result<Vec<SearchResult>, VectorIndexError>
    where
        F: Fn(&DocumentId) -> bool,
    {
        // Search more to account for filtering
        // Simple heuristic: k * (1 / selectivity). Bounded.
        let multiplier = if let (Some(f), Some(e)) = (self.filter, self.estimator) {
             (1.0 / e.estimate(f).max(0.01)).min(20.0) as usize
        } else {
             4 // Default multiplier
        };
        
        let fetch_k = top_k.saturating_mul(multiplier).max(top_k + 50);
        let results = self.index.search(query, fetch_k)?;
        
        // Filter and take top k
        let filtered: Vec<SearchResult> = results.into_iter()
            .filter(|r| check_fn(&r.id))
            .take(top_k)
            .collect();
        
        Ok(filtered)
    }

    fn score_candidates<M: MatchScorer>(
         &self,
         candidates: Vec<DocumentId>,
         query: &[f32],
         top_k: usize,
         match_scorer: &M
    ) -> Result<Vec<SearchResult>, VectorIndexError> {
        let mut results = Vec::with_capacity(candidates.len());
        for id in candidates {
             if let Some(score) = match_scorer.score(&id, query) {
                 results.push(SearchResult { id, score });
             }
        }
        
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(top_k);
        Ok(results)
    }
}
