use crate::{SearchResult, VectorIndex, VectorIndexError};
use crate::types::Filter;
use crate::filtered_search::{FilteredVectorSearch, MatchScorer};
use rayon::prelude::*;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

/// Configuration for batch search operations
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of queries to process in parallel
    pub max_parallelism: usize,
    /// Enable memory prefetching hints
    pub enable_prefetch: bool,
    /// Chunk size for processing
    pub chunk_size: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfig {
            max_parallelism: num_cpus::get(),
            enable_prefetch: true,
            chunk_size: 32,
        }
    }
}

/// Batch search executor for multiple queries
pub struct BatchSearch<'a> {
    index: &'a dyn VectorIndex,
    config: BatchConfig,
}

impl<'a> BatchSearch<'a> {
    pub fn new(index: &'a dyn VectorIndex) -> Self {
        Self {
            index,
            config: BatchConfig::default(),
        }
    }

    pub fn with_config(mut self, config: BatchConfig) -> Self {
        self.config = config;
        self
    }
    
    /// Execute batch search with parallel processing
    pub fn search(
        &self,
        queries: &[Vec<f32>],
        top_k: usize,
    ) -> Result<Vec<Vec<SearchResult>>, VectorIndexError> {
        let chunk_size = self.config.chunk_size.max(1);
        
        // Use rayon for parallelism
        let results: Result<Vec<Vec<SearchResult>>, VectorIndexError> = queries
            .par_chunks(chunk_size)
            .flat_map(|chunk| {
                if self.config.enable_prefetch {
                    // Primitive prefetch of query vectors (likely already in cache but good practice)
                    for q in chunk {
                        self.prefetch_vector(q);
                    }
                }
                
                chunk.iter().map(|query| {
                    self.index.search(query, top_k)
                }).collect::<Vec<_>>()
            })
            .collect();

        results
    }
    
    /// Execute batch search with per-query filters
    /// Note: This requires FilteredVectorSearch logic.
    pub fn search_filtered<F, C, M>(
        &self,
        queries: &[(Vec<f32>, Option<Filter>)],
        top_k: usize,
        match_scorer: &M,
        candidates_fn: &C,
        check_fn: &F,
    ) -> Result<Vec<Vec<SearchResult>>, VectorIndexError>
    where
        F: Fn(&crate::DocumentId) -> bool + Sync + Send,
        C: Fn() -> Option<Vec<crate::DocumentId>> + Sync + Send,
        M: MatchScorer + Sync + Send,
    {
         let chunk_size = self.config.chunk_size.max(1);

         let results: Result<Vec<Vec<SearchResult>>, VectorIndexError> = queries
            .par_chunks(chunk_size)
            .flat_map(|chunk| {
                chunk.iter().map(|(query, filter)| {
                     let mut searcher = FilteredVectorSearch::new(self.index);
                     if let Some(f) = filter {
                         searcher = searcher.with_filter(f);
                     }
                     
                     searcher.search(query, top_k, match_scorer, candidates_fn, check_fn)
                }).collect::<Vec<_>>()
            })
            .collect();
            
         results
    }
    
    fn prefetch_vector(&self, _vec: &[f32]) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let ptr = _vec.as_ptr() as *const i8;
            // Prefetch first cache line
            _mm_prefetch(ptr, _MM_HINT_T0);
            // If vector is large, usually we iterate it so hardware prefetcher kicks in.
        }
    }
}
