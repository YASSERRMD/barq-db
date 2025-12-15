use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use barq_index::{DocumentId, DocumentIdError, SearchResult};

#[derive(Debug, thiserror::Error)]
pub enum TextIndexError {
    #[error("document id error: {0}")]
    DocumentId(#[from] DocumentIdError),

    #[error("search requested top_k={top_k}, but a positive value is required")]
    InvalidTopK { top_k: usize },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Bm25Config {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

#[derive(Debug, Clone)]
pub struct DocumentTerms {
    pub length: usize,
    pub frequencies: HashMap<String, usize>,
}

impl DocumentTerms {
    fn new(tokens: &[String]) -> Self {
        let mut frequencies = HashMap::new();
        for token in tokens {
            *frequencies.entry(token.clone()).or_insert(0) += 1;
        }
        Self {
            length: tokens.len(),
            frequencies,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Posting {
    pub doc_id: DocumentId,
    pub term_freq: usize,
}

#[derive(Debug, Default, Clone)]
pub struct Analyzer;

impl Analyzer {
    pub fn tokenize(&self, text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter_map(|token| {
                let normalized = token.to_ascii_lowercase();
                if normalized.is_empty() {
                    None
                } else {
                    Some(normalized)
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Bm25Index {
    analyzer: Analyzer,
    config: Bm25Config,
    postings: HashMap<String, Vec<Posting>>, // term -> postings list
    documents: HashMap<DocumentId, DocumentTerms>,
    total_doc_length: usize,
}

impl Bm25Index {
    pub fn new(config: Bm25Config) -> Self {
        Self {
            analyzer: Analyzer::default(),
            config,
            postings: HashMap::new(),
            documents: HashMap::new(),
            total_doc_length: 0,
        }
    }

    pub fn insert(
        &mut self,
        doc_id: DocumentId,
        text_fields: &[String],
    ) -> Result<(), TextIndexError> {
        doc_id.validate()?;
        if self.documents.contains_key(&doc_id) {
            self.remove(&doc_id);
        }

        let tokens = self.analyzer.tokenize(&text_fields.join(" "));
        let terms = DocumentTerms::new(&tokens);

        for (term, freq) in &terms.frequencies {
            self.postings
                .entry(term.clone())
                .or_default()
                .push(Posting {
                    doc_id: doc_id.clone(),
                    term_freq: *freq,
                });
        }
        self.total_doc_length += terms.length;
        self.documents.insert(doc_id, terms);
        Ok(())
    }

    pub fn remove(&mut self, doc_id: &DocumentId) {
        if let Some(terms) = self.documents.remove(doc_id) {
            for (term, _) in terms.frequencies {
                if let Some(list) = self.postings.get_mut(&term) {
                    list.retain(|posting| posting.doc_id != *doc_id);
                    if list.is_empty() {
                        self.postings.remove(&term);
                    }
                }
            }
            self.total_doc_length = self.total_doc_length.saturating_sub(terms.length);
        }
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn document_frequency(&self, term: &str) -> usize {
        self.postings.get(term).map(|p| p.len()).unwrap_or(0)
    }

    pub fn term_frequency(&self, doc_id: &DocumentId, term: &str) -> Option<usize> {
        self.documents
            .get(doc_id)
            .and_then(|terms| terms.frequencies.get(term).copied())
    }

    pub fn document_length(&self, doc_id: &DocumentId) -> Option<usize> {
        self.documents.get(doc_id).map(|terms| terms.length)
    }

    pub fn config(&self) -> Bm25Config {
        self.config
    }

    pub fn avg_doc_length(&self) -> f32 {
        if self.documents.is_empty() {
            0.0
        } else {
            self.total_doc_length as f32 / self.documents.len() as f32
        }
    }

    pub fn search(&self, query: &str, top_k: usize) -> Result<Vec<SearchResult>, TextIndexError> {
        if top_k == 0 {
            return Err(TextIndexError::InvalidTopK { top_k });
        }
        if self.documents.is_empty() {
            return Ok(Vec::new());
        }
        let tokens = self.analyzer.tokenize(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let mut scores: HashMap<DocumentId, f32> = HashMap::new();
        let avg_dl = self.avg_doc_length();
        let total_docs = self.documents.len() as f32;

        for term in tokens {
            if let Some(postings) = self.postings.get(&term) {
                let df = postings.len() as f32;
                let idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();
                for posting in postings {
                    if let Some(doc_terms) = self.documents.get(&posting.doc_id) {
                        let tf = posting.term_freq as f32;
                        let numerator = tf * (self.config.k1 + 1.0);
                        let denominator = tf
                            + self.config.k1
                                * (1.0 - self.config.b
                                    + self.config.b * (doc_terms.length as f32 / avg_dl));
                        let contribution = idf * (numerator / denominator);
                        scores
                            .entry(posting.doc_id.clone())
                            .and_modify(|score| *score += contribution)
                            .or_insert(contribution);
                    }
                }
            }
        }

        let mut results: Vec<SearchResult> = scores
            .into_iter()
            .map(|(id, score)| SearchResult { id, score })
            .collect();
        results.par_sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k.min(results.len()));
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_and_scores_documents() {
        let mut index = Bm25Index::new(Bm25Config::default());
        index
            .insert(
                DocumentId::U64(1),
                &["Rust programming language".to_string()],
            )
            .unwrap();
        index
            .insert(
                DocumentId::U64(2),
                &["The Rust book and community".to_string()],
            )
            .unwrap();

        let results = index.search("rust book", 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, DocumentId::U64(2));
        assert!(results[0].score >= results[1].score);
    }

    #[test]
    fn removes_documents() {
        let mut index = Bm25Index::new(Bm25Config::default());
        index
            .insert(DocumentId::U64(1), &["hello world".to_string()])
            .unwrap();
        assert_eq!(index.document_count(), 1);
        index.remove(&DocumentId::U64(1));
        assert_eq!(index.document_count(), 0);
        let results = index.search("hello", 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn rejects_zero_top_k() {
        let index = Bm25Index::new(Bm25Config::default());
        let err = index.search("hi", 0).unwrap_err();
        matches!(err, TextIndexError::InvalidTopK { .. });
    }

    #[test]
    fn tracks_document_statistics() {
        let mut index = Bm25Index::new(Bm25Config::default());
        index
            .insert(DocumentId::U64(1), &["Rust language book".to_string()])
            .unwrap();
        index
            .insert(DocumentId::U64(2), &["Rust programming guide".to_string()])
            .unwrap();

        assert_eq!(index.document_count(), 2);
        assert_eq!(index.document_frequency("rust"), 2);
        assert_eq!(index.term_frequency(&DocumentId::U64(1), "rust"), Some(1));
        assert!(index.avg_doc_length() > 0.0);

        index.remove(&DocumentId::U64(1));
        assert_eq!(index.document_frequency("rust"), 1);
        assert_eq!(index.document_length(&DocumentId::U64(2)), Some(3));
    }

    #[test]
    fn respects_custom_config() {
        let config = Bm25Config { k1: 0.9, b: 0.0 };
        let mut index = Bm25Index::new(config);
        index
            .insert(DocumentId::U64(1), &["rust rust guide".to_string()])
            .unwrap();
        index
            .insert(DocumentId::U64(2), &["rust reference".to_string()])
            .unwrap();

        let results = index.search("rust", 2).unwrap();
        let idf = (((2.0f32 - 2.0 + 0.5) / (2.0 + 0.5)) + 1.0).ln();
        let tf = 2.0f32;
        let expected_score = idf * (tf * (config.k1 + 1.0) / (tf + config.k1));
        assert!((results[0].score - expected_score).abs() < 1e-5);
        assert_eq!(index.config(), config);
    }
}
