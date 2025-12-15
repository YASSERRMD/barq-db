use barq_bm25::{Bm25Config, Bm25Index, TextIndexError};
use barq_index::{
    build_index, DistanceMetric, DocumentId, DocumentIdError, IndexConfig, IndexType, SearchResult,
    VectorIndex,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("collection {0} already exists")]
    CollectionExists(String),

    #[error("collection {0} not found")]
    CollectionMissing(String),

    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    #[error("index error: {0}")]
    Index(#[from] barq_index::VectorIndexError),

    #[error("text index error: {0}")]
    TextIndex(#[from] TextIndexError),

    #[error("invalid document id: {0}")]
    DocumentId(#[from] DocumentIdError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldType {
    Vector {
        dimension: usize,
        metric: DistanceMetric,
        #[serde(default)]
        index: Option<IndexType>,
    },
    Text {
        indexed: bool,
    },
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionSchema {
    pub name: String,
    pub fields: Vec<FieldSchema>,
    #[serde(default)]
    pub bm25_config: Option<Bm25Config>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PayloadValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Array(Vec<PayloadValue>),
    Object(HashMap<String, PayloadValue>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Document {
    pub id: DocumentId,
    pub vector: Vec<f32>,
    pub payload: Option<PayloadValue>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct HybridWeights {
    pub bm25: f32,
    pub vector: f32,
}

impl Default for HybridWeights {
    fn default() -> Self {
        Self {
            bm25: 0.5,
            vector: 0.5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HybridSearchResult {
    pub id: DocumentId,
    pub bm25_score: Option<f32>,
    pub vector_score: Option<f32>,
    pub score: f32,
}

impl CollectionSchema {
    pub fn validate(&self) -> Result<(), CatalogError> {
        let vector_fields: Vec<_> = self
            .fields
            .iter()
            .filter_map(|field| match field.field_type {
                FieldType::Vector {
                    dimension,
                    metric: _,
                    index: _,
                } => Some((field.name.clone(), dimension)),
                _ => None,
            })
            .collect();

        if vector_fields.is_empty() {
            return Err(CatalogError::InvalidSchema(
                "schema missing vector field".to_string(),
            ));
        }

        if vector_fields.iter().any(|(_, dim)| *dim == 0) {
            return Err(CatalogError::InvalidSchema(
                "vector dimension must be positive".to_string(),
            ));
        }

        Ok(())
    }

    pub fn set_vector_index(&mut self, index: IndexType) {
        if let Some(field) = self
            .fields
            .iter_mut()
            .find(|field| matches!(field.field_type, FieldType::Vector { .. }))
        {
            if let FieldType::Vector { index: idx, .. } = &mut field.field_type {
                *idx = Some(index);
            }
        }
    }

    pub fn vector_config(&self) -> Option<(usize, DistanceMetric, IndexType)> {
        self.fields
            .iter()
            .find_map(|field| match &field.field_type {
                FieldType::Vector {
                    dimension,
                    metric,
                    index,
                } => Some((
                    *dimension,
                    *metric,
                    index.clone().unwrap_or(IndexType::Flat),
                )),
                _ => None,
            })
    }

    pub fn bm25_config(&self) -> Bm25Config {
        self.bm25_config.unwrap_or_default()
    }

    pub fn indexed_text_fields(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter_map(|field| match field.field_type {
                FieldType::Text { indexed } if indexed => Some(field.name.clone()),
                _ => None,
            })
            .collect()
    }
}

pub struct Collection {
    schema: CollectionSchema,
    index: Box<dyn VectorIndex>,
    vectors: HashMap<DocumentId, Vec<f32>>,
    payloads: HashMap<DocumentId, PayloadValue>,
    dimension: usize,
    metric: DistanceMetric,
    index_type: IndexType,
    text_index: Option<Bm25Index>,
}

impl fmt::Debug for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collection")
            .field("schema", &self.schema)
            .field("vectors", &self.vectors.len())
            .field("payloads", &self.payloads.len())
            .field("dimension", &self.dimension)
            .field("metric", &self.metric)
            .field("index_type", &self.index_type)
            .field("text_index", &self.text_index.is_some())
            .finish()
    }
}

impl Collection {
    pub fn new(schema: CollectionSchema) -> Result<Self, CatalogError> {
        let mut schema = schema;
        schema.validate()?;
        let (dimension, metric, index_type) = schema
            .vector_config()
            .ok_or_else(|| CatalogError::InvalidSchema("schema missing vector field".into()))?;
        let has_text_index = !schema.indexed_text_fields().is_empty();
        let bm25_config = schema.bm25_config();
        schema.set_vector_index(index_type.clone());

        Ok(Self {
            schema,
            index: build_index(IndexConfig::new(metric, dimension, index_type.clone())),
            vectors: HashMap::new(),
            payloads: HashMap::new(),
            dimension,
            metric,
            index_type,
            text_index: if has_text_index {
                Some(Bm25Index::new(bm25_config))
            } else {
                None
            },
        })
    }

    pub fn insert(&mut self, document: Document) -> Result<(), CatalogError> {
        self.validate_document(&document)?;
        let text_values = self.text_field_values(&document.payload)?;
        self.index
            .insert(document.id.clone(), document.vector.clone())?;
        self.vectors
            .insert(document.id.clone(), document.vector.clone());
        if let Some(index) = &mut self.text_index {
            index.insert(document.id.clone(), &text_values)?;
        }
        if let Some(payload) = document.payload {
            self.payloads.insert(document.id, payload);
        }
        Ok(())
    }

    pub fn upsert(&mut self, document: Document) -> Result<(), CatalogError> {
        if self.payloads.contains_key(&document.id) {
            self.index.remove(&document.id);
            self.vectors.remove(&document.id);
            if let Some(index) = &mut self.text_index {
                index.remove(&document.id);
            }
        }
        self.insert(document)
    }

    pub fn delete(&mut self, id: &DocumentId) -> bool {
        let removed = self.index.remove(id);
        self.vectors.remove(id);
        self.payloads.remove(id);
        if let Some(index) = &mut self.text_index {
            index.remove(id);
        }
        removed.is_some()
    }

    pub fn search(&self, vector: &[f32], top_k: usize) -> Result<Vec<SearchResult>, CatalogError> {
        Ok(self.index.search(vector, top_k)?)
    }

    pub fn search_text(
        &self,
        query: &str,
        top_k: usize,
    ) -> Result<Vec<SearchResult>, CatalogError> {
        let index = self
            .text_index
            .as_ref()
            .ok_or_else(|| CatalogError::InvalidSchema("collection has no text index".into()))?;
        Ok(index.search(query, top_k)?)
    }

    pub fn search_hybrid(
        &self,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<HybridWeights>,
    ) -> Result<Vec<HybridSearchResult>, CatalogError> {
        let weights = weights.unwrap_or_default();
        let text_index = self
            .text_index
            .as_ref()
            .ok_or_else(|| CatalogError::InvalidSchema("collection has no text index".into()))?;

        if top_k == 0 {
            return Err(CatalogError::InvalidSchema(
                "top_k must be positive".to_string(),
            ));
        }

        let (bm25_results, vector_results) = rayon::join(
            || text_index.search(query, top_k * 2),
            || self.index.search(vector, top_k * 2),
        );

        let bm25_results = bm25_results?;
        let vector_results = vector_results?;

        let normalized_bm25 = normalize_scores(&bm25_results);
        let normalized_vectors = normalize_scores(&vector_results);

        let mut combined: HashMap<DocumentId, HybridSearchResult> = HashMap::new();

        for result in bm25_results {
            let normalized = *normalized_bm25.get(&result.id).unwrap_or(&0.0);
            combined
                .entry(result.id.clone())
                .and_modify(|entry| {
                    entry.bm25_score = Some(result.score);
                    entry.score += weights.bm25 * normalized;
                })
                .or_insert(HybridSearchResult {
                    id: result.id,
                    bm25_score: Some(result.score),
                    vector_score: None,
                    score: weights.bm25 * normalized,
                });
        }

        for result in vector_results {
            let normalized = *normalized_vectors.get(&result.id).unwrap_or(&0.0);
            combined
                .entry(result.id.clone())
                .and_modify(|entry| {
                    entry.vector_score = Some(result.score);
                    entry.score += weights.vector * normalized;
                })
                .or_insert(HybridSearchResult {
                    id: result.id,
                    bm25_score: None,
                    vector_score: Some(result.score),
                    score: weights.vector * normalized,
                });
        }

        let mut results: Vec<HybridSearchResult> = combined.into_values().collect();
        results.par_sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k.min(results.len()));
        Ok(results)
    }

    pub fn explain_hybrid(
        &self,
        vector: &[f32],
        query: &str,
        top_k: usize,
        id: &DocumentId,
        weights: Option<HybridWeights>,
    ) -> Result<Option<HybridSearchResult>, CatalogError> {
        let results = self.search_hybrid(vector, query, top_k, weights)?;
        Ok(results.into_iter().find(|res| &res.id == id))
    }

    pub fn rebuild_index(&mut self, index_type: Option<IndexType>) -> Result<(), CatalogError> {
        let target = index_type.unwrap_or_else(|| self.index_type.clone());
        let mut new_index = build_index(IndexConfig::new(
            self.metric,
            self.dimension,
            target.clone(),
        ));
        for (id, vector) in self.vectors.iter() {
            new_index.insert(id.clone(), vector.clone())?;
        }
        self.index = new_index;
        self.index_type = target.clone();
        self.schema.set_vector_index(target);
        Ok(())
    }

    pub fn schema(&self) -> &CollectionSchema {
        &self.schema
    }

    pub fn vector_dimension(&self) -> usize {
        self.dimension
    }

    fn validate_document(&self, document: &Document) -> Result<(), CatalogError> {
        document.id.validate()?;
        if document.vector.len() != self.dimension {
            return Err(CatalogError::InvalidSchema(format!(
                "vector dimension mismatch: expected {}, got {}",
                self.dimension,
                document.vector.len()
            )));
        }
        self.ensure_text_fields(document)?;
        Ok(())
    }

    fn ensure_text_fields(&self, document: &Document) -> Result<(), CatalogError> {
        let text_fields: Vec<_> = self
            .schema
            .fields
            .iter()
            .filter(|field| matches!(field.field_type, FieldType::Text { .. }))
            .collect();
        if text_fields.is_empty() {
            return Ok(());
        }
        let payload_obj = match &document.payload {
            Some(PayloadValue::Object(map)) => Some(map),
            Some(_) => None,
            None => None,
        };

        for field in text_fields {
            let value = payload_obj.and_then(|map| map.get(&field.name));
            match value {
                Some(PayloadValue::String(_)) => {}
                Some(_) => {
                    return Err(CatalogError::InvalidSchema(format!(
                        "text field {} must be a string",
                        field.name
                    )));
                }
                None if field.required => {
                    return Err(CatalogError::InvalidSchema(format!(
                        "missing required text field {}",
                        field.name
                    )));
                }
                None => {}
            }
        }
        Ok(())
    }

    fn text_field_values(
        &self,
        payload: &Option<PayloadValue>,
    ) -> Result<Vec<String>, CatalogError> {
        if self.text_index.is_none() {
            return Ok(Vec::new());
        }
        let payload_obj = match payload {
            Some(PayloadValue::Object(map)) => Some(map),
            _ => None,
        };

        let mut values = Vec::new();
        for field in self.schema.fields.iter() {
            if let FieldType::Text { indexed } = field.field_type {
                if !indexed {
                    continue;
                }
                if let Some(value) = payload_obj.and_then(|map| map.get(&field.name)) {
                    match value {
                        PayloadValue::String(s) => values.push(s.clone()),
                        _ => {
                            return Err(CatalogError::InvalidSchema(format!(
                                "text field {} must be a string",
                                field.name
                            )))
                        }
                    }
                }
            }
        }
        Ok(values)
    }
}

#[derive(Debug, Default)]
pub struct Catalog {
    collections: HashMap<String, Collection>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_collection(&mut self, schema: CollectionSchema) -> Result<(), CatalogError> {
        if self.collections.contains_key(&schema.name) {
            return Err(CatalogError::CollectionExists(schema.name));
        }
        let collection = Collection::new(schema.clone())?;
        self.collections.insert(schema.name, collection);
        Ok(())
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<(), CatalogError> {
        self.collections
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::CollectionMissing(name.to_string()))
    }

    pub fn collection(&self, name: &str) -> Result<&Collection, CatalogError> {
        self.collections
            .get(name)
            .ok_or_else(|| CatalogError::CollectionMissing(name.to_string()))
    }

    pub fn collection_mut(&mut self, name: &str) -> Result<&mut Collection, CatalogError> {
        self.collections
            .get_mut(name)
            .ok_or_else(|| CatalogError::CollectionMissing(name.to_string()))
    }
}

fn normalize_scores(results: &[SearchResult]) -> HashMap<DocumentId, f32> {
    if results.is_empty() {
        return HashMap::new();
    }
    let mut min_score = f32::MAX;
    let mut max_score = f32::MIN;
    for result in results {
        min_score = min_score.min(result.score);
        max_score = max_score.max(result.score);
    }

    results
        .iter()
        .map(|result| {
            let normalized = if (max_score - min_score).abs() < f32::EPSILON {
                1.0
            } else {
                (result.score - min_score) / (max_score - min_score)
            };
            (result.id.clone(), normalized)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use barq_index::HnswParams;

    fn sample_schema() -> CollectionSchema {
        CollectionSchema {
            name: "products".to_string(),
            fields: vec![FieldSchema {
                name: "vector".to_string(),
                field_type: FieldType::Vector {
                    dimension: 3,
                    metric: DistanceMetric::Cosine,
                    index: None,
                },
                required: true,
            }],
            bm25_config: None,
        }
    }

    fn text_schema() -> CollectionSchema {
        CollectionSchema {
            name: "articles".to_string(),
            fields: vec![
                FieldSchema {
                    name: "vector".to_string(),
                    field_type: FieldType::Vector {
                        dimension: 3,
                        metric: DistanceMetric::Cosine,
                        index: None,
                    },
                    required: true,
                },
                FieldSchema {
                    name: "body".to_string(),
                    field_type: FieldType::Text { indexed: true },
                    required: true,
                },
            ],
            bm25_config: None,
        }
    }

    #[test]
    fn catalog_lifecycle() {
        let mut catalog = Catalog::new();
        catalog.create_collection(sample_schema()).unwrap();
        assert!(catalog.collection("products").is_ok());
        catalog.drop_collection("products").unwrap();
        assert!(catalog.collection("products").is_err());
    }

    #[test]
    fn insert_and_search_document() {
        let mut catalog = Catalog::new();
        catalog.create_collection(sample_schema()).unwrap();
        let collection = catalog.collection_mut("products").unwrap();

        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![0.0, 1.0, 0.5],
                payload: None,
            })
            .unwrap();

        let results = collection.search(&[0.0, 0.9, 0.5], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));
    }

    #[test]
    fn delete_document() {
        let mut catalog = Catalog::new();
        catalog.create_collection(sample_schema()).unwrap();
        let collection = catalog.collection_mut("products").unwrap();

        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![1.0, 0.0, 0.0],
                payload: Some(PayloadValue::String("foo".into())),
            })
            .unwrap();

        assert!(collection.delete(&DocumentId::U64(1)));
        assert!(collection.search(&[1.0, 0.0, 0.0], 1).unwrap().is_empty());
    }

    #[test]
    fn text_search_scores() {
        let mut catalog = Catalog::new();
        catalog.create_collection(text_schema()).unwrap();
        let collection = catalog.collection_mut("articles").unwrap();

        let mut payload1 = HashMap::new();
        payload1.insert(
            "body".to_string(),
            PayloadValue::String("Rust language book".into()),
        );

        let mut payload2 = HashMap::new();
        payload2.insert(
            "body".to_string(),
            PayloadValue::String("Comprehensive guide to Rust".into()),
        );

        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![0.1, 0.2, 0.3],
                payload: Some(PayloadValue::Object(payload1)),
            })
            .unwrap();
        collection
            .insert(Document {
                id: DocumentId::U64(2),
                vector: vec![0.2, 0.3, 0.4],
                payload: Some(PayloadValue::Object(payload2)),
            })
            .unwrap();

        let results = collection.search_text("rust guide", 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, DocumentId::U64(2));
    }

    #[test]
    fn hybrid_includes_both_scores() {
        let mut catalog = Catalog::new();
        catalog.create_collection(text_schema()).unwrap();
        let collection = catalog.collection_mut("articles").unwrap();

        let mut payload1 = HashMap::new();
        payload1.insert(
            "body".to_string(),
            PayloadValue::String("Rust systems programming".into()),
        );
        let mut payload2 = HashMap::new();
        payload2.insert(
            "body".to_string(),
            PayloadValue::String("Guide to databases".into()),
        );

        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![0.0, 1.0, 0.0],
                payload: Some(PayloadValue::Object(payload1)),
            })
            .unwrap();
        collection
            .insert(Document {
                id: DocumentId::U64(2),
                vector: vec![1.0, 0.0, 0.0],
                payload: Some(PayloadValue::Object(payload2)),
            })
            .unwrap();

        let results = collection
            .search_hybrid(&[0.0, 1.0, 0.0], "rust", 2, None)
            .unwrap();
        assert_eq!(results.len(), 2);
        let first = &results[0];
        assert!(first.bm25_score.is_some());
        assert!(first.vector_score.is_some());
    }

    #[test]
    fn rebuilds_index_with_new_type() {
        let mut catalog = Catalog::new();
        catalog.create_collection(sample_schema()).unwrap();
        let collection = catalog.collection_mut("products").unwrap();

        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![0.0, 1.0, 0.0],
                payload: None,
            })
            .unwrap();

        collection
            .rebuild_index(Some(IndexType::Hnsw(HnswParams::default())))
            .unwrap();

        let results = collection.search(&[0.0, 0.9, 0.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));

        let (_, _, configured) = collection.schema().vector_config().unwrap();
        assert!(matches!(configured, IndexType::Hnsw(_)));
    }

    #[test]
    fn bm25_config_is_applied() {
        let mut schema = text_schema();
        schema.bm25_config = Some(Bm25Config { k1: 1.7, b: 0.6 });
        let mut catalog = Catalog::new();
        catalog.create_collection(schema.clone()).unwrap();
        let collection = catalog.collection_mut("articles").unwrap();
        assert_eq!(collection.schema.bm25_config, schema.bm25_config);
        let bm25 = collection.text_index.as_ref().unwrap();
        assert_eq!(bm25.config(), schema.bm25_config.unwrap());
    }
}
