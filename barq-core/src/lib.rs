use barq_bm25::{Bm25Config, Bm25Index, TextIndexError};
pub use barq_index::{
    build_index, DistanceMetric, DocumentId, DocumentIdError, IndexConfig, IndexType, SearchResult,
    VectorIndex, Filter, GeoBoundingBox, GeoPoint, PayloadValue, BatchSearch, score_with_metric,
};
use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("collection {0} already exists")]
    CollectionExists(String),

    #[error("collection {0} not found")]
    CollectionMissing(String),

    #[error("tenant {0} does not exist")]
    TenantMissing(TenantId),

    #[error("tenant mismatch: schema belongs to {schema} but tenant {tenant} was requested")]
    TenantMismatch { tenant: TenantId, schema: TenantId },

    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    #[error("index error: {0}")]
    Index(#[from] barq_index::VectorIndexError),

    #[error("text index error: {0}")]
    TextIndex(#[from] TextIndexError),

    #[error("invalid document id: {0}")]
    DocumentId(#[from] DocumentIdError),

    #[error("invalid filter: {0}")]
    Filter(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self("default".to_string())
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for TenantId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
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
    #[serde(default)]
    pub tenant_id: TenantId,
}




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ValueKey {
    Bool(bool),
    I64(i64),
    F64(u64),
    String(String),
    Timestamp(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum OrderedValue {
    I64(i64),
    F64(u64),
    Timestamp(i64),
}

impl OrderedValue {
    fn from_payload(value: &PayloadValue) -> Option<Self> {
        match value {
            PayloadValue::I64(v) => Some(Self::I64(*v)),
            PayloadValue::F64(v) => Some(Self::F64(v.to_bits())),
            PayloadValue::Timestamp(ts) => Some(Self::Timestamp(ts.timestamp_millis())),
            _ => None,
        }
    }
}

fn value_key(value: &PayloadValue) -> Option<ValueKey> {
    match value {
        PayloadValue::Bool(v) => Some(ValueKey::Bool(*v)),
        PayloadValue::I64(v) => Some(ValueKey::I64(*v)),
        PayloadValue::F64(v) => Some(ValueKey::F64(v.to_bits())),
        PayloadValue::String(v) => Some(ValueKey::String(v.clone())),
        PayloadValue::Timestamp(ts) => Some(ValueKey::Timestamp(ts.timestamp_millis())),
        _ => None,
    }
}

#[derive(Debug, Default, Clone)]
struct FieldIndex {
    presence: HashMap<DocumentId, usize>,
    equality: HashMap<ValueKey, HashSet<DocumentId>>,
    ranges: BTreeMap<OrderedValue, HashSet<DocumentId>>,
    geo: HashMap<DocumentId, GeoPoint>,
}

impl FieldIndex {
    fn touch(&mut self, doc_id: &DocumentId) {
        *self.presence.entry(doc_id.clone()).or_insert(0) += 1;
    }

    fn untouch(&mut self, doc_id: &DocumentId) {
        if let Some(count) = self.presence.get_mut(doc_id) {
            if *count <= 1 {
                self.presence.remove(doc_id);
            } else {
                *count -= 1;
            }
        }
    }

    fn insert(&mut self, doc_id: &DocumentId, value: &PayloadValue) {
        self.touch(doc_id);
        match value {
            PayloadValue::GeoPoint(point) => {
                self.geo.insert(doc_id.clone(), *point);
            }
            other => {
                if let Some(key) = value_key(other) {
                    self.equality.entry(key).or_default().insert(doc_id.clone());
                }
                if let Some(order) = OrderedValue::from_payload(other) {
                    self.ranges.entry(order).or_default().insert(doc_id.clone());
                }
            }
        }
    }

    fn remove(&mut self, doc_id: &DocumentId, value: &PayloadValue) {
        match value {
            PayloadValue::GeoPoint(_) => {
                self.geo.remove(doc_id);
            }
            other => {
                if let Some(key) = value_key(other) {
                    if let Some(set) = self.equality.get_mut(&key) {
                        set.remove(doc_id);
                        if set.is_empty() {
                            self.equality.remove(&key);
                        }
                    }
                }
                if let Some(order) = OrderedValue::from_payload(other) {
                    if let Some(set) = self.ranges.get_mut(&order) {
                        set.remove(doc_id);
                        if set.is_empty() {
                            self.ranges.remove(&order);
                        }
                    }
                }
            }
        }
        self.untouch(doc_id);
    }

    fn is_empty(&self) -> bool {
        self.presence.is_empty()
            && self.equality.is_empty()
            && self.ranges.is_empty()
            && self.geo.is_empty()
    }
}

#[derive(Debug, Default, Clone)]
struct MetadataIndex {
    fields: HashMap<String, FieldIndex>,
}

impl MetadataIndex {
    fn insert_payload(&mut self, doc_id: &DocumentId, payload: &PayloadValue) {
        if let Some(map) = payload.as_object() {
            for (key, value) in map.iter() {
                self.insert_value(doc_id, key, value);
            }
        }
    }

    fn remove_payload(&mut self, doc_id: &DocumentId, payload: &PayloadValue) {
        if let Some(map) = payload.as_object() {
            for (key, value) in map.iter() {
                self.remove_value(doc_id, key, value);
            }
        }
    }

    fn insert_value(&mut self, doc_id: &DocumentId, path: &str, value: &PayloadValue) {
        match value {
            PayloadValue::Object(map) => {
                self.fields
                    .entry(path.to_string())
                    .or_default()
                    .touch(doc_id);
                for (child, child_value) in map.iter() {
                    let nested_path = format!("{}.{}", path, child);
                    self.insert_value(doc_id, &nested_path, child_value);
                }
            }
            PayloadValue::Array(items) => {
                for item in items {
                    self.insert_value(doc_id, path, item);
                }
            }
            other => {
                self.fields
                    .entry(path.to_string())
                    .or_default()
                    .insert(doc_id, other);
            }
        }
    }

    fn remove_value(&mut self, doc_id: &DocumentId, path: &str, value: &PayloadValue) {
        match value {
            PayloadValue::Object(map) => {
                if let Some(index) = self.fields.get_mut(path) {
                    index.untouch(doc_id);
                    if index.is_empty() {
                        self.fields.remove(path);
                    }
                }
                for (child, child_value) in map.iter() {
                    let nested_path = format!("{}.{}", path, child);
                    self.remove_value(doc_id, &nested_path, child_value);
                }
            }
            PayloadValue::Array(items) => {
                for item in items {
                    self.remove_value(doc_id, path, item);
                }
            }
            other => {
                if let Some(index) = self.fields.get_mut(path) {
                    index.remove(doc_id, other);
                    if index.is_empty() {
                        self.fields.remove(path);
                    }
                }
            }
        }
    }

    fn candidates(&self, filter: &Filter) -> Option<HashSet<DocumentId>> {
        match filter {
            Filter::And { filters } => {
                let mut iter = filters.iter().filter_map(|f| self.candidates(f));
                let first = iter.next()?;
                let mut acc = first;
                for set in iter {
                    acc = acc.intersection(&set).cloned().collect();
                }
                Some(acc)
            }
            Filter::Or { filters } => {
                let mut acc: HashSet<DocumentId> = HashSet::new();
                for f in filters {
                    if let Some(set) = self.candidates(f) {
                        acc.extend(set);
                    } else {
                        return None;
                    }
                }
                Some(acc)
            }
            Filter::Not { .. } => None,
            Filter::GeoWithin {
                field,
                bounding_box,
            } => self.geo_candidates(field, bounding_box),
            Filter::Eq { field, value } => self.equality_candidates(field, value),
            Filter::Ne { .. } => None,
            Filter::Gt { field, value: _ }
            | Filter::Gte { field, value: _ }
            | Filter::Lt { field, value: _ }
            | Filter::Lte { field, value: _ } => self.range_candidates(field, filter),
            Filter::In { field, values } => {
                let mut acc: HashSet<DocumentId> = HashSet::new();
                for v in values {
                    if let Some(set) = self.equality_candidates(field, v) {
                        acc.extend(set);
                    }
                }
                if acc.is_empty() {
                    None
                } else {
                    Some(acc)
                }
            }
            Filter::Exists { field } => self.field_exists(field),
        }
    }

    fn equality_candidates(
        &self,
        field: &str,
        value: &PayloadValue,
    ) -> Option<HashSet<DocumentId>> {
        let key = value_key(value)?;
        self.fields
            .get(field)
            .and_then(|idx| idx.equality.get(&key).cloned())
    }

    fn field_exists(&self, field: &str) -> Option<HashSet<DocumentId>> {
        self.fields.get(field).map(|idx| {
            idx.presence
                .keys()
                .cloned()
                .chain(idx.equality.values().flat_map(|set| set.iter().cloned()))
                .chain(idx.geo.keys().cloned())
                .collect()
        })
    }

    fn geo_candidates(
        &self,
        field: &str,
        bounding_box: &GeoBoundingBox,
    ) -> Option<HashSet<DocumentId>> {
        let idx = self.fields.get(field)?;
        let mut set = HashSet::new();
        for (doc, point) in idx.geo.iter() {
            if point.lat <= bounding_box.top_left.lat
                && point.lat >= bounding_box.bottom_right.lat
                && point.lon >= bounding_box.top_left.lon
                && point.lon <= bounding_box.bottom_right.lon
            {
                set.insert(doc.clone());
            }
        }
        Some(set)
    }

    fn range_candidates(&self, field: &str, filter: &Filter) -> Option<HashSet<DocumentId>> {
        let idx = self.fields.get(field)?;
        let (start, end, inclusive_start, inclusive_end) = match filter {
            Filter::Gt { value, .. } => (OrderedValue::from_payload(value), None, false, false),
            Filter::Gte { value, .. } => (OrderedValue::from_payload(value), None, true, false),
            Filter::Lt { value, .. } => (None, OrderedValue::from_payload(value), false, false),
            Filter::Lte { value, .. } => (None, OrderedValue::from_payload(value), false, true),
            _ => return None,
        };
        if matches!(filter, Filter::Gt { .. } | Filter::Gte { .. }) && start.is_none() {
            return None;
        }
        if matches!(filter, Filter::Lt { .. } | Filter::Lte { .. }) && end.is_none() {
            return None;
        }
        let mut set: HashSet<DocumentId> = HashSet::new();
        for (key, docs) in idx.ranges.iter() {
            let lower_ok = match &start {
                Some(bound) => {
                    if inclusive_start {
                        key >= bound
                    } else {
                        key > bound
                    }
                }
                None => true,
            };
            let upper_ok = match &end {
                Some(bound) => {
                    if inclusive_end {
                        key <= bound
                    } else {
                        key < bound
                    }
                }
                None => true,
            };
            if lower_ok && upper_ok {
                set.extend(docs.iter().cloned());
            }
        }
        if set.is_empty() {
            None
        } else {
            Some(set)
        }
    }
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
        if self.name.trim().is_empty() {
            return Err(CatalogError::InvalidSchema(
                "collection name cannot be empty".to_string(),
            ));
        }

        let mut seen_fields = HashSet::new();
        for field in &self.fields {
            if field.name.trim().is_empty() {
                return Err(CatalogError::InvalidSchema(
                    "field name cannot be empty".to_string(),
                ));
            }
            if !seen_fields.insert(field.name.clone()) {
                return Err(CatalogError::InvalidSchema(format!(
                    "duplicate field name: {}",
                    field.name
                )));
            }
        }

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
    metadata_index: MetadataIndex,
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
            metadata_index: MetadataIndex::default(),
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
            self.metadata_index.insert_payload(&document.id, &payload);
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
            if let Some(existing) = self.payloads.remove(&document.id) {
                self.metadata_index.remove_payload(&document.id, &existing);
            }
        }
        self.insert(document)
    }

    pub fn document_count(&self) -> usize {
        self.vectors.len()
    }

    pub fn document_footprint(&self, id: &DocumentId) -> Option<usize> {
        let vector_bytes = self
            .vectors
            .get(id)
            .map(|v| v.len() * std::mem::size_of::<f32>());
        let payload_bytes = self
            .payloads
            .get(id)
            .and_then(|payload| serde_json::to_vec(payload).ok().map(|bytes| bytes.len()));
        match (vector_bytes, payload_bytes) {
            (Some(v), Some(p)) => Some(v + p),
            (Some(v), None) => Some(v),
            _ => None,
        }
    }

    pub fn total_footprint(&self) -> (usize, usize) {
        let mut bytes = 0;
        for (id, vector) in &self.vectors {
            bytes += vector.len() * std::mem::size_of::<f32>();
            if let Some(payload) = self.payloads.get(id) {
                if let Ok(encoded) = serde_json::to_vec(payload) {
                    bytes += encoded.len();
                }
            }
        }
        (self.document_count(), bytes)
    }

    pub fn delete(&mut self, id: &DocumentId) -> bool {
        let removed = self.index.remove(id);
        self.vectors.remove(id);
        if let Some(payload) = self.payloads.remove(id) {
            self.metadata_index.remove_payload(id, &payload);
        }
        if let Some(index) = &mut self.text_index {
            index.remove(id);
        }
        removed.is_some()
    }

    pub fn get(&self, id: &DocumentId) -> Option<Document> {
        let vector = self.vectors.get(id)?.clone();
        let payload = self.payloads.get(id).cloned();
        Some(Document {
            id: id.clone(),
            vector,
            payload,
        })
    }

    pub fn search(&self, vector: &[f32], top_k: usize) -> Result<Vec<SearchResult>, CatalogError> {
        self.search_with_filter(vector, top_k, None)
    }

    pub fn search_with_filter(
        &self,
        vector: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<SearchResult>, CatalogError> {
        if let Some(f) = filter {
            self.validate_filter(f)?;
        }
        let candidates = filter.and_then(|f| self.metadata_index.candidates(f));
        
        let mut results = if let Some(ids) = &candidates {
            self.search_over_candidates(vector, ids)?
        } else {
            self.index.search(vector, top_k * 2)?
        };

        results = self.filter_results(results, filter);
        if results.len() < top_k {
           // Simple fallback strategy (could be improved with FilteredVectorSearch in future)
           let search_k = if candidates.is_some() { top_k * 10 } else { top_k * 4 };
           let fallback = self.index.search(vector, search_k)?;
           let filtered_fallback = self.filter_results(fallback, filter);
           results.extend(filtered_fallback);
        }
        
        results.par_sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.dedup_by(|a, b| a.id == b.id);
        results.truncate(top_k.min(results.len()));
        Ok(results)
    }

    pub fn batch_search(
        &self,
        queries: &[(Vec<f32>, Option<Filter>)],
        top_k: usize,
    ) -> Result<Vec<Vec<SearchResult>>, CatalogError> {
        let batch_search = BatchSearch::new(&*self.index);
        
        let candidates_provider = |filter: &Filter| -> Option<Vec<DocumentId>> {
            self.metadata_index.candidates(filter).map(|set| set.into_iter().collect())
        };

        let check_provider = |id: &DocumentId, filter: &Filter| -> bool {
             if let Some(payload) = self.payloads.get(id) {
                 filter.matches(payload)
             } else {
                 false
             }
        };
        
        let match_scorer = |id: &DocumentId, query: &[f32]| -> Option<f32> {
             if let Some(vec) = self.vectors.get(id) {
                 Some(score_with_metric(self.metric, vec, query))
             } else {
                 None
             }
        };
        
        Ok(batch_search.search_filtered(
            queries, 
            top_k, 
            &match_scorer, 
            &candidates_provider, 
            &check_provider
        )?)
    }


    fn search_over_candidates(
        &self,
        vector: &[f32],
        candidates: &HashSet<DocumentId>,
    ) -> Result<Vec<SearchResult>, CatalogError> {
        let mut scored: Vec<SearchResult> = candidates
            .par_iter()
            .filter_map(|id| self.vectors.get(id).map(|vec| (id, vec)))
            .map(|(id, vec)| SearchResult {
                id: id.clone(),
                score: score_with_metric(self.metric, vector, vec),
            })
            .collect();
        scored.par_sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(scored)
    }

    fn filter_results(
        &self,
        mut results: Vec<SearchResult>,
        filter: Option<&Filter>,
    ) -> Vec<SearchResult> {
        if let Some(f) = filter {
            results.retain(|res| self.matches_filter(&res.id, f));
        }
        results
    }

    fn matches_filter(&self, id: &DocumentId, filter: &Filter) -> bool {
        let payload = self.payloads.get(id);
        evaluate_filter(filter, payload)
    }

    pub fn search_text(
        &self,
        query: &str,
        top_k: usize,
    ) -> Result<Vec<SearchResult>, CatalogError> {
        self.search_text_with_filter(query, top_k, None)
    }

    pub fn search_text_with_filter(
        &self,
        query: &str,
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<SearchResult>, CatalogError> {
        if let Some(f) = filter {
            self.validate_filter(f)?;
        }
        let index = self
            .text_index
            .as_ref()
            .ok_or_else(|| CatalogError::InvalidSchema("collection has no text index".into()))?;
        let mut results = index.search(query, top_k * 2)?;
        results = self.filter_results(results, filter);
        if results.len() < top_k {
            let mut fallback = index.search(query, top_k * 4)?;
            fallback = self.filter_results(fallback, filter);
            results.extend(fallback);
        }
        results.par_sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.dedup_by(|a, b| a.id == b.id);
        results.truncate(top_k.min(results.len()));
        Ok(results)
    }

    pub fn search_hybrid(
        &self,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<HybridWeights>,
        filter: Option<&Filter>,
    ) -> Result<Vec<HybridSearchResult>, CatalogError> {
        if let Some(f) = filter {
            self.validate_filter(f)?;
        }
        let weights = weights.unwrap_or_default();
        self.text_index
            .as_ref()
            .ok_or_else(|| CatalogError::InvalidSchema("collection has no text index".into()))?;

        if top_k == 0 {
            return Err(CatalogError::InvalidSchema(
                "top_k must be positive".to_string(),
            ));
        }

        let (bm25_results, vector_results) = rayon::join(
            || self.search_text_with_filter(query, top_k * 2, filter),
            || self.search_with_filter(vector, top_k * 2, filter),
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
        let results = self.search_hybrid(vector, query, top_k, weights, None)?;
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

    fn validate_filter(&self, filter: &Filter) -> Result<(), CatalogError> {
        match filter {
            Filter::And { filters } | Filter::Or { filters } => {
                for f in filters {
                    self.validate_filter(f)?;
                }
            }
            Filter::Not { filter } => self.validate_filter(filter)?,
            Filter::Eq { field, .. }
            | Filter::Ne { field, .. }
            | Filter::Gt { field, .. }
            | Filter::Gte { field, .. }
            | Filter::Lt { field, .. }
            | Filter::Lte { field, .. }
            | Filter::In { field, .. }
            | Filter::GeoWithin { field, .. }
            | Filter::Exists { field } => {
                let valid = field
                    .split('.')
                    .next()
                    .and_then(|root| self.schema.fields.iter().find(|f| f.name == root))
                    .is_some();
                if !valid {
                    return Err(CatalogError::Filter(format!(
                        "field {} not in schema",
                        field
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Catalog {
    collections: HashMap<TenantId, HashMap<String, Collection>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_collection(
        &mut self,
        tenant: TenantId,
        schema: CollectionSchema,
    ) -> Result<(), CatalogError> {
        if schema.tenant_id != tenant {
            return Err(CatalogError::TenantMismatch {
                tenant,
                schema: schema.tenant_id,
            });
        }
        let collections = self
            .collections
            .entry(schema.tenant_id.clone())
            .or_default();
        if collections.contains_key(&schema.name) {
            return Err(CatalogError::CollectionExists(schema.name));
        }
        let collection = Collection::new(schema.clone())?;
        collections.insert(schema.name, collection);
        Ok(())
    }

    pub fn drop_collection(&mut self, tenant: &TenantId, name: &str) -> Result<(), CatalogError> {
        self.collections
            .get_mut(tenant)
            .ok_or_else(|| CatalogError::TenantMissing(tenant.clone()))?
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::CollectionMissing(name.to_string()))
    }

    pub fn collection(&self, tenant: &TenantId, name: &str) -> Result<&Collection, CatalogError> {
        self.collections
            .get(tenant)
            .ok_or_else(|| CatalogError::TenantMissing(tenant.clone()))?
            .get(name)
            .ok_or_else(|| CatalogError::CollectionMissing(name.to_string()))
    }

    pub fn collection_mut(
        &mut self,
        tenant: &TenantId,
        name: &str,
    ) -> Result<&mut Collection, CatalogError> {
        self.collections
            .get_mut(tenant)
            .ok_or_else(|| CatalogError::TenantMissing(tenant.clone()))?
            .get_mut(name)
            .ok_or_else(|| CatalogError::CollectionMissing(name.to_string()))
    }

    pub fn collection_names(&self, tenant: &TenantId) -> Vec<String> {
        self.collections
            .get(tenant)
            .map(|c| c.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn tenants(&self) -> impl Iterator<Item = (&TenantId, &HashMap<String, Collection>)> {
        self.collections.iter()
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

    fn default_tenant() -> TenantId {
        TenantId::default()
    }

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
            tenant_id: TenantId::default(),
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
            tenant_id: TenantId::default(),
        }
    }

    fn json_schema() -> CollectionSchema {
        CollectionSchema {
            name: "products_meta".to_string(),
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
                    name: "attrs".to_string(),
                    field_type: FieldType::Json,
                    required: false,
                },
                FieldSchema {
                    name: "tags".to_string(),
                    field_type: FieldType::Json,
                    required: false,
                },
            ],
            bm25_config: None,
            tenant_id: TenantId::default(),
        }
    }

    #[test]
    fn collection_validation_rejects_empty_names() {
        let mut schema = sample_schema();
        schema.name = "   ".to_string();
        let err = schema.validate().expect_err("expected validation failure");
        assert!(matches!(err, CatalogError::InvalidSchema(msg) if msg.contains("collection name")));

        let mut schema = sample_schema();
        schema.fields[0].name = "".to_string();
        let err = schema.validate().expect_err("expected validation failure");
        assert!(matches!(err, CatalogError::InvalidSchema(msg) if msg.contains("field name")));
    }

    #[test]
    fn collection_validation_rejects_duplicate_fields() {
        let mut schema = json_schema();
        schema.fields.push(FieldSchema {
            name: "attrs".to_string(),
            field_type: FieldType::Json,
            required: false,
        });
        let err = schema.validate().expect_err("expected validation failure");
        assert!(
            matches!(err, CatalogError::InvalidSchema(msg) if msg.contains("duplicate field name"))
        );
    }

    #[test]
    fn catalog_lifecycle() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        assert!(catalog.collection(&tenant, "products").is_ok());
        catalog.drop_collection(&tenant, "products").unwrap();
        assert!(catalog.collection(&tenant, "products").is_err());
    }

    #[test]
    fn insert_and_search_document() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "products").unwrap();

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
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "products").unwrap();

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
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), text_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "articles").unwrap();

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
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), text_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "articles").unwrap();

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
            .search_hybrid(&[0.0, 1.0, 0.0], "rust", 2, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
        let first = &results[0];
        assert!(first.bm25_score.is_some());
        assert!(first.vector_score.is_some());
    }

    #[test]
    fn rebuilds_index_with_new_type() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "products").unwrap();

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
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), schema.clone())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "articles").unwrap();
        assert_eq!(collection.schema.bm25_config, schema.bm25_config);
        let bm25 = collection.text_index.as_ref().unwrap();
        assert_eq!(bm25.config(), schema.bm25_config.unwrap());
    }

    #[test]
    fn metadata_index_tracks_nested_fields() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), json_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "products_meta").unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("category".to_string(), PayloadValue::String("tech".into()));
        let mut dimensions = HashMap::new();
        dimensions.insert("length".to_string(), PayloadValue::I64(10));
        attrs.insert("dimensions".to_string(), PayloadValue::Object(dimensions));

        let mut payload1 = HashMap::new();
        payload1.insert("attrs".to_string(), PayloadValue::Object(attrs.clone()));
        payload1.insert(
            "tags".to_string(),
            PayloadValue::Array(vec![
                PayloadValue::String("rust".into()),
                PayloadValue::String("systems".into()),
            ]),
        );

        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![0.0, 1.0, 0.0],
                payload: Some(PayloadValue::Object(payload1.clone())),
            })
            .unwrap();

        let mut payload2 = HashMap::new();
        payload2.insert(
            "attrs".to_string(),
            PayloadValue::Object({
                let mut other_attrs = HashMap::new();
                other_attrs.insert("category".to_string(), PayloadValue::String("home".into()));
                other_attrs
            }),
        );
        payload2.insert(
            "tags".to_string(),
            PayloadValue::Array(vec![PayloadValue::String("decor".into())]),
        );

        collection
            .insert(Document {
                id: DocumentId::U64(2),
                vector: vec![0.0, 0.5, 1.0],
                payload: Some(PayloadValue::Object(payload2)),
            })
            .unwrap();

        let tech_candidates = collection
            .metadata_index
            .candidates(&Filter::Eq {
                field: "attrs.category".to_string(),
                value: PayloadValue::String("tech".into()),
            })
            .unwrap();
        assert!(tech_candidates.contains(&DocumentId::U64(1)));
        assert!(!tech_candidates.contains(&DocumentId::U64(2)));

        let tag_candidates = collection
            .metadata_index
            .candidates(&Filter::Eq {
                field: "tags".to_string(),
                value: PayloadValue::String("rust".into()),
            })
            .unwrap();
        assert_eq!(tag_candidates.len(), 1);
        assert!(tag_candidates.contains(&DocumentId::U64(1)));

        let dimension_exists = collection
            .metadata_index
            .candidates(&Filter::Exists {
                field: "attrs.dimensions.length".to_string(),
            })
            .unwrap();
        assert!(dimension_exists.contains(&DocumentId::U64(1)));
        assert!(!dimension_exists.contains(&DocumentId::U64(2)));

        let mut updated_attrs = attrs.clone();
        updated_attrs.insert(
            "category".to_string(),
            PayloadValue::String("kitchen".into()),
        );
        let mut updated_payload = payload1.clone();
        updated_payload.insert("attrs".to_string(), PayloadValue::Object(updated_attrs));

        collection
            .upsert(Document {
                id: DocumentId::U64(1),
                vector: vec![0.0, 1.0, 0.0],
                payload: Some(PayloadValue::Object(updated_payload)),
            })
            .unwrap();

        let refreshed_candidates = collection
            .metadata_index
            .candidates(&Filter::Eq {
                field: "attrs.category".to_string(),
                value: PayloadValue::String("tech".into()),
            })
            .unwrap_or_default();
        assert!(!refreshed_candidates.contains(&DocumentId::U64(1)));
    }

    #[test]
    fn hybrid_search_respects_metadata_filters() {
        let mut schema = text_schema();
        schema.name = "articles_with_meta".to_string();
        schema.fields.push(FieldSchema {
            name: "meta".to_string(),
            field_type: FieldType::Json,
            required: false,
        });

        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog.create_collection(tenant.clone(), schema).unwrap();
        let collection = catalog
            .collection_mut(&tenant, "articles_with_meta")
            .unwrap();

        let mut payload1 = HashMap::new();
        payload1.insert(
            "body".to_string(),
            PayloadValue::String("Rust language guide".into()),
        );
        payload1.insert(
            "meta".to_string(),
            PayloadValue::Object({
                let mut meta = HashMap::new();
                meta.insert("category".to_string(), PayloadValue::String("tech".into()));
                meta
            }),
        );

        let mut payload2 = HashMap::new();
        payload2.insert(
            "body".to_string(),
            PayloadValue::String("Cooking tips and recipes".into()),
        );
        payload2.insert(
            "meta".to_string(),
            PayloadValue::Object({
                let mut meta = HashMap::new();
                meta.insert(
                    "category".to_string(),
                    PayloadValue::String("lifestyle".into()),
                );
                meta
            }),
        );

        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![0.9, 0.1, 0.0],
                payload: Some(PayloadValue::Object(payload1)),
            })
            .unwrap();

        collection
            .insert(Document {
                id: DocumentId::U64(2),
                vector: vec![0.1, 0.9, 0.0],
                payload: Some(PayloadValue::Object(payload2)),
            })
            .unwrap();

        let results = collection
            .search_hybrid(
                &[1.0, 0.0, 0.0],
                "rust language",
                2,
                None,
                Some(&Filter::Eq {
                    field: "meta.category".to_string(),
                    value: PayloadValue::String("tech".into()),
                }),
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));
    }
}

fn evaluate_filter(filter: &Filter, payload: Option<&PayloadValue>) -> bool {
    match filter {
        Filter::And { filters } => filters.iter().all(|f| evaluate_filter(f, payload)),
        Filter::Or { filters } => filters.iter().any(|f| evaluate_filter(f, payload)),
        Filter::Not { filter } => !evaluate_filter(filter, payload),
        Filter::Eq { field, value } => field_values(payload, field)
            .iter()
            .any(|candidate| *candidate == value),
        Filter::Ne { field, value } => field_values(payload, field)
            .iter()
            .all(|candidate| *candidate != value),
        Filter::Gt { field, value } => compare_field(payload, field, value, Ordering::Greater),
        Filter::Gte { field, value } => {
            compare_field(payload, field, value, Ordering::Greater)
                || field_values(payload, field)
                    .iter()
                    .any(|candidate| *candidate == value)
        }
        Filter::Lt { field, value } => compare_field(payload, field, value, Ordering::Less),
        Filter::Lte { field, value } => {
            compare_field(payload, field, value, Ordering::Less)
                || field_values(payload, field)
                    .iter()
                    .any(|candidate| *candidate == value)
        }
        Filter::In { field, values } => field_values(payload, field)
            .iter()
            .any(|candidate| values.iter().any(|v| v == *candidate)),
        Filter::GeoWithin {
            field,
            bounding_box,
        } => field_values(payload, field)
            .iter()
            .any(|candidate| match candidate {
                PayloadValue::GeoPoint(point) => {
                    point.lat <= bounding_box.top_left.lat
                        && point.lat >= bounding_box.bottom_right.lat
                        && point.lon >= bounding_box.top_left.lon
                        && point.lon <= bounding_box.bottom_right.lon
                }
                _ => false,
            }),
        Filter::Exists { field } => !field_values(payload, field).is_empty(),
    }
}

fn field_values<'a>(payload: Option<&'a PayloadValue>, field: &str) -> Vec<&'a PayloadValue> {
    let mut result = Vec::new();
    let parts: Vec<&str> = field.split('.').collect();
    if let Some(value) = payload {
        collect_field_values(value, &parts, &mut result);
    }
    result
}

fn collect_field_values<'a>(
    value: &'a PayloadValue,
    path: &[&str],
    output: &mut Vec<&'a PayloadValue>,
) {
    if path.is_empty() {
        output.push(value);
        return;
    }
    match value {
        PayloadValue::Object(map) => {
            if let Some(next) = map.get(path[0]) {
                collect_field_values(next, &path[1..], output);
            }
        }
        PayloadValue::Array(items) => {
            for item in items {
                collect_field_values(item, path, output);
            }
        }
        _ => {}
    }
}

fn compare_field(
    payload: Option<&PayloadValue>,
    field: &str,
    target: &PayloadValue,
    ordering: Ordering,
) -> bool {
    field_values(payload, field)
        .iter()
        .any(|candidate| compare_values(candidate, target, ordering))
}

fn compare_values(lhs: &PayloadValue, rhs: &PayloadValue, desired: Ordering) -> bool {
    match (lhs, rhs) {
        (PayloadValue::I64(a), PayloadValue::I64(b)) => a.cmp(b) == desired,
        (PayloadValue::F64(a), PayloadValue::F64(b)) => a.partial_cmp(b) == Some(desired),
        (PayloadValue::Timestamp(a), PayloadValue::Timestamp(b)) => {
            a.timestamp_millis().cmp(&b.timestamp_millis()) == desired
        }
        (PayloadValue::I64(a), PayloadValue::F64(b)) => (*a as f64).partial_cmp(b) == Some(desired),
        (PayloadValue::F64(a), PayloadValue::I64(b)) => {
            a.partial_cmp(&(*b as f64)) == Some(desired)
        }
        _ => false,
    }
}


