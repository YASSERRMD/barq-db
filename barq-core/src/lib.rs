use crate::storage::vector_store::{BudgetedVectorStore, VectorStore, VectorStoreConfig};
use barq_bm25::{Bm25Config, Bm25Index, TextIndexError};
pub use barq_index::{
    build_index, score_with_metric, BatchSearch, DistanceMetric, DocumentId, DocumentIdError,
    Filter, GeoBoundingBox, GeoPoint, IndexConfig, IndexType, PayloadValue, SearchResult,
    VectorIndex,
};
use chrono::{DateTime, Utc};
pub mod storage;

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

/// Lifecycle state for a collection's vector index.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum IndexState {
    /// Rebuild is in progress and search must use fallback execution.
    Building,
    /// Index is current and can serve optimized search.
    #[default]
    Ready,
    /// Data changed since the last completed build and the index is no longer current.
    Stale,
}

impl IndexState {
    /// Returns true when transitioning from `self` to `next` is permitted.
    pub fn can_transition_to(self, next: IndexState) -> bool {
        match (self, next) {
            (IndexState::Ready, IndexState::Building)
            | (IndexState::Ready, IndexState::Stale)
            | (IndexState::Building, IndexState::Ready)
            | (IndexState::Building, IndexState::Stale)
            | (IndexState::Stale, IndexState::Building) => true,
            (current, target) => current == target,
        }
    }

    /// Validates a state transition and returns the target state on success.
    pub fn transition_to(self, next: IndexState) -> Result<IndexState, CatalogError> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(CatalogError::InvalidSchema(format!(
                "invalid index state transition: {self:?} -> {next:?}"
            )))
        }
    }
}

/// Immutable input required to rebuild a collection index off the main thread.
#[derive(Debug, Clone)]
pub struct IndexBuildSnapshot {
    pub metric: DistanceMetric,
    pub dimension: usize,
    pub index_type: IndexType,
    pub vectors: Vec<(DocumentId, Vec<f32>)>,
}

impl IndexBuildSnapshot {
    /// Builds a fresh vector index from the captured snapshot.
    pub fn build(&self) -> Result<Box<dyn VectorIndex>, CatalogError> {
        let mut index = build_index(IndexConfig::new(
            self.metric,
            self.dimension,
            self.index_type.clone(),
        ));
        for (id, vector) in &self.vectors {
            index.insert(id.clone(), vector.clone())?;
        }
        Ok(index)
    }
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

/// High-level shape for a planned query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryVariant {
    /// Vector similarity search without a metadata filter.
    VectorOnly,
    /// Text search without a metadata filter.
    TextOnly,
    /// Hybrid search that blends text and vector scoring.
    Hybrid,
    /// Vector similarity search restricted by a metadata filter.
    FilteredVector,
    /// Text search restricted by a metadata filter.
    FilteredText,
    /// Hybrid search that blends text and vector scoring with a metadata filter.
    FilteredHybrid,
}

impl QueryVariant {
    /// Returns whether the planned query reads vector scores.
    pub fn uses_vector(self) -> bool {
        matches!(
            self,
            Self::VectorOnly | Self::Hybrid | Self::FilteredVector | Self::FilteredHybrid
        )
    }

    /// Returns whether the planned query reads text scores.
    pub fn uses_text(self) -> bool {
        matches!(
            self,
            Self::TextOnly | Self::Hybrid | Self::FilteredText | Self::FilteredHybrid
        )
    }

    /// Returns whether the planned query carries a metadata filter.
    pub fn has_filter(self) -> bool {
        matches!(
            self,
            Self::FilteredVector | Self::FilteredText | Self::FilteredHybrid
        )
    }
}

/// Planned query shape and validation output for collection search execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryPlan {
    variant: QueryVariant,
    top_k: usize,
    vector_path: Option<QueryExecutionPath>,
    text_path: Option<QueryExecutionPath>,
}

impl QueryPlan {
    /// Creates a query plan from the provided vector/text inputs and optional filter.
    pub fn new(
        vector: Option<&[f32]>,
        text: Option<&str>,
        filter: Option<&Filter>,
        top_k: usize,
    ) -> Result<Self, CatalogError> {
        if top_k == 0 {
            return Err(CatalogError::InvalidSchema(
                "top_k must be positive".to_string(),
            ));
        }

        let has_text = text.map(|value| !value.trim().is_empty()).unwrap_or(false);
        let has_vector = match vector {
            Some(values) if values.is_empty() && !has_text => {
                return Err(CatalogError::InvalidSchema(
                    "vector query cannot be empty".to_string(),
                ))
            }
            Some(values) => !values.is_empty(),
            None => false,
        };

        let variant = match (has_vector, has_text, filter.is_some()) {
            (true, false, false) => QueryVariant::VectorOnly,
            (false, true, false) => QueryVariant::TextOnly,
            (true, true, false) => QueryVariant::Hybrid,
            (true, false, true) => QueryVariant::FilteredVector,
            (false, true, true) => QueryVariant::FilteredText,
            (true, true, true) => QueryVariant::FilteredHybrid,
            (false, false, _) => {
                return Err(CatalogError::InvalidSchema(
                    "query plan requires vector and/or text input".to_string(),
                ))
            }
        };

        Ok(Self {
            variant,
            top_k,
            vector_path: None,
            text_path: None,
        })
    }

    /// Returns the planned high-level query variant.
    pub fn variant(self) -> QueryVariant {
        self.variant
    }

    /// Returns the requested top-k for the plan.
    pub fn top_k(self) -> usize {
        self.top_k
    }

    /// Returns whether the plan reads vector scores.
    pub fn uses_vector(self) -> bool {
        self.variant.uses_vector()
    }

    /// Returns whether the plan reads text scores.
    pub fn uses_text(self) -> bool {
        self.variant.uses_text()
    }

    /// Returns whether the plan applies a metadata filter.
    pub fn has_filter(self) -> bool {
        self.variant.has_filter()
    }

    /// Returns the chosen vector execution path, when vector search is part of the plan.
    pub fn vector_path(self) -> Option<QueryExecutionPath> {
        self.vector_path
    }

    /// Returns the chosen text execution path, when text search is part of the plan.
    pub fn text_path(self) -> Option<QueryExecutionPath> {
        self.text_path
    }

    fn with_paths(
        mut self,
        vector_path: Option<QueryExecutionPath>,
        text_path: Option<QueryExecutionPath>,
    ) -> Self {
        self.vector_path = vector_path;
        self.text_path = text_path;
        self
    }
}

/// Concrete execution path chosen for a planned query branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryExecutionPath {
    /// Use the vector index directly and post-filter if needed.
    VectorIndex,
    /// Score only the filter-selected candidate vectors.
    VectorFilterScan,
    /// Score every vector because the vector index is not ready.
    VectorFullScan,
    /// Use the BM25 index directly and post-filter if needed.
    TextIndex,
    /// Score only the filter-selected candidate text documents.
    TextFilterScan,
}

/// Heuristic knobs for the rule-based query planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryPlannerOptions {
    filter_candidate_threshold: usize,
}

impl Default for QueryPlannerOptions {
    fn default() -> Self {
        Self {
            filter_candidate_threshold: 64,
        }
    }
}

impl QueryPlannerOptions {
    /// Returns the configured filter candidate threshold.
    pub fn filter_candidate_threshold(self) -> usize {
        self.filter_candidate_threshold
    }

    /// Overrides the candidate threshold used for filter pushdown planning.
    pub fn with_filter_candidate_threshold(mut self, threshold: usize) -> Self {
        self.filter_candidate_threshold = threshold.max(1);
        self
    }
}

/// Rule-based planner that chooses stable execution paths for query branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryPlanner {
    options: QueryPlannerOptions,
}

impl QueryPlanner {
    /// Creates a planner with the provided options.
    pub fn new(options: QueryPlannerOptions) -> Self {
        Self { options }
    }

    /// Returns the planner options.
    pub fn options(self) -> QueryPlannerOptions {
        self.options
    }
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
    index_state: IndexState,
    vectors: HashMap<DocumentId, Vec<f32>>,
    vector_ids: HashSet<DocumentId>,
    vector_store: Option<BudgetedVectorStore>,
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
            .field("vectors", &self.vector_ids.len())
            .field("payloads", &self.payloads.len())
            .field("index_state", &self.index_state)
            .field("vector_store", &self.vector_store.is_some())
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

        let vector_store =
            if std::env::var("BARQ_VECTOR_STORE_MODE").ok().as_deref() == Some("mmap") {
                let base = std::env::var("BARQ_VECTOR_STORE_PATH")
                    .unwrap_or_else(|_| std::env::temp_dir().display().to_string());
                let file_path =
                    std::path::Path::new(&base).join(format!("{}.vectors.bin", schema.name));
                BudgetedVectorStore::open(
                    file_path,
                    Some(dimension),
                    true,
                    VectorStoreConfig::default(),
                )
                .ok()
            } else {
                None
            };

        let mut collection = Self {
            schema,
            index: build_index(IndexConfig::new(metric, dimension, index_type.clone())),
            index_state: IndexState::Ready,
            vectors: HashMap::new(),
            vector_ids: HashSet::new(),
            vector_store,
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
        };

        if let Some(store) = &collection.vector_store {
            let mut ids = Vec::new();
            store.for_each_id(&mut |id| ids.push(id));
            for id in ids {
                if let Some(vector) = store.get(id) {
                    let doc_id = DocumentId::U64(id);
                    collection.index.insert(doc_id.clone(), vector.to_vec())?;
                    collection.vector_ids.insert(doc_id);
                }
            }
        }

        Ok(collection)
    }

    pub fn insert(&mut self, document: Document) -> Result<(), CatalogError> {
        self.validate_document(&document)?;
        let text_values = self.text_field_values(&document.payload)?;
        self.index
            .insert(document.id.clone(), document.vector.clone())?;
        let mut stored_in_vector_store = false;
        if let (Some(store), DocumentId::U64(id)) = (&mut self.vector_store, &document.id) {
            stored_in_vector_store = true;
            store.insert(*id, &document.vector).map_err(|e| {
                CatalogError::InvalidSchema(format!("vector store insert failed: {e}"))
            })?;
        }
        if !stored_in_vector_store {
            self.vectors
                .insert(document.id.clone(), document.vector.clone());
        }
        self.vector_ids.insert(document.id.clone());
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
            self.vector_ids.remove(&document.id);
            if let (Some(store), DocumentId::U64(id)) = (&mut self.vector_store, &document.id) {
                store.delete(*id);
            }
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
        self.vector_ids.len()
    }

    pub fn document_footprint(&self, id: &DocumentId) -> Option<usize> {
        let vector_bytes = self
            .vector_slice(id)
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
        for id in &self.vector_ids {
            if let Some(vector) = self.vector_slice(id) {
                bytes += vector.len() * std::mem::size_of::<f32>();
            }
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
        self.vector_ids.remove(id);
        if let (Some(store), DocumentId::U64(raw_id)) = (&mut self.vector_store, id) {
            store.delete(*raw_id);
        }
        if let Some(payload) = self.payloads.remove(id) {
            self.metadata_index.remove_payload(id, &payload);
        }
        if let Some(index) = &mut self.text_index {
            index.remove(id);
        }
        removed.is_some()
    }

    pub fn get(&self, id: &DocumentId) -> Option<Document> {
        let vector = self.vector_slice(id)?.to_vec();
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

    /// Searches the collection with an optional metadata filter.
    ///
    /// When the vector index is `Building` or `Stale`, this falls back to
    /// brute-force scoring over the stored vectors so reads remain correct while
    /// rebuilds are in flight.
    pub fn search_with_filter(
        &self,
        vector: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<SearchResult>, CatalogError> {
        let plan = QueryPlanner::default().plan_collection(self, Some(vector), None, filter, top_k)?;
        let top_k = plan.top_k();
        if let Some(f) = filter {
            self.validate_filter(f)?;
        }
        let candidates = self.filter_candidates(filter);
        let results = match plan.vector_path() {
            Some(QueryExecutionPath::VectorFilterScan) => {
                self.search_over_candidates(vector, candidates.as_ref().expect("candidate set"))?
            }
            Some(QueryExecutionPath::VectorFullScan) => {
                // During index builds or stale windows we preserve correctness by scoring every vector.
                self.search_all_vectors(vector)
            }
            Some(QueryExecutionPath::VectorIndex) => self.index.search(vector, top_k * 2)?,
            path => panic!("unexpected vector execution path: {path:?}"),
        };

        let mut sources = vec![self.filter_results(results, filter)];
        if sources[0].len() < top_k
            && matches!(plan.vector_path(), Some(QueryExecutionPath::VectorIndex))
        {
            // Simple fallback strategy (could be improved with FilteredVectorSearch in future)
            let search_k = if candidates.is_some() {
                top_k * 10
            } else {
                top_k * 4
            };
            let fallback = self.index.search(vector, search_k)?;
            let filtered_fallback = self.filter_results(fallback, filter);
            sources.push(filtered_fallback);
        }
        Ok(merge_search_result_sources(sources, top_k))
    }

    pub fn batch_search(
        &self,
        queries: &[(Vec<f32>, Option<Filter>)],
        top_k: usize,
    ) -> Result<Vec<Vec<SearchResult>>, CatalogError> {
        let batch_search = BatchSearch::new(&*self.index);

        let candidates_provider = |filter: &Filter| -> Option<Vec<DocumentId>> {
            self.metadata_index
                .candidates(filter)
                .map(|set| set.into_iter().collect())
        };

        let check_provider = |id: &DocumentId, filter: &Filter| -> bool {
            if let Some(payload) = self.payloads.get(id) {
                filter.matches(payload)
            } else {
                false
            }
        };

        let match_scorer = |id: &DocumentId, query: &[f32]| -> Option<f32> {
            self.vector_slice(id)
                .map(|vec| score_with_metric(self.metric, vec, query))
        };

        Ok(batch_search.search_filtered(
            queries,
            top_k,
            &match_scorer,
            &candidates_provider,
            &check_provider,
        )?)
    }

    fn vector_slice(&self, id: &DocumentId) -> Option<&[f32]> {
        if let Some(vector) = self.vectors.get(id) {
            return Some(vector.as_slice());
        }
        if let (Some(store), DocumentId::U64(raw_id)) = (&self.vector_store, id) {
            return store.get(*raw_id);
        }
        None
    }

    fn search_over_candidates(
        &self,
        vector: &[f32],
        candidates: &HashSet<DocumentId>,
    ) -> Result<Vec<SearchResult>, CatalogError> {
        let mut scored: Vec<SearchResult> = candidates
            .par_iter()
            .filter_map(|id| self.vector_slice(id).map(|vec| (id, vec)))
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

    fn search_all_vectors(&self, vector: &[f32]) -> Vec<SearchResult> {
        let mut scored: Vec<SearchResult> = self
            .vector_ids
            .par_iter()
            .filter_map(|id| self.vector_slice(id).map(|vec| (id, vec)))
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
        scored
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

    fn filter_candidates(&self, filter: Option<&Filter>) -> Option<HashSet<DocumentId>> {
        filter.and_then(|candidate_filter| self.metadata_index.candidates(candidate_filter))
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
        let plan = QueryPlanner::default().plan_collection(self, None, Some(query), filter, top_k)?;
        let top_k = plan.top_k();
        if let Some(f) = filter {
            self.validate_filter(f)?;
        }
        let index = self
            .text_index
            .as_ref()
            .ok_or_else(|| CatalogError::InvalidSchema("collection has no text index".into()))?;
        let candidates = self.filter_candidates(filter);
        let results = match plan.text_path() {
            Some(QueryExecutionPath::TextFilterScan) => {
                index.search_with_candidates(query, top_k * 2, candidates.as_ref())?
            }
            Some(QueryExecutionPath::TextIndex) => index.search(query, top_k * 2)?,
            path => panic!("unexpected text execution path: {path:?}"),
        };
        let mut sources = vec![self.filter_results(results, filter)];
        if sources[0].len() < top_k {
            let mut fallback = match plan.text_path() {
                Some(QueryExecutionPath::TextFilterScan) => {
                    index.search_with_candidates(query, top_k * 4, candidates.as_ref())?
                }
                Some(QueryExecutionPath::TextIndex) => index.search(query, top_k * 4)?,
                path => panic!("unexpected text execution path: {path:?}"),
            };
            fallback = self.filter_results(fallback, filter);
            sources.push(fallback);
        }
        Ok(merge_search_result_sources(sources, top_k))
    }

    pub fn search_hybrid(
        &self,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<HybridWeights>,
        filter: Option<&Filter>,
    ) -> Result<Vec<HybridSearchResult>, CatalogError> {
        let plan =
            QueryPlanner::default().plan_collection(self, Some(vector), Some(query), filter, top_k)?;
        let top_k = plan.top_k();
        if let Some(f) = filter {
            self.validate_filter(f)?;
        }
        let weights = weights.unwrap_or_default();
        match plan.variant() {
            QueryVariant::VectorOnly | QueryVariant::FilteredVector => {
                let results = self.search_with_filter(vector, top_k, filter)?;
                Ok(project_hybrid_branch(results, HybridBranch::Vector, weights.vector))
            }
            QueryVariant::TextOnly | QueryVariant::FilteredText => {
                self.text_index.as_ref().ok_or_else(|| {
                    CatalogError::InvalidSchema("collection has no text index".into())
                })?;
                let results = self.search_text_with_filter(query, top_k, filter)?;
                Ok(project_hybrid_branch(results, HybridBranch::Text, weights.bm25))
            }
            QueryVariant::Hybrid | QueryVariant::FilteredHybrid => {
                self.text_index.as_ref().ok_or_else(|| {
                    CatalogError::InvalidSchema("collection has no text index".into())
                })?;

                let (bm25_results, vector_results) = rayon::join(
                    || self.search_text_with_filter(query, top_k * 2, filter),
                    || self.search_with_filter(vector, top_k * 2, filter),
                );

                Ok(fuse_hybrid_results(
                    bm25_results?,
                    vector_results?,
                    weights,
                    top_k,
                ))
            }
        }
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

    pub fn index_build_snapshot(&self, index_type: Option<IndexType>) -> IndexBuildSnapshot {
        let mut vectors: Vec<_> = self
            .vector_ids
            .iter()
            .filter_map(|id| {
                self.vector_slice(id)
                    .map(|vector| (id.clone(), vector.to_vec()))
            })
            .collect();
        vectors.sort_by(|(left, _), (right, _)| left.cmp(right));
        IndexBuildSnapshot {
            metric: self.metric,
            dimension: self.dimension,
            index_type: index_type.unwrap_or_else(|| self.index_type.clone()),
            vectors,
        }
    }

    pub fn install_rebuilt_index(
        &mut self,
        index_type: IndexType,
        index: Box<dyn VectorIndex>,
    ) -> Result<(), CatalogError> {
        self.set_index_state(IndexState::Ready)?;
        self.index = index;
        self.index_type = index_type.clone();
        self.schema.set_vector_index(index_type);
        Ok(())
    }

    pub fn rebuild_index(&mut self, index_type: Option<IndexType>) -> Result<(), CatalogError> {
        self.set_index_state(IndexState::Building)?;
        let snapshot = self.index_build_snapshot(index_type);
        let index = snapshot.build()?;
        self.install_rebuilt_index(snapshot.index_type, index)
    }

    pub fn schema(&self) -> &CollectionSchema {
        &self.schema
    }

    pub fn index_state(&self) -> IndexState {
        self.index_state
    }

    pub fn set_index_state(&mut self, next: IndexState) -> Result<(), CatalogError> {
        self.index_state = self.index_state.transition_to(next)?;
        Ok(())
    }

    pub fn vector_dimension(&self) -> usize {
        self.dimension
    }

    pub fn resident_vector_memory_bytes(&self) -> usize {
        let in_memory = self
            .vectors
            .values()
            .map(|v| v.len() * std::mem::size_of::<f32>())
            .sum::<usize>();
        let budgeted = self
            .vector_store
            .as_ref()
            .map(|store| store.resident_memory_bytes())
            .unwrap_or(0);
        in_memory + budgeted
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

    pub fn total_resident_vector_memory_bytes(&self, tenant: &TenantId) -> usize {
        self.collections
            .get(tenant)
            .map(|collections| {
                collections
                    .values()
                    .map(Collection::resident_vector_memory_bytes)
                    .sum()
            })
            .unwrap_or(0)
    }
}

impl QueryPlanner {
    /// Builds a query plan for the provided collection and query inputs.
    pub fn plan_collection(
        &self,
        collection: &Collection,
        vector: Option<&[f32]>,
        text: Option<&str>,
        filter: Option<&Filter>,
        top_k: usize,
    ) -> Result<QueryPlan, CatalogError> {
        let plan = QueryPlan::new(vector, text, filter, top_k)?;
        if plan.uses_text() && collection.text_index.is_none() {
            return Err(CatalogError::InvalidSchema(
                "collection has no text index".into(),
            ));
        }

        let candidates = collection.filter_candidates(filter);
        let vector_path = if plan.uses_vector() {
            Some(self.plan_vector_path(collection, candidates.as_ref()))
        } else {
            None
        };
        let text_path = if plan.uses_text() {
            Some(self.plan_text_path(candidates.as_ref()))
        } else {
            None
        };

        Ok(plan.with_paths(vector_path, text_path))
    }

    fn plan_vector_path(
        &self,
        collection: &Collection,
        candidates: Option<&HashSet<DocumentId>>,
    ) -> QueryExecutionPath {
        if collection.index_state != IndexState::Ready {
            QueryExecutionPath::VectorFullScan
        } else if matches!(candidates, Some(set) if set.len() <= self.options.filter_candidate_threshold())
        {
            QueryExecutionPath::VectorFilterScan
        } else {
            QueryExecutionPath::VectorIndex
        }
    }

    fn plan_text_path(&self, candidates: Option<&HashSet<DocumentId>>) -> QueryExecutionPath {
        if matches!(candidates, Some(set) if set.len() <= self.options.filter_candidate_threshold()) {
            QueryExecutionPath::TextFilterScan
        } else {
            QueryExecutionPath::TextIndex
        }
    }
}

fn merge_search_result_sources(
    sources: Vec<Vec<SearchResult>>,
    top_k: usize,
) -> Vec<SearchResult> {
    let mut merged: HashMap<DocumentId, SearchResult> = HashMap::new();

    for result in sources.into_iter().flatten() {
        let score = sanitize_score(result.score);
        merged
            .entry(result.id.clone())
            .and_modify(|existing| {
                if score > sanitize_score(existing.score) {
                    *existing = SearchResult {
                        id: result.id.clone(),
                        score,
                    };
                }
            })
            .or_insert(SearchResult {
                id: result.id,
                score,
            });
    }

    let mut results: Vec<_> = merged.into_values().collect();
    sort_search_results(&mut results);
    results.truncate(top_k.min(results.len()));
    results
}

const HYBRID_RRF_K: f32 = 60.0;

fn reciprocal_rank_score(rank: usize, weight: f32) -> f32 {
    weight / (HYBRID_RRF_K + rank as f32 + 1.0)
}

#[derive(Clone, Copy)]
enum HybridBranch {
    Text,
    Vector,
}

fn project_hybrid_branch(
    results: Vec<SearchResult>,
    branch: HybridBranch,
    weight: f32,
) -> Vec<HybridSearchResult> {
    let mut projected: Vec<_> = results
        .into_iter()
        .enumerate()
        .map(|result| {
            let (rank, result) = result;
            let score = reciprocal_rank_score(rank, weight);
            match branch {
                HybridBranch::Text => HybridSearchResult {
                    id: result.id,
                    bm25_score: Some(result.score),
                    vector_score: None,
                    score,
                },
                HybridBranch::Vector => HybridSearchResult {
                    id: result.id,
                    bm25_score: None,
                    vector_score: Some(result.score),
                    score,
                },
            }
        })
        .collect();
    sort_hybrid_results(&mut projected);
    projected
}

fn fuse_hybrid_results(
    bm25_results: Vec<SearchResult>,
    vector_results: Vec<SearchResult>,
    weights: HybridWeights,
    top_k: usize,
) -> Vec<HybridSearchResult> {
    let mut combined: HashMap<DocumentId, HybridSearchResult> = HashMap::new();

    for (rank, result) in bm25_results.into_iter().enumerate() {
        let score = reciprocal_rank_score(rank, weights.bm25);
        combined
            .entry(result.id.clone())
            .and_modify(|entry| {
                entry.bm25_score = Some(result.score);
                entry.score += score;
            })
            .or_insert(HybridSearchResult {
                id: result.id,
                bm25_score: Some(result.score),
                vector_score: None,
                score,
            });
    }

    for (rank, result) in vector_results.into_iter().enumerate() {
        let score = reciprocal_rank_score(rank, weights.vector);
        combined
            .entry(result.id.clone())
            .and_modify(|entry| {
                entry.vector_score = Some(result.score);
                entry.score += score;
            })
            .or_insert(HybridSearchResult {
                id: result.id,
                bm25_score: None,
                vector_score: Some(result.score),
                score,
            });
    }

    let mut results: Vec<_> = combined.into_values().collect();
    sort_hybrid_results(&mut results);
    results.truncate(top_k.min(results.len()));
    results
}

fn sanitize_score(score: f32) -> f32 {
    if score.is_finite() {
        score
    } else {
        0.0
    }
}

fn sort_search_results(results: &mut [SearchResult]) {
    results.sort_by(|left, right| {
        sanitize_score(right.score)
            .partial_cmp(&sanitize_score(left.score))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn sort_hybrid_results(results: &mut [HybridSearchResult]) {
    results.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use barq_index::HnswParams;
    use proptest::prelude::*;
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

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
    fn index_state_allows_valid_transitions() {
        let cases = [
            (IndexState::Ready, IndexState::Ready),
            (IndexState::Ready, IndexState::Building),
            (IndexState::Ready, IndexState::Stale),
            (IndexState::Building, IndexState::Building),
            (IndexState::Building, IndexState::Ready),
            (IndexState::Building, IndexState::Stale),
            (IndexState::Stale, IndexState::Stale),
            (IndexState::Stale, IndexState::Building),
        ];

        for (from, to) in cases {
            assert_eq!(from.transition_to(to).unwrap(), to, "{from:?} -> {to:?}");
        }
    }

    #[test]
    fn index_state_rejects_invalid_transitions() {
        let err = IndexState::Stale
            .transition_to(IndexState::Ready)
            .unwrap_err();

        assert!(
            matches!(err, CatalogError::InvalidSchema(message) if message.contains("invalid index state transition"))
        );
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
    fn search_fallback_matches_ready_results_while_index_is_building() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "products").unwrap();

        for (id, vector) in [
            (1, vec![1.0, 0.0, 0.0]),
            (2, vec![0.0, 1.0, 0.0]),
            (3, vec![0.5, 0.5, 0.0]),
        ] {
            collection
                .insert(Document {
                    id: DocumentId::U64(id),
                    vector,
                    payload: None,
                })
                .unwrap();
        }

        let ready = collection.search(&[0.8, 0.2, 0.0], 3).unwrap();
        collection.set_index_state(IndexState::Building).unwrap();

        let fallback = collection.search(&[0.8, 0.2, 0.0], 3).unwrap();

        assert_eq!(fallback, ready);
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
    fn query_plan_creates_common_query_variants() {
        let vector = [0.1, 0.2, 0.3];
        let filter = Filter::Exists {
            field: "body".to_string(),
        };
        let cases = [
            (
                QueryPlan::new(Some(&vector), None, None, 3).unwrap(),
                QueryVariant::VectorOnly,
            ),
            (
                QueryPlan::new(None, Some("rust"), None, 3).unwrap(),
                QueryVariant::TextOnly,
            ),
            (
                QueryPlan::new(Some(&vector), Some("rust"), None, 3).unwrap(),
                QueryVariant::Hybrid,
            ),
            (
                QueryPlan::new(Some(&vector), None, Some(&filter), 3).unwrap(),
                QueryVariant::FilteredVector,
            ),
            (
                QueryPlan::new(None, Some("rust"), Some(&filter), 3).unwrap(),
                QueryVariant::FilteredText,
            ),
            (
                QueryPlan::new(Some(&vector), Some("rust"), Some(&filter), 3).unwrap(),
                QueryVariant::FilteredHybrid,
            ),
        ];

        for (plan, expected) in cases {
            assert_eq!(plan.variant(), expected);
            assert_eq!(plan.has_filter(), expected.has_filter());
            assert_eq!(plan.uses_vector(), expected.uses_vector());
            assert_eq!(plan.uses_text(), expected.uses_text());
            assert_eq!(plan.top_k(), 3);
        }
    }

    #[test]
    fn query_plan_rejects_or_normalizes_invalid_combinations() {
        let vector = [0.1, 0.2, 0.3];
        let empty_vector: [f32; 0] = [];
        let filter = Filter::Exists {
            field: "body".to_string(),
        };

        let normalized = QueryPlan::new(Some(&vector), Some("   "), None, 5).unwrap();
        assert_eq!(normalized.variant(), QueryVariant::VectorOnly);

        let missing_query = QueryPlan::new(None, Some("   "), Some(&filter), 5).unwrap_err();
        assert!(matches!(missing_query, CatalogError::InvalidSchema(_)));

        let zero_top_k = QueryPlan::new(Some(&vector), None, None, 0).unwrap_err();
        assert!(matches!(zero_top_k, CatalogError::InvalidSchema(_)));

        let empty_vector_err = QueryPlan::new(Some(&empty_vector), None, None, 5).unwrap_err();
        assert!(matches!(empty_vector_err, CatalogError::InvalidSchema(_)));
    }

    #[test]
    fn planner_picks_expected_paths_for_known_scenarios() {
        let mut schema = text_schema();
        schema.name = "planner_articles".to_string();
        schema.fields.push(FieldSchema {
            name: "meta".to_string(),
            field_type: FieldType::Json,
            required: false,
        });

        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog.create_collection(tenant.clone(), schema).unwrap();
        let collection = catalog.collection_mut(&tenant, "planner_articles").unwrap();

        for (id, body, category) in [
            (1, "rust systems", "tech"),
            (2, "rust cookbook", "tech"),
            (3, "garden tools", "home"),
        ] {
            let mut payload = HashMap::new();
            payload.insert("body".to_string(), PayloadValue::String(body.into()));
            payload.insert(
                "meta".to_string(),
                PayloadValue::Object({
                    let mut meta = HashMap::new();
                    meta.insert("category".to_string(), PayloadValue::String(category.into()));
                    meta
                }),
            );
            collection
                .insert(Document {
                    id: DocumentId::U64(id),
                    vector: vec![id as f32, 0.0, 0.0],
                    payload: Some(PayloadValue::Object(payload)),
                })
                .unwrap();
        }

        let planner =
            QueryPlanner::new(QueryPlannerOptions::default().with_filter_candidate_threshold(2));
        let small_filter = Filter::Eq {
            field: "meta.category".to_string(),
            value: PayloadValue::String("home".into()),
        };
        let small_plan = planner
            .plan_collection(
                collection,
                Some(&[1.0, 0.0, 0.0]),
                Some("rust"),
                Some(&small_filter),
                2,
            )
            .unwrap();
        assert_eq!(small_plan.vector_path(), Some(QueryExecutionPath::VectorFilterScan));
        assert_eq!(small_plan.text_path(), Some(QueryExecutionPath::TextFilterScan));

        let broad_filter = Filter::Exists {
            field: "body".to_string(),
        };
        let broad_plan = QueryPlanner::new(
            QueryPlannerOptions::default().with_filter_candidate_threshold(1),
        )
        .plan_collection(
            collection,
            Some(&[1.0, 0.0, 0.0]),
            Some("rust"),
            Some(&broad_filter),
            2,
        )
        .unwrap();
        assert_eq!(broad_plan.vector_path(), Some(QueryExecutionPath::VectorIndex));
        assert_eq!(broad_plan.text_path(), Some(QueryExecutionPath::TextIndex));

        collection.set_index_state(IndexState::Building).unwrap();
        let fallback_plan = planner
            .plan_collection(collection, Some(&[1.0, 0.0, 0.0]), None, Some(&small_filter), 2)
            .unwrap();
        assert_eq!(fallback_plan.vector_path(), Some(QueryExecutionPath::VectorFullScan));
        assert_eq!(fallback_plan.text_path(), None);
    }

    #[test]
    fn planner_remains_deterministic() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), text_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "articles").unwrap();

        let mut payload = HashMap::new();
        payload.insert(
            "body".to_string(),
            PayloadValue::String("rust systems programming".into()),
        );
        collection
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![1.0, 0.0, 0.0],
                payload: Some(PayloadValue::Object(payload)),
            })
            .unwrap();

        let filter = Filter::Exists {
            field: "body".to_string(),
        };
        let planner =
            QueryPlanner::new(QueryPlannerOptions::default().with_filter_candidate_threshold(4));

        let first = planner
            .plan_collection(collection, Some(&[1.0, 0.0, 0.0]), Some("rust"), Some(&filter), 2)
            .unwrap();
        let second = planner
            .plan_collection(collection, Some(&[1.0, 0.0, 0.0]), Some("rust"), Some(&filter), 2)
            .unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn merge_search_results_is_deterministic_and_breaks_ties_by_id() {
        let sources = vec![
            vec![
                SearchResult {
                    id: DocumentId::U64(10),
                    score: 1.0,
                },
                SearchResult {
                    id: DocumentId::U64(2),
                    score: 1.0,
                },
            ],
            vec![SearchResult {
                id: DocumentId::U64(2),
                score: 1.0,
            }],
        ];

        let first = merge_search_result_sources(sources.clone(), 2);
        let second = merge_search_result_sources(sources, 2);

        assert_eq!(first, second);
        assert_eq!(first[0].id, DocumentId::U64(10));
        assert_eq!(first[1].id, DocumentId::U64(2));
    }

    #[test]
    fn merge_search_results_keeps_best_score_per_document() {
        let sources = vec![
            vec![
                SearchResult {
                    id: DocumentId::U64(1),
                    score: 0.2,
                },
                SearchResult {
                    id: DocumentId::U64(2),
                    score: 0.7,
                },
            ],
            vec![
                SearchResult {
                    id: DocumentId::U64(1),
                    score: 0.9,
                },
                SearchResult {
                    id: DocumentId::U64(3),
                    score: 0.8,
                },
            ],
        ];

        let merged = merge_search_result_sources(sources, 2);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].id, DocumentId::U64(1));
        assert_eq!(merged[0].score, 0.9);
        assert_eq!(merged[1].id, DocumentId::U64(3));
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
    fn hybrid_rrf_breaks_ties_deterministically() {
        let weights = HybridWeights {
            bm25: 0.5,
            vector: 0.5,
        };
        let bm25_results = vec![
            SearchResult {
                id: DocumentId::U64(1),
                score: 10.0,
            },
            SearchResult {
                id: DocumentId::U64(2),
                score: 0.0,
            },
        ];
        let vector_results = vec![
            SearchResult {
                id: DocumentId::U64(1),
                score: 0.0,
            },
            SearchResult {
                id: DocumentId::U64(2),
                score: 10.0,
            },
        ];

        let first = fuse_hybrid_results(bm25_results.clone(), vector_results.clone(), weights, 2);
        let second = fuse_hybrid_results(bm25_results, vector_results, weights, 2);

        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].id, DocumentId::U64(1));
        assert_eq!(first[1].id, DocumentId::U64(2));
        assert_eq!(first[0].score, reciprocal_rank_score(0, 0.5) + reciprocal_rank_score(0, 0.5));
        assert_eq!(first[1].score, reciprocal_rank_score(1, 0.5) + reciprocal_rank_score(1, 0.5));
        assert!(first[0].score > first[1].score);
    }

    #[test]
    fn hybrid_falls_back_to_vector_results_when_text_query_is_blank() {
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
                payload: None,
            })
            .unwrap();
        collection
            .insert(Document {
                id: DocumentId::U64(2),
                vector: vec![0.0, 1.0, 0.0],
                payload: None,
            })
            .unwrap();

        let results = collection
            .search_hybrid(&[1.0, 0.0, 0.0], "   ", 2, None, None)
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, DocumentId::U64(1));
        assert!(results.iter().all(|result| result.bm25_score.is_none()));
        assert!(results.iter().all(|result| result.vector_score.is_some()));
    }

    #[test]
    fn hybrid_falls_back_to_text_results_when_vector_query_is_empty() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), text_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "articles").unwrap();

        for (id, body) in [
            (1, "Rust systems programming guide"),
            (2, "Database internals"),
        ] {
            let mut payload = HashMap::new();
            payload.insert("body".to_string(), PayloadValue::String(body.into()));
            collection
                .insert(Document {
                    id: DocumentId::U64(id),
                    vector: vec![id as f32, 0.0, 0.0],
                    payload: Some(PayloadValue::Object(payload)),
                })
                .unwrap();
        }

        let results = collection
            .search_hybrid(&[], "rust guide", 2, None, None)
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));
        assert!(results.iter().all(|result| result.bm25_score.is_some()));
        assert!(results.iter().all(|result| result.vector_score.is_none()));
    }

    #[test]
    fn hybrid_search_remains_correct_while_vector_index_is_building() {
        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), text_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "articles").unwrap();

        let mut payload1 = HashMap::new();
        payload1.insert(
            "body".to_string(),
            PayloadValue::String("rust systems programming".into()),
        );
        let mut payload2 = HashMap::new();
        payload2.insert(
            "body".to_string(),
            PayloadValue::String("distributed database guide".into()),
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
                vector: vec![0.0, 1.0, 0.0],
                payload: Some(PayloadValue::Object(payload2)),
            })
            .unwrap();

        let ready = collection
            .search_hybrid(&[0.8, 0.2, 0.0], "rust", 2, None, None)
            .unwrap();
        collection.set_index_state(IndexState::Building).unwrap();

        let fallback = collection
            .search_hybrid(&[0.8, 0.2, 0.0], "rust", 2, None, None)
            .unwrap();

        assert_eq!(fallback, ready);
    }

    #[test]
    fn reciprocal_rank_score_decreases_by_rank() {
        let first = reciprocal_rank_score(0, 0.5);
        let second = reciprocal_rank_score(1, 0.5);
        let tenth = reciprocal_rank_score(9, 0.5);

        assert!(first > second);
        assert!(second > tenth);
    }

    #[test]
    fn sanitize_score_treats_non_finite_as_zero() {
        assert_eq!(sanitize_score(f32::NAN), 0.0);
        assert_eq!(sanitize_score(f32::INFINITY), 0.0);
        assert_eq!(sanitize_score(-f32::INFINITY), 0.0);
        assert_eq!(sanitize_score(2.0), 2.0);
    }

    #[test]
    fn hybrid_rrf_favors_documents_ranked_by_both_branches() {
        let weights = HybridWeights {
            bm25: 0.5,
            vector: 0.5,
        };
        let bm25_results = vec![
            SearchResult {
                id: DocumentId::U64(1),
                score: 100.0,
            },
            SearchResult {
                id: DocumentId::U64(2),
                score: 99.0,
            },
        ];
        let vector_results = vec![
            SearchResult {
                id: DocumentId::U64(2),
                score: 99.0,
            },
            SearchResult {
                id: DocumentId::U64(3),
                score: 98.0,
            },
        ];

        let fused = fuse_hybrid_results(bm25_results, vector_results, weights, 3);

        assert_eq!(fused[0].id, DocumentId::U64(2));
        assert!(fused[0].bm25_score.is_some());
        assert!(fused[0].vector_score.is_some());
    }

    proptest! {
        #[test]
        fn merged_results_match_bruteforce_baseline(
            sources in proptest::collection::vec(
                proptest::collection::vec((1u8..16u8, -1000i16..1000i16), 0..8),
                1..4
            ),
            top_k in 1usize..8,
        ) {
            let ranked_sources: Vec<Vec<SearchResult>> = sources
                .iter()
                .map(|source| {
                    source
                        .iter()
                        .map(|(id, score)| SearchResult {
                            id: DocumentId::U64(*id as u64),
                            score: *score as f32 / 100.0,
                        })
                        .collect()
                })
                .collect();

            let mut expected: HashMap<DocumentId, SearchResult> = HashMap::new();
            for result in ranked_sources.iter().flatten() {
                expected
                    .entry(result.id.clone())
                    .and_modify(|existing| {
                        if sanitize_score(result.score) > sanitize_score(existing.score) {
                            *existing = result.clone();
                        }
                    })
                    .or_insert_with(|| result.clone());
            }

            let mut expected: Vec<_> = expected.into_values().collect();
            sort_search_results(&mut expected);
            expected.truncate(top_k.min(expected.len()));

            let merged = merge_search_result_sources(ranked_sources, top_k);
            prop_assert_eq!(merged, expected);
        }
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

    #[test]
    fn text_search_filter_pushdown_matches_post_filter_baseline() {
        let mut schema = text_schema();
        schema.name = "articles_text_filter".to_string();
        schema.fields.push(FieldSchema {
            name: "meta".to_string(),
            field_type: FieldType::Json,
            required: false,
        });

        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog.create_collection(tenant.clone(), schema).unwrap();
        let collection = catalog
            .collection_mut(&tenant, "articles_text_filter")
            .unwrap();

        let docs = [
            (
                1,
                vec![0.9, 0.1, 0.0],
                "Rust systems programming guide",
                "tech",
            ),
            (2, vec![0.8, 0.2, 0.0], "Rust cookbook", "tech"),
            (3, vec![0.1, 0.9, 0.0], "Garden cookbook", "home"),
        ];

        for (id, vector, body, category) in docs {
            let mut payload = HashMap::new();
            payload.insert("body".to_string(), PayloadValue::String(body.into()));
            payload.insert(
                "meta".to_string(),
                PayloadValue::Object({
                    let mut meta = HashMap::new();
                    meta.insert("category".to_string(), PayloadValue::String(category.into()));
                    meta
                }),
            );
            collection
                .insert(Document {
                    id: DocumentId::U64(id),
                    vector,
                    payload: Some(PayloadValue::Object(payload)),
                })
                .unwrap();
        }

        let filter = Filter::Eq {
            field: "meta.category".to_string(),
            value: PayloadValue::String("tech".into()),
        };

        let actual = collection
            .search_text_with_filter("rust guide", 2, Some(&filter))
            .unwrap();

        let mut expected = collection
            .text_index
            .as_ref()
            .unwrap()
            .search("rust guide", collection.document_count())
            .unwrap();
        expected = collection.filter_results(expected, Some(&filter));
        expected.truncate(2);

        assert_eq!(actual, expected);
    }

    #[test]
    fn hybrid_filter_pushdown_matches_post_filter_baseline() {
        let mut schema = text_schema();
        schema.name = "articles_hybrid_filter".to_string();
        schema.fields.push(FieldSchema {
            name: "meta".to_string(),
            field_type: FieldType::Json,
            required: false,
        });

        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog.create_collection(tenant.clone(), schema).unwrap();
        let collection = catalog
            .collection_mut(&tenant, "articles_hybrid_filter")
            .unwrap();

        let docs = [
            (
                1,
                vec![1.0, 0.0, 0.0],
                "Rust systems programming guide",
                "tech",
            ),
            (2, vec![0.8, 0.2, 0.0], "Rust cookbook", "tech"),
            (3, vec![0.0, 1.0, 0.0], "Garden cookbook", "home"),
        ];

        for (id, vector, body, category) in docs {
            let mut payload = HashMap::new();
            payload.insert("body".to_string(), PayloadValue::String(body.into()));
            payload.insert(
                "meta".to_string(),
                PayloadValue::Object({
                    let mut meta = HashMap::new();
                    meta.insert("category".to_string(), PayloadValue::String(category.into()));
                    meta
                }),
            );
            collection
                .insert(Document {
                    id: DocumentId::U64(id),
                    vector,
                    payload: Some(PayloadValue::Object(payload)),
                })
                .unwrap();
        }

        let filter = Filter::Eq {
            field: "meta.category".to_string(),
            value: PayloadValue::String("tech".into()),
        };
        let weights = HybridWeights {
            bm25: 0.6,
            vector: 0.4,
        };

        let actual = collection
            .search_hybrid(
                &[1.0, 0.0, 0.0],
                "rust guide",
                2,
                Some(weights),
                Some(&filter),
            )
            .unwrap();

        let mut bm25_results = collection
            .text_index
            .as_ref()
            .unwrap()
            .search("rust guide", collection.document_count())
            .unwrap();
        bm25_results = collection.filter_results(bm25_results, Some(&filter));
        bm25_results.truncate(4);

        let mut vector_results = collection.search_all_vectors(&[1.0, 0.0, 0.0]);
        vector_results = collection.filter_results(vector_results, Some(&filter));
        vector_results.truncate(4);

        let mut combined: HashMap<DocumentId, HybridSearchResult> = HashMap::new();

        for (rank, result) in bm25_results.into_iter().enumerate() {
            let score = reciprocal_rank_score(rank, weights.bm25);
            combined
                .entry(result.id.clone())
                .and_modify(|entry| {
                    entry.bm25_score = Some(result.score);
                    entry.score += score;
                })
                .or_insert(HybridSearchResult {
                    id: result.id,
                    bm25_score: Some(result.score),
                    vector_score: None,
                    score,
                });
        }

        for (rank, result) in vector_results.into_iter().enumerate() {
            let score = reciprocal_rank_score(rank, weights.vector);
            combined
                .entry(result.id.clone())
                .and_modify(|entry| {
                    entry.vector_score = Some(result.score);
                    entry.score += score;
                })
                .or_insert(HybridSearchResult {
                    id: result.id,
                    bm25_score: None,
                    vector_score: Some(result.score),
                    score,
                });
        }

        let mut expected: Vec<_> = combined.into_values().collect();
        expected.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        expected.truncate(2);

        assert_eq!(actual, expected);
    }

    #[test]
    fn collection_insert_retrieve_search_in_memory_mode() {
        let _guard = env_test_lock();
        std::env::remove_var("BARQ_VECTOR_STORE_MODE");
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
                payload: None,
            })
            .unwrap();

        let doc = collection.get(&DocumentId::U64(1)).unwrap();
        assert_eq!(doc.vector, vec![1.0, 0.0, 0.0]);
        let results = collection.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(results[0].id, DocumentId::U64(1));
    }

    #[test]
    fn collection_insert_retrieve_search_in_mmap_mode() {
        let _guard = env_test_lock();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("BARQ_VECTOR_STORE_MODE", "mmap");
        std::env::set_var("BARQ_VECTOR_STORE_PATH", dir.path());
        std::env::set_var("BARQ_MAX_MEMORY_MB", "1");

        let mut catalog = Catalog::new();
        let tenant = default_tenant();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        let collection = catalog.collection_mut(&tenant, "products").unwrap();
        collection
            .insert(Document {
                id: DocumentId::U64(2),
                vector: vec![0.0, 1.0, 0.0],
                payload: None,
            })
            .unwrap();

        let doc = collection.get(&DocumentId::U64(2)).unwrap();
        assert_eq!(doc.vector, vec![0.0, 1.0, 0.0]);
        let results = collection.search(&[0.0, 1.0, 0.0], 1).unwrap();
        assert_eq!(results[0].id, DocumentId::U64(2));

        std::env::remove_var("BARQ_VECTOR_STORE_MODE");
        std::env::remove_var("BARQ_VECTOR_STORE_PATH");
        std::env::remove_var("BARQ_MAX_MEMORY_MB");
    }

    #[test]
    fn collection_restart_in_mmap_mode_keeps_search_working() {
        let _guard = env_test_lock();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("BARQ_VECTOR_STORE_MODE", "mmap");
        std::env::set_var("BARQ_VECTOR_STORE_PATH", dir.path());

        let tenant = default_tenant();
        let mut catalog = Catalog::new();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        {
            let collection = catalog.collection_mut(&tenant, "products").unwrap();
            collection
                .insert(Document {
                    id: DocumentId::U64(3),
                    vector: vec![0.0, 0.0, 1.0],
                    payload: None,
                })
                .unwrap();
            let r = collection.search(&[0.0, 0.0, 1.0], 1).unwrap();
            assert_eq!(r[0].id, DocumentId::U64(3));
        }

        let mut catalog_restarted = Catalog::new();
        catalog_restarted
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        let collection = catalog_restarted
            .collection_mut(&tenant, "products")
            .unwrap();
        collection
            .insert(Document {
                id: DocumentId::U64(3),
                vector: vec![0.0, 0.0, 1.0],
                payload: None,
            })
            .unwrap();
        let r = collection.search(&[0.0, 0.0, 1.0], 1).unwrap();
        assert_eq!(r[0].id, DocumentId::U64(3));

        std::env::remove_var("BARQ_VECTOR_STORE_MODE");
        std::env::remove_var("BARQ_VECTOR_STORE_PATH");
    }

    #[test]
    fn collection_restart_hydrates_from_mmap_store_without_reinsert() {
        let _guard = env_test_lock();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("BARQ_VECTOR_STORE_MODE", "mmap");
        std::env::set_var("BARQ_VECTOR_STORE_PATH", dir.path());

        let tenant = default_tenant();
        let mut catalog = Catalog::new();
        catalog
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        {
            let collection = catalog.collection_mut(&tenant, "products").unwrap();
            collection
                .insert(Document {
                    id: DocumentId::U64(7),
                    vector: vec![0.3, 0.4, 0.5],
                    payload: None,
                })
                .unwrap();
        }

        let mut restarted = Catalog::new();
        restarted
            .create_collection(tenant.clone(), sample_schema())
            .unwrap();
        let collection = restarted.collection(&tenant, "products").unwrap();
        let r = collection.search(&[0.3, 0.4, 0.5], 1).unwrap();
        assert_eq!(r[0].id, DocumentId::U64(7));

        std::env::remove_var("BARQ_VECTOR_STORE_MODE");
        std::env::remove_var("BARQ_VECTOR_STORE_PATH");
    }

    #[test]
    fn catalog_reports_aggregate_vector_memory() {
        let _guard = env_test_lock();
        std::env::remove_var("BARQ_VECTOR_STORE_MODE");
        let tenant = default_tenant();
        let mut catalog = Catalog::new();

        let mut schema_a = sample_schema();
        schema_a.name = "products_a".into();
        let mut schema_b = sample_schema();
        schema_b.name = "products_b".into();

        catalog.create_collection(tenant.clone(), schema_a).unwrap();
        catalog.create_collection(tenant.clone(), schema_b).unwrap();
        catalog
            .collection_mut(&tenant, "products_a")
            .unwrap()
            .insert(Document {
                id: DocumentId::U64(1),
                vector: vec![1.0, 0.0, 0.0],
                payload: None,
            })
            .unwrap();
        catalog
            .collection_mut(&tenant, "products_b")
            .unwrap()
            .insert(Document {
                id: DocumentId::U64(2),
                vector: vec![0.0, 1.0, 0.0],
                payload: None,
            })
            .unwrap();

        assert!(
            catalog.total_resident_vector_memory_bytes(&tenant)
                >= 2 * 3 * std::mem::size_of::<f32>()
        );
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
