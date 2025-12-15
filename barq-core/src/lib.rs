use barq_index::{
    DistanceMetric, DocumentId, DocumentIdError, FlatIndex, SearchResult, VectorIndex,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

    #[error("invalid document id: {0}")]
    DocumentId(#[from] DocumentIdError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldType {
    Vector {
        dimension: usize,
        metric: DistanceMetric,
    },
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionSchema {
    pub name: String,
    pub fields: Vec<FieldSchema>,
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

impl CollectionSchema {
    pub fn validate(&self) -> Result<(), CatalogError> {
        let vector_fields: Vec<_> = self
            .fields
            .iter()
            .filter_map(|field| match field.field_type {
                FieldType::Vector {
                    dimension,
                    metric: _,
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

    pub fn vector_config(&self) -> Option<(usize, DistanceMetric)> {
        self.fields.iter().find_map(|field| match field.field_type {
            FieldType::Vector { dimension, metric } => Some((dimension, metric)),
            _ => None,
        })
    }
}

#[derive(Debug)]
pub struct Collection {
    schema: CollectionSchema,
    index: FlatIndex,
    payloads: HashMap<DocumentId, PayloadValue>,
    dimension: usize,
}

impl Collection {
    pub fn new(schema: CollectionSchema) -> Result<Self, CatalogError> {
        schema.validate()?;
        let (dimension, metric) = schema
            .vector_config()
            .ok_or_else(|| CatalogError::InvalidSchema("schema missing vector field".into()))?;

        Ok(Self {
            schema,
            index: FlatIndex::new(metric, dimension),
            payloads: HashMap::new(),
            dimension,
        })
    }

    pub fn insert(&mut self, document: Document) -> Result<(), CatalogError> {
        self.validate_document(&document)?;
        self.index.insert(document.id.clone(), document.vector)?;
        if let Some(payload) = document.payload {
            self.payloads.insert(document.id, payload);
        }
        Ok(())
    }

    pub fn upsert(&mut self, document: Document) -> Result<(), CatalogError> {
        if self.payloads.contains_key(&document.id) {
            self.index.remove(&document.id);
        }
        self.insert(document)
    }

    pub fn delete(&mut self, id: &DocumentId) -> bool {
        let removed = self.index.remove(id);
        self.payloads.remove(id);
        removed.is_some()
    }

    pub fn search(&self, vector: &[f32], top_k: usize) -> Result<Vec<SearchResult>, CatalogError> {
        Ok(self.index.search(vector, top_k)?)
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
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> CollectionSchema {
        CollectionSchema {
            name: "products".to_string(),
            fields: vec![FieldSchema {
                name: "vector".to_string(),
                field_type: FieldType::Vector {
                    dimension: 3,
                    metric: DistanceMetric::Cosine,
                },
                required: true,
            }],
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
}
