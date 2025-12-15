use barq_core::{Catalog, CatalogError, CollectionSchema, Document};
use barq_index::DocumentId;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    Insert(Document),
    Delete(DocumentId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub op: WalOp,
}

#[derive(Debug)]
pub struct Storage {
    root: PathBuf,
    catalog: Catalog,
}

impl Storage {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("collections"))?;
        let mut storage = Self {
            root,
            catalog: Catalog::new(),
        };
        storage.load_collections()?;
        Ok(storage)
    }

    pub fn create_collection(&mut self, schema: CollectionSchema) -> Result<(), StorageError> {
        self.catalog.create_collection(schema.clone())?;
        let dir = self.collection_dir(&schema.name);
        fs::create_dir_all(&dir)?;
        let schema_path = dir.join("schema.json");
        let mut file = File::create(schema_path)?;
        serde_json::to_writer_pretty(&mut file, &schema)?;
        file.flush()?;
        Ok(())
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<(), StorageError> {
        self.catalog.drop_collection(name)?;
        let dir = self.collection_dir(name);
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        Ok(())
    }

    pub fn insert(
        &mut self,
        collection: &str,
        document: Document,
        upsert: bool,
    ) -> Result<(), StorageError> {
        {
            let coll = self.catalog.collection_mut(collection)?;
            if upsert {
                coll.upsert(document.clone())?;
            } else {
                coll.insert(document.clone())?;
            }
        }
        self.append_wal(
            collection,
            WalEntry {
                op: WalOp::Insert(document),
            },
        )
    }

    pub fn delete(&mut self, collection: &str, id: DocumentId) -> Result<bool, StorageError> {
        let removed = {
            let coll = self.catalog.collection_mut(collection)?;
            coll.delete(&id)
        };
        if removed {
            self.append_wal(
                collection,
                WalEntry {
                    op: WalOp::Delete(id),
                },
            )?;
        }
        Ok(removed)
    }

    pub fn search(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        let coll = self.catalog.collection(collection)?;
        Ok(coll.search(query, top_k)?)
    }

    pub fn search_text(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        let coll = self.catalog.collection(collection)?;
        Ok(coll.search_text(query, top_k)?)
    }

    pub fn search_hybrid(
        &self,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<barq_core::HybridWeights>,
    ) -> Result<Vec<barq_core::HybridSearchResult>, StorageError> {
        let coll = self.catalog.collection(collection)?;
        Ok(coll.search_hybrid(vector, query, top_k, weights)?)
    }

    pub fn explain_hybrid(
        &self,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        id: &barq_index::DocumentId,
        weights: Option<barq_core::HybridWeights>,
    ) -> Result<Option<barq_core::HybridSearchResult>, StorageError> {
        let coll = self.catalog.collection(collection)?;
        Ok(coll.explain_hybrid(vector, query, top_k, id, weights)?)
    }

    pub fn collection_schema(&self, name: &str) -> Result<&CollectionSchema, StorageError> {
        Ok(self.catalog.collection(name)?.schema())
    }

    pub fn collection_names(&self) -> Result<Vec<String>, StorageError> {
        let mut names = Vec::new();
        let collections_root = self.root.join("collections");
        for entry in fs::read_dir(&collections_root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                names.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        Ok(names)
    }

    fn load_collections(&mut self) -> Result<(), StorageError> {
        let collections_root = self.root.join("collections");
        for entry in fs::read_dir(&collections_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let schema_path = entry.path().join("schema.json");
            if !schema_path.exists() {
                continue;
            }
            let schema_file = File::open(&schema_path)?;
            let schema: CollectionSchema = serde_json::from_reader(schema_file)?;
            self.catalog.create_collection(schema)?;
            self.replay_wal(&name)?;
        }
        Ok(())
    }

    fn replay_wal(&mut self, collection: &str) -> Result<(), StorageError> {
        let wal_path = self.wal_path(collection);
        if !wal_path.exists() {
            return Ok(());
        }
        let file = File::open(&wal_path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: WalEntry = serde_json::from_str(&line)?;
            match entry.op {
                WalOp::Insert(doc) => {
                    let coll = self.catalog.collection_mut(collection)?;
                    // Use upsert semantics during replay to guarantee last write wins.
                    coll.upsert(doc)?;
                }
                WalOp::Delete(id) => {
                    let coll = self.catalog.collection_mut(collection)?;
                    coll.delete(&id);
                }
            }
        }
        Ok(())
    }

    fn append_wal(&self, collection: &str, entry: WalEntry) -> Result<(), StorageError> {
        let wal_path = self.wal_path(collection);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path)?;
        let line = serde_json::to_string(&entry)?;
        writeln!(file, "{}", line)?;
        file.flush()?;
        Ok(())
    }

    fn collection_dir(&self, name: &str) -> PathBuf {
        self.root.join("collections").join(name)
    }

    fn wal_path(&self, name: &str) -> PathBuf {
        self.collection_dir(name).join("wal.jsonl")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barq_core::{FieldSchema, FieldType, PayloadValue};
    use barq_index::{DistanceMetric, DocumentId};

    fn sample_schema(name: &str) -> CollectionSchema {
        CollectionSchema {
            name: name.to_string(),
            fields: vec![FieldSchema {
                name: "vector".to_string(),
                field_type: FieldType::Vector {
                    dimension: 3,
                    metric: DistanceMetric::L2,
                },
                required: true,
            }],
            bm25_config: None,
        }
    }

    #[test]
    fn wal_replay_restores_data() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.create_collection(sample_schema("items")).unwrap();
            storage
                .insert(
                    "items",
                    Document {
                        id: DocumentId::U64(1),
                        vector: vec![1.0, 0.0, 0.0],
                        payload: Some(PayloadValue::String("a".into())),
                    },
                    false,
                )
                .unwrap();
        }

        let storage = Storage::open(dir.path()).unwrap();
        let results = storage.search("items", &[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));
    }
}
