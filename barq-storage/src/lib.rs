use barq_core::{Catalog, CatalogError, CollectionSchema, Document, Filter, TenantId};
use barq_index::{DocumentId, IndexType};
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
    default_tenant: TenantId,
}

impl Storage {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let root = root.as_ref().to_path_buf();
        let mut storage = Self {
            root,
            catalog: Catalog::new(),
            default_tenant: TenantId::default(),
        };
        storage.ensure_tenant_root(&storage.default_tenant)?;
        storage.load_collections()?;
        Ok(storage)
    }

    pub fn create_collection(&mut self, schema: CollectionSchema) -> Result<(), StorageError> {
        self.create_collection_for_tenant(self.default_tenant.clone(), schema)
    }

    pub fn create_collection_for_tenant(
        &mut self,
        tenant: impl Into<TenantId>,
        mut schema: CollectionSchema,
    ) -> Result<(), StorageError> {
        let tenant = tenant.into();
        if schema.tenant_id != tenant {
            schema.tenant_id = tenant.clone();
        }
        self.ensure_tenant_root(&tenant)?;
        self.catalog
            .create_collection(tenant.clone(), schema.clone())?;
        self.persist_schema(&tenant, &schema)
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<(), StorageError> {
        self.drop_collection_for_tenant(&self.default_tenant.clone(), name)
    }

    pub fn drop_collection_for_tenant(
        &mut self,
        tenant: &TenantId,
        name: &str,
    ) -> Result<(), StorageError> {
        self.catalog.drop_collection(tenant, name)?;
        let dir = self.collection_dir(tenant, name);
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
        self.insert_for_tenant(&self.default_tenant.clone(), collection, document, upsert)
    }

    pub fn insert_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        document: Document,
        upsert: bool,
    ) -> Result<(), StorageError> {
        {
            let coll = self.catalog.collection_mut(tenant, collection)?;
            if upsert {
                coll.upsert(document.clone())?;
            } else {
                coll.insert(document.clone())?;
            }
        }
        self.append_wal(
            tenant,
            collection,
            WalEntry {
                op: WalOp::Insert(document),
            },
        )
    }

    pub fn delete(&mut self, collection: &str, id: DocumentId) -> Result<bool, StorageError> {
        self.delete_for_tenant(&self.default_tenant.clone(), collection, id)
    }

    pub fn delete_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        id: DocumentId,
    ) -> Result<bool, StorageError> {
        let removed = {
            let coll = self.catalog.collection_mut(tenant, collection)?;
            coll.delete(&id)
        };
        if removed {
            self.append_wal(
                tenant,
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
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        self.search_for_tenant(&self.default_tenant, collection, query, top_k, filter)
    }

    pub fn search_for_tenant(
        &self,
        tenant: &TenantId,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.search_with_filter(query, top_k, filter)?)
    }

    pub fn search_text(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        self.search_text_for_tenant(&self.default_tenant, collection, query, top_k, filter)
    }

    pub fn search_text_for_tenant(
        &self,
        tenant: &TenantId,
        collection: &str,
        query: &str,
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.search_text_with_filter(query, top_k, filter)?)
    }

    pub fn search_hybrid(
        &self,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<barq_core::HybridWeights>,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_core::HybridSearchResult>, StorageError> {
        self.search_hybrid_for_tenant(
            &self.default_tenant,
            collection,
            vector,
            query,
            top_k,
            weights,
            filter,
        )
    }

    pub fn search_hybrid_for_tenant(
        &self,
        tenant: &TenantId,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<barq_core::HybridWeights>,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_core::HybridSearchResult>, StorageError> {
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.search_hybrid(vector, query, top_k, weights, filter)?)
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
        self.explain_hybrid_for_tenant(
            &self.default_tenant,
            collection,
            vector,
            query,
            top_k,
            id,
            weights,
        )
    }

    pub fn explain_hybrid_for_tenant(
        &self,
        tenant: &TenantId,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        id: &barq_index::DocumentId,
        weights: Option<barq_core::HybridWeights>,
    ) -> Result<Option<barq_core::HybridSearchResult>, StorageError> {
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.explain_hybrid(vector, query, top_k, id, weights)?)
    }

    pub fn rebuild_index(
        &mut self,
        collection: &str,
        index: Option<IndexType>,
    ) -> Result<(), StorageError> {
        self.rebuild_index_for_tenant(&self.default_tenant.clone(), collection, index)
    }

    pub fn rebuild_index_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        index: Option<IndexType>,
    ) -> Result<(), StorageError> {
        {
            let coll = self.catalog.collection_mut(tenant, collection)?;
            coll.rebuild_index(index)?;
            let schema = coll.schema().clone();
            self.persist_schema(tenant, &schema)?;
        }
        Ok(())
    }

    pub fn collection_schema(&self, name: &str) -> Result<&CollectionSchema, StorageError> {
        self.collection_schema_for_tenant(&self.default_tenant, name)
    }

    pub fn collection_schema_for_tenant(
        &self,
        tenant: &TenantId,
        name: &str,
    ) -> Result<&CollectionSchema, StorageError> {
        Ok(self.catalog.collection(tenant, name)?.schema())
    }

    pub fn collection_names(&self) -> Result<Vec<String>, StorageError> {
        self.collection_names_for_tenant(&self.default_tenant)
    }

    pub fn collection_names_for_tenant(
        &self,
        tenant: &TenantId,
    ) -> Result<Vec<String>, StorageError> {
        let mut names = Vec::new();
        let collections_root = self.collections_root(tenant);
        if !collections_root.exists() {
            return Ok(names);
        }
        for entry in fs::read_dir(&collections_root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                names.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        Ok(names)
    }

    fn load_collections(&mut self) -> Result<(), StorageError> {
        let tenants_root = self.tenants_root();
        if !tenants_root.exists() {
            return Ok(());
        }
        for tenant_entry in fs::read_dir(&tenants_root)? {
            let tenant_entry = tenant_entry?;
            if !tenant_entry.file_type()?.is_dir() {
                continue;
            }
            let tenant = TenantId::new(tenant_entry.file_name().to_string_lossy());
            let collections_root = tenant_entry.path().join("collections");
            if !collections_root.exists() {
                continue;
            }
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
                let mut schema: CollectionSchema = serde_json::from_reader(schema_file)?;
                if schema.tenant_id != tenant {
                    schema.tenant_id = tenant.clone();
                }
                self.catalog.create_collection(tenant.clone(), schema)?;
                self.replay_wal(&tenant, &name)?;
            }
        }
        Ok(())
    }

    fn replay_wal(&mut self, tenant: &TenantId, collection: &str) -> Result<(), StorageError> {
        let wal_path = self.wal_path(tenant, collection);
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
                    let coll = self.catalog.collection_mut(tenant, collection)?;
                    // Use upsert semantics during replay to guarantee last write wins.
                    coll.upsert(doc)?;
                }
                WalOp::Delete(id) => {
                    let coll = self.catalog.collection_mut(tenant, collection)?;
                    coll.delete(&id);
                }
            }
        }
        Ok(())
    }

    fn append_wal(
        &self,
        tenant: &TenantId,
        collection: &str,
        entry: WalEntry,
    ) -> Result<(), StorageError> {
        let wal_path = self.wal_path(tenant, collection);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path)?;
        let line = serde_json::to_string(&entry)?;
        writeln!(file, "{}", line)?;
        file.flush()?;
        Ok(())
    }

    fn persist_schema(
        &self,
        tenant: &TenantId,
        schema: &CollectionSchema,
    ) -> Result<(), StorageError> {
        let dir = self.collection_dir(tenant, &schema.name);
        fs::create_dir_all(&dir)?;
        let schema_path = dir.join("schema.json");
        let mut file = File::create(schema_path)?;
        serde_json::to_writer_pretty(&mut file, &schema)?;
        file.flush()?;
        Ok(())
    }

    fn collection_dir(&self, tenant: &TenantId, name: &str) -> PathBuf {
        self.collections_root(tenant).join(name)
    }

    fn wal_path(&self, tenant: &TenantId, name: &str) -> PathBuf {
        self.collection_dir(tenant, name).join("wal.jsonl")
    }

    fn collections_root(&self, tenant: &TenantId) -> PathBuf {
        self.tenant_root(tenant).join("collections")
    }

    fn tenant_root(&self, tenant: &TenantId) -> PathBuf {
        self.tenants_root().join(tenant.as_str())
    }

    fn tenants_root(&self) -> PathBuf {
        self.root.join("tenants")
    }

    fn ensure_tenant_root(&self, tenant: &TenantId) -> Result<(), StorageError> {
        fs::create_dir_all(self.collections_root(tenant))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barq_core::{FieldSchema, FieldType, PayloadValue};
    use barq_index::{DistanceMetric, DocumentId, HnswParams, IndexType};

    fn sample_schema(name: &str) -> CollectionSchema {
        CollectionSchema {
            name: name.to_string(),
            fields: vec![FieldSchema {
                name: "vector".to_string(),
                field_type: FieldType::Vector {
                    dimension: 3,
                    metric: DistanceMetric::L2,
                    index: None,
                },
                required: true,
            }],
            bm25_config: None,
            tenant_id: TenantId::default(),
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
        let results = storage.search("items", &[1.0, 0.0, 0.0], 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));
    }

    #[test]
    fn rebuilds_indexes_and_persists_schema() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage.create_collection(sample_schema("items")).unwrap();
        storage
            .insert(
                "items",
                Document {
                    id: DocumentId::U64(1),
                    vector: vec![0.0, 1.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();

        storage
            .rebuild_index("items", Some(IndexType::Hnsw(HnswParams::default())))
            .unwrap();

        let (_, _, index_type) = storage
            .collection_schema("items")
            .unwrap()
            .vector_config()
            .unwrap();
        assert!(matches!(index_type, IndexType::Hnsw(_)));
    }
}
