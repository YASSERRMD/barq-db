use barq_core::{Catalog, CatalogError, CollectionSchema, Document, Filter, TenantId};
use barq_index::{DocumentId, IndexType};
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("tenant {tenant} quota exceeded: {reason}")]
    QuotaExceeded { tenant: TenantId, reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TenantQuota {
    pub max_collections: Option<usize>,
    pub max_disk_bytes: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_qps: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantUsageReport {
    pub tenant: TenantId,
    pub collections: usize,
    pub documents: usize,
    pub disk_bytes: u64,
    pub memory_bytes: u64,
    pub current_qps: u32,
    pub quota: TenantQuota,
}

#[derive(Debug, Clone)]
struct TenantUsage {
    collections: usize,
    documents: usize,
    disk_bytes: u64,
    memory_bytes: u64,
    window_start: Instant,
    requests_in_window: u32,
}

impl Default for TenantUsage {
    fn default() -> Self {
        Self {
            collections: 0,
            documents: 0,
            disk_bytes: 0,
            memory_bytes: 0,
            window_start: Instant::now(),
            requests_in_window: 0,
        }
    }
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
    tenant_quotas: HashMap<TenantId, TenantQuota>,
    tenant_usage: HashMap<TenantId, TenantUsage>,
}

impl Storage {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let root = root.as_ref().to_path_buf();
        let mut storage = Self {
            root,
            catalog: Catalog::new(),
            default_tenant: TenantId::default(),
            tenant_quotas: HashMap::new(),
            tenant_usage: HashMap::new(),
        };
        storage.ensure_tenant_root(&storage.default_tenant)?;
        storage.load_collections()?;
        storage.recalculate_usage();
        Ok(storage)
    }

    fn ensure_tenant_state(&mut self, tenant: &TenantId) {
        self.tenant_quotas.entry(tenant.clone()).or_default();
        self.tenant_usage.entry(tenant.clone()).or_default();
    }

    fn enforce_qps(&mut self, tenant: &TenantId) -> Result<(), StorageError> {
        self.ensure_tenant_state(tenant);
        let quota = self.tenant_quotas.get(tenant).cloned().unwrap_or_default();
        let usage = self
            .tenant_usage
            .get_mut(tenant)
            .expect("usage must exist after ensure_tenant_state");

        if let Some(max_qps) = quota.max_qps {
            let now = Instant::now();
            if now.duration_since(usage.window_start) >= Duration::from_secs(1) {
                usage.window_start = now;
                usage.requests_in_window = 0;
            }
            if usage.requests_in_window >= max_qps {
                return Err(StorageError::QuotaExceeded {
                    tenant: tenant.clone(),
                    reason: format!("QPS limit {} exceeded", max_qps),
                });
            }
            usage.requests_in_window += 1;
        } else {
            usage.requests_in_window = usage.requests_in_window.saturating_add(1);
        }

        let tenant_label = tenant.to_string();
        counter!("tenant_requests_total", "tenant" => tenant_label.clone()).increment(1);
        gauge!("tenant_usage_current_qps", "tenant" => tenant_label)
            .set(usage.requests_in_window as f64);

        Ok(())
    }

    fn enforce_capacity(
        &mut self,
        tenant: &TenantId,
        projected_docs: isize,
        projected_bytes: isize,
    ) -> Result<(), StorageError> {
        self.ensure_tenant_state(tenant);
        let quota = self.tenant_quotas.get(tenant).cloned().unwrap_or_default();
        let usage = self.tenant_usage.get(tenant).cloned().unwrap_or_default();

        let docs = usage.documents as isize + projected_docs;
        let disk = usage.disk_bytes as isize + projected_bytes;
        let memory = usage.memory_bytes as isize + projected_bytes;

        if let Some(limit) = quota.max_disk_bytes {
            if disk > limit as isize {
                return Err(StorageError::QuotaExceeded {
                    tenant: tenant.clone(),
                    reason: format!("disk limit {} exceeded", limit),
                });
            }
        }

        if let Some(limit) = quota.max_memory_bytes {
            if memory > limit as isize {
                return Err(StorageError::QuotaExceeded {
                    tenant: tenant.clone(),
                    reason: format!("memory limit {} exceeded", limit),
                });
            }
        }

        if docs < 0 || disk < 0 || memory < 0 {
            return Err(StorageError::QuotaExceeded {
                tenant: tenant.clone(),
                reason: "calculated negative usage".into(),
            });
        }

        Ok(())
    }

    fn enforce_collection_limit(
        &self,
        tenant: &TenantId,
        additional: usize,
    ) -> Result<(), StorageError> {
        let quota = self.tenant_quotas.get(tenant).cloned().unwrap_or_default();
        let usage = self.tenant_usage.get(tenant).cloned().unwrap_or_default();
        if let Some(max_collections) = quota.max_collections {
            if usage.collections + additional > max_collections {
                return Err(StorageError::QuotaExceeded {
                    tenant: tenant.clone(),
                    reason: format!("collection limit {} exceeded", max_collections),
                });
            }
        }
        Ok(())
    }

    fn adjust_usage(&mut self, tenant: &TenantId, collections: isize, docs: isize, bytes: isize) {
        self.ensure_tenant_state(tenant);
        {
            let usage = self
                .tenant_usage
                .get_mut(tenant)
                .expect("usage must exist after ensure_tenant_state");
            usage.collections = usage.collections.saturating_add_signed(collections);
            usage.documents = usage.documents.saturating_add_signed(docs);
            let byte_delta: i64 = bytes as i64;
            usage.disk_bytes = usage.disk_bytes.saturating_add_signed(byte_delta);
            usage.memory_bytes = usage.memory_bytes.saturating_add_signed(byte_delta);
        }
        self.emit_usage_metrics(tenant);
    }

    fn document_size_bytes(&self, tenant: &TenantId, collection: &str, id: &DocumentId) -> usize {
        if let Ok(coll) = self.catalog.collection(tenant, collection) {
            coll.document_footprint(id).unwrap_or(0)
        } else {
            0
        }
    }

    fn estimate_document_size(document: &Document) -> usize {
        let vector_bytes = document.vector.len() * std::mem::size_of::<f32>();
        let payload_bytes = document
            .payload
            .as_ref()
            .and_then(|p| serde_json::to_vec(p).ok())
            .map(|v| v.len())
            .unwrap_or(0);
        vector_bytes + payload_bytes
    }

    fn emit_usage_metrics(&self, tenant: &TenantId) {
        let quota = self.tenant_quotas.get(tenant).cloned().unwrap_or_default();
        if let Some(usage) = self.tenant_usage.get(tenant) {
            self.record_usage_metrics(tenant, usage, &quota);
        }
    }

    fn record_usage_metrics(&self, tenant: &TenantId, usage: &TenantUsage, quota: &TenantQuota) {
        let tenant_label = tenant.to_string();

        gauge!("tenant_usage_collections", "tenant" => tenant_label.clone())
            .set(usage.collections as f64);
        gauge!("tenant_usage_documents", "tenant" => tenant_label.clone())
            .set(usage.documents as f64);
        gauge!("tenant_usage_disk_bytes", "tenant" => tenant_label.clone())
            .set(usage.disk_bytes as f64);
        gauge!("tenant_usage_memory_bytes", "tenant" => tenant_label.clone())
            .set(usage.memory_bytes as f64);
        gauge!("tenant_usage_current_qps", "tenant" => tenant_label.clone())
            .set(usage.requests_in_window as f64);

        if let Some(max) = quota.max_collections {
            gauge!("tenant_quota_collections", "tenant" => tenant_label.clone()).set(max as f64);
        }
        if let Some(max) = quota.max_disk_bytes {
            gauge!("tenant_quota_disk_bytes", "tenant" => tenant_label.clone()).set(max as f64);
        }
        if let Some(max) = quota.max_memory_bytes {
            gauge!("tenant_quota_memory_bytes", "tenant" => tenant_label.clone()).set(max as f64);
        }
        if let Some(max) = quota.max_qps {
            gauge!("tenant_quota_qps", "tenant" => tenant_label).set(max as f64);
        }
    }

    fn recalculate_usage(&mut self) {
        self.tenant_usage.clear();
        for (tenant, collections) in self.catalog.tenants() {
            let mut usage = TenantUsage::default();
            usage.collections = collections.len();
            for collection in collections.values() {
                let (docs, bytes) = collection.total_footprint();
                usage.documents += docs;
                usage.disk_bytes += bytes as u64;
                usage.memory_bytes += bytes as u64;
            }
            self.tenant_usage.insert(tenant.clone(), usage);
        }
        // ensure default tenant exists
        let default = self.default_tenant.clone();
        self.ensure_tenant_state(&default);
        for tenant in self.tenant_usage.keys() {
            self.emit_usage_metrics(tenant);
        }
    }

    pub fn tenant_usage_report(&mut self, tenant: &TenantId) -> TenantUsageReport {
        self.ensure_tenant_state(tenant);
        let usage = self.tenant_usage.get(tenant).cloned().unwrap_or_default();
        let quota = self.tenant_quotas.get(tenant).cloned().unwrap_or_default();
        self.record_usage_metrics(tenant, &usage, &quota);
        TenantUsageReport {
            tenant: tenant.clone(),
            collections: usage.collections,
            documents: usage.documents,
            disk_bytes: usage.disk_bytes,
            memory_bytes: usage.memory_bytes,
            current_qps: usage.requests_in_window,
            quota,
        }
    }

    pub fn tenant_usage_reports(&mut self) -> Vec<TenantUsageReport> {
        let tenants: Vec<_> = self.tenant_usage.keys().cloned().collect();
        tenants
            .iter()
            .map(|tenant| self.tenant_usage_report(tenant))
            .collect()
    }

    pub fn set_tenant_quota(&mut self, tenant: TenantId, quota: TenantQuota) {
        self.ensure_tenant_state(&tenant);
        self.tenant_quotas.insert(tenant.clone(), quota);
        if let Some(usage) = self.tenant_usage.get_mut(&tenant) {
            usage.requests_in_window = 0;
            usage.window_start = Instant::now();
        }
        self.emit_usage_metrics(&tenant);
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
        self.ensure_tenant_state(&tenant);
        self.enforce_qps(&tenant)?;
        self.enforce_collection_limit(&tenant, 1)?;
        if schema.tenant_id != tenant {
            schema.tenant_id = tenant.clone();
        }
        self.ensure_tenant_root(&tenant)?;
        self.catalog
            .create_collection(tenant.clone(), schema.clone())?;
        self.persist_schema(&tenant, &schema)?;
        self.adjust_usage(&tenant, 1, 0, 0);
        Ok(())
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<(), StorageError> {
        self.drop_collection_for_tenant(&self.default_tenant.clone(), name)
    }

    pub fn drop_collection_for_tenant(
        &mut self,
        tenant: &TenantId,
        name: &str,
    ) -> Result<(), StorageError> {
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let (docs, bytes) = if let Ok(collection) = self.catalog.collection(tenant, name) {
            collection.total_footprint()
        } else {
            (0, 0)
        };
        self.catalog.drop_collection(tenant, name)?;
        let dir = self.collection_dir(tenant, name);
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        self.adjust_usage(tenant, -1, -(docs as isize), -(bytes as isize));
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
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let previous_size = self.document_size_bytes(tenant, collection, &document.id);
        let is_new = previous_size == 0;
        let projected_docs = if is_new { 1 } else { 0 };
        let new_size = Self::estimate_document_size(&document);
        let delta_bytes = new_size as isize - previous_size as isize;
        self.enforce_capacity(tenant, projected_docs, delta_bytes)?;
        {
            let coll = self.catalog.collection_mut(tenant, collection)?;
            if upsert {
                coll.upsert(document.clone())?;
            } else {
                coll.insert(document.clone())?;
            }
        }
        self.adjust_usage(tenant, 0, projected_docs, delta_bytes);
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
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let existing_bytes = self.document_size_bytes(tenant, collection, &id);
        let removed = {
            let coll = self.catalog.collection_mut(tenant, collection)?;
            coll.delete(&id)
        };
        if removed {
            self.adjust_usage(tenant, 0, -1, -(existing_bytes as isize));
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
        &mut self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        let default = self.default_tenant.clone();
        self.search_for_tenant(&default, collection, query, top_k, filter)
    }

    pub fn search_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        self.enforce_qps(tenant)?;
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.search_with_filter(query, top_k, filter)?)
    }

    pub fn search_text(
        &mut self,
        collection: &str,
        query: &str,
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        let default = self.default_tenant.clone();
        self.search_text_for_tenant(&default, collection, query, top_k, filter)
    }

    pub fn search_text_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        query: &str,
        top_k: usize,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_index::SearchResult>, StorageError> {
        self.enforce_qps(tenant)?;
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.search_text_with_filter(query, top_k, filter)?)
    }

    pub fn search_hybrid(
        &mut self,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<barq_core::HybridWeights>,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_core::HybridSearchResult>, StorageError> {
        let default = self.default_tenant.clone();
        self.search_hybrid_for_tenant(&default, collection, vector, query, top_k, weights, filter)
    }

    pub fn search_hybrid_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        weights: Option<barq_core::HybridWeights>,
        filter: Option<&Filter>,
    ) -> Result<Vec<barq_core::HybridSearchResult>, StorageError> {
        self.enforce_qps(tenant)?;
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.search_hybrid(vector, query, top_k, weights, filter)?)
    }

    pub fn explain_hybrid(
        &mut self,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        id: &barq_index::DocumentId,
        weights: Option<barq_core::HybridWeights>,
    ) -> Result<Option<barq_core::HybridSearchResult>, StorageError> {
        let default = self.default_tenant.clone();
        self.explain_hybrid_for_tenant(&default, collection, vector, query, top_k, id, weights)
    }

    pub fn explain_hybrid_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        vector: &[f32],
        query: &str,
        top_k: usize,
        id: &barq_index::DocumentId,
        weights: Option<barq_core::HybridWeights>,
    ) -> Result<Option<barq_core::HybridSearchResult>, StorageError> {
        self.enforce_qps(tenant)?;
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
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
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

        let mut storage = Storage::open(dir.path()).unwrap();
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
