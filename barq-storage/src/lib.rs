use barq_core::{
    Catalog, CatalogError, CollectionSchema, Document, Filter, IndexBuildSnapshot, IndexState,
    TenantId,
};
use barq_index::{DocumentId, IndexType, VectorIndex};
use chrono::{DateTime, Utc};
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

pub mod object_store;
pub use object_store::{
    is_retryable, with_retry, LocalObjectStore, ObjectMetadata, ObjectStore, ObjectStoreError,
    RetryConfig, RetryingObjectStore, StorageTier, TierConfig, TieringManager, TieringPolicy,
};

#[cfg(feature = "s3")]
pub use object_store::S3ObjectStore;

#[cfg(feature = "gcs")]
pub use object_store::GcsObjectStore;

#[cfg(feature = "azure")]
pub use object_store::AzureBlobStore;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("object store error: {0}")]
    ObjectStore(String),

    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("tenant {tenant} quota exceeded: {reason}")]
    QuotaExceeded { tenant: TenantId, reason: String },

    #[error("segment for collection {collection} is not writable: {state:?}")]
    SegmentNotWritable {
        collection: String,
        state: SegmentState,
    },
}

impl From<ObjectStoreError> for StorageError {
    fn from(err: ObjectStoreError) -> Self {
        StorageError::ObjectStore(err.to_string())
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub version: String,
    pub created_at: DateTime<Utc>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub file_name: String,
    pub created_at: DateTime<Utc>,
    pub entries: usize,
    #[serde(default)]
    pub state: SegmentState,
}

/// Persisted metadata for a sealed or compacted segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentIndexMetadata {
    pub file_name: String,
    pub state: SegmentState,
    #[serde(default)]
    pub index_state: IndexState,
    #[serde(default)]
    pub index_version: u64,
    pub index_built: bool,
    pub indexed_at: DateTime<Utc>,
}

/// Lifecycle state for immutable segment files.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SegmentState {
    /// Segment is still accepting writes.
    #[default]
    Growing,
    /// Segment is frozen and can only be read.
    Sealed,
    /// Segment has been compacted into another segment.
    Compacted,
}

impl SegmentState {
    /// Returns true when transitioning from `self` to `next` is permitted.
    pub fn can_transition_to(self, next: SegmentState) -> bool {
        match (self, next) {
            (SegmentState::Growing, SegmentState::Sealed)
            | (SegmentState::Sealed, SegmentState::Compacted) => true,
            (current, target) => current == target,
        }
    }

    /// Validates a state transition and returns the target state on success.
    pub fn transition_to(self, next: SegmentState) -> Result<SegmentState, StorageError> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(StorageError::ObjectStore(format!(
                "invalid segment state transition: {self:?} -> {next:?}"
            )))
        }
    }

    fn metric_label(self) -> &'static str {
        match self {
            SegmentState::Growing => "growing",
            SegmentState::Sealed => "sealed",
            SegmentState::Compacted => "compacted",
        }
    }

    fn metric_order(self) -> u8 {
        match self {
            SegmentState::Growing => 0,
            SegmentState::Sealed => 1,
            SegmentState::Compacted => 2,
        }
    }
}

pub struct StorageOptions {
    pub tiering_manager: Option<Arc<TieringManager>>,
    pub auto_seal_threshold: usize,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            tiering_manager: None,
            auto_seal_threshold: usize::MAX,
        }
    }
}

/// Per-collection resident vector memory gauge sample captured for tests and admin surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CollectionMemorySample {
    tenant: String,
    collection: String,
    resident_vector_memory_bytes: u64,
}

/// Per-tenant resident vector memory gauge sample captured for tests and admin surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TenantMemorySample {
    tenant: String,
    resident_vector_memory_bytes: u64,
}

/// Per-collection WAL gauge sample captured for deterministic tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CollectionWalSample {
    tenant: String,
    collection: String,
    entries: u64,
    bytes: u64,
}

/// Per-collection segment file count captured for deterministic tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CollectionSegmentFileSample {
    tenant: String,
    collection: String,
    state: SegmentState,
    count: u64,
}

/// Per-collection current segment lifecycle state captured for deterministic tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CollectionSegmentStateSample {
    tenant: String,
    collection: String,
    state: SegmentState,
    active: bool,
}

/// Snapshot of storage-level metrics used by deterministic tests.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct StorageMetricsSnapshot {
    refresh_count: u64,
    total_resident_vector_memory_bytes: u64,
    wal_appends_total: u64,
    wal_bytes_written_total: u64,
    compactions_total: u64,
    tenant_memory_bytes: Vec<TenantMemorySample>,
    collection_memory_bytes: Vec<CollectionMemorySample>,
    collection_wal: Vec<CollectionWalSample>,
    collection_segment_files: Vec<CollectionSegmentFileSample>,
    collection_segment_states: Vec<CollectionSegmentStateSample>,
}

impl StorageMetricsSnapshot {
    #[cfg(test)]
    fn collection_memory_bytes(&self, tenant: &str, collection: &str) -> Option<u64> {
        self.collection_memory_bytes
            .iter()
            .find(|sample| sample.tenant == tenant && sample.collection == collection)
            .map(|sample| sample.resident_vector_memory_bytes)
    }

    #[cfg(test)]
    fn tenant_memory_bytes(&self, tenant: &str) -> Option<u64> {
        self.tenant_memory_bytes
            .iter()
            .find(|sample| sample.tenant == tenant)
            .map(|sample| sample.resident_vector_memory_bytes)
    }

    #[cfg(test)]
    fn collection_wal_entries(&self, tenant: &str, collection: &str) -> Option<u64> {
        self.collection_wal
            .iter()
            .find(|sample| sample.tenant == tenant && sample.collection == collection)
            .map(|sample| sample.entries)
    }

    #[cfg(test)]
    fn collection_wal_bytes(&self, tenant: &str, collection: &str) -> Option<u64> {
        self.collection_wal
            .iter()
            .find(|sample| sample.tenant == tenant && sample.collection == collection)
            .map(|sample| sample.bytes)
    }

    #[cfg(test)]
    fn collection_segment_file_count(
        &self,
        tenant: &str,
        collection: &str,
        state: SegmentState,
    ) -> Option<u64> {
        self.collection_segment_files
            .iter()
            .find(|sample| {
                sample.tenant == tenant && sample.collection == collection && sample.state == state
            })
            .map(|sample| sample.count)
    }

    #[cfg(test)]
    fn collection_segment_state_active(
        &self,
        tenant: &str,
        collection: &str,
        state: SegmentState,
    ) -> bool {
        self.collection_segment_states
            .iter()
            .find(|sample| {
                sample.tenant == tenant && sample.collection == collection && sample.state == state
            })
            .map(|sample| sample.active)
            .unwrap_or(false)
    }
}

/// Storage-owned metric state that mirrors emitted Prometheus gauges for deterministic tests.
#[derive(Debug)]
struct StorageMetrics {
    snapshot: Arc<RwLock<StorageMetricsSnapshot>>,
    wal_appends_total: AtomicU64,
    wal_bytes_written_total: AtomicU64,
    compactions_total: AtomicU64,
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(StorageMetricsSnapshot::default())),
            wal_appends_total: AtomicU64::new(0),
            wal_bytes_written_total: AtomicU64::new(0),
            compactions_total: AtomicU64::new(0),
        }
    }
}

impl StorageMetrics {
    fn record_wal_append(&self, bytes: u64) {
        self.wal_appends_total.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes_written_total
            .fetch_add(bytes, Ordering::Relaxed);
        metrics::counter!("storage_wal_appends_total").increment(1);
        metrics::counter!("storage_wal_bytes_written_total").increment(bytes);
    }

    fn record_compaction(&self) {
        self.compactions_total.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("storage_compactions_total").increment(1);
    }

    fn refresh(&self, storage: &Storage) {
        let mut tenant_samples = Vec::new();
        let mut collection_samples = Vec::new();
        let mut wal_samples = Vec::new();
        let mut segment_file_samples = Vec::new();
        let mut segment_state_samples = Vec::new();
        let mut total_resident_vector_memory_bytes = 0u64;

        for (tenant, collections) in storage.catalog.tenants() {
            let tenant_label = tenant.to_string();
            let mut tenant_total = 0u64;
            for (collection_name, collection) in collections {
                let resident_bytes = collection.resident_vector_memory_bytes() as u64;
                gauge!(
                    "resident_vector_memory_bytes",
                    "tenant" => tenant_label.clone(),
                    "collection" => collection_name.clone(),
                )
                .set(resident_bytes as f64);
                collection_samples.push(CollectionMemorySample {
                    tenant: tenant_label.clone(),
                    collection: collection_name.clone(),
                    resident_vector_memory_bytes: resident_bytes,
                });
                tenant_total = tenant_total.saturating_add(resident_bytes);

                let wal_path = storage.wal_path(tenant, collection_name);
                let (wal_entries, wal_bytes) = match std::fs::read_to_string(&wal_path) {
                    Ok(contents) => (
                        contents
                            .lines()
                            .filter(|line| !line.trim().is_empty())
                            .count() as u64,
                        contents.len() as u64,
                    ),
                    Err(_) => (0, 0),
                };
                gauge!(
                    "storage_wal_entries",
                    "tenant" => tenant_label.clone(),
                    "collection" => collection_name.clone(),
                )
                .set(wal_entries as f64);
                gauge!(
                    "storage_wal_bytes",
                    "tenant" => tenant_label.clone(),
                    "collection" => collection_name.clone(),
                )
                .set(wal_bytes as f64);
                wal_samples.push(CollectionWalSample {
                    tenant: tenant_label.clone(),
                    collection: collection_name.clone(),
                    entries: wal_entries,
                    bytes: wal_bytes,
                });

                let mut sealed_files = 0u64;
                let mut compacted_files = 0u64;
                if let Ok(segment_files) = storage.segment_files(tenant, collection_name) {
                    for segment in segment_files {
                        let state = Storage::segment_file_state(&segment);
                        match state {
                            SegmentState::Sealed => sealed_files += 1,
                            SegmentState::Compacted => compacted_files += 1,
                            SegmentState::Growing => {}
                        }
                    }
                }
                for (state, count) in [
                    (SegmentState::Growing, 0u64),
                    (SegmentState::Sealed, sealed_files),
                    (SegmentState::Compacted, compacted_files),
                ] {
                    gauge!(
                        "storage_segment_files_count",
                        "tenant" => tenant_label.clone(),
                        "collection" => collection_name.clone(),
                        "state" => state.metric_label(),
                    )
                    .set(count as f64);
                    segment_file_samples.push(CollectionSegmentFileSample {
                        tenant: tenant_label.clone(),
                        collection: collection_name.clone(),
                        state,
                        count,
                    });
                }

                let current_state = storage.collection_segment_state(tenant, collection_name);
                for state in [
                    SegmentState::Growing,
                    SegmentState::Sealed,
                    SegmentState::Compacted,
                ] {
                    let active = current_state == state;
                    gauge!(
                        "storage_collection_segment_state",
                        "tenant" => tenant_label.clone(),
                        "collection" => collection_name.clone(),
                        "state" => state.metric_label(),
                    )
                    .set(u64::from(active) as f64);
                    segment_state_samples.push(CollectionSegmentStateSample {
                        tenant: tenant_label.clone(),
                        collection: collection_name.clone(),
                        state,
                        active,
                    });
                }
            }
            gauge!(
                "tenant_resident_vector_memory_bytes",
                "tenant" => tenant_label.clone(),
            )
            .set(tenant_total as f64);
            tenant_samples.push(TenantMemorySample {
                tenant: tenant_label,
                resident_vector_memory_bytes: tenant_total,
            });
            total_resident_vector_memory_bytes =
                total_resident_vector_memory_bytes.saturating_add(tenant_total);
        }

        gauge!("resident_vector_memory_bytes_total").set(total_resident_vector_memory_bytes as f64);

        tenant_samples.sort_by(|left, right| left.tenant.cmp(&right.tenant));
        collection_samples.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| left.collection.cmp(&right.collection))
        });
        wal_samples.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| left.collection.cmp(&right.collection))
        });
        segment_file_samples.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| left.collection.cmp(&right.collection))
                .then_with(|| left.state.metric_order().cmp(&right.state.metric_order()))
        });
        segment_state_samples.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| left.collection.cmp(&right.collection))
                .then_with(|| left.state.metric_order().cmp(&right.state.metric_order()))
        });

        let mut snapshot = self
            .snapshot
            .write()
            .expect("storage metrics snapshot lock poisoned");
        snapshot.refresh_count = snapshot.refresh_count.saturating_add(1);
        snapshot.total_resident_vector_memory_bytes = total_resident_vector_memory_bytes;
        snapshot.wal_appends_total = self.wal_appends_total.load(Ordering::Relaxed);
        snapshot.wal_bytes_written_total = self.wal_bytes_written_total.load(Ordering::Relaxed);
        snapshot.compactions_total = self.compactions_total.load(Ordering::Relaxed);
        snapshot.tenant_memory_bytes = tenant_samples;
        snapshot.collection_memory_bytes = collection_samples;
        snapshot.collection_wal = wal_samples;
        snapshot.collection_segment_files = segment_file_samples;
        snapshot.collection_segment_states = segment_state_samples;
    }

    fn snapshot(&self) -> StorageMetricsSnapshot {
        self.snapshot
            .read()
            .expect("storage metrics snapshot lock poisoned")
            .clone()
    }
}

#[derive(Debug)]
pub struct Storage {
    root: PathBuf,
    catalog: Catalog,
    default_tenant: TenantId,
    tenant_quotas: HashMap<TenantId, TenantQuota>,
    tenant_usage: HashMap<TenantId, TenantUsage>,
    tiering_manager: Option<Arc<TieringManager>>,
    collection_segment_states: HashMap<(TenantId, String), SegmentState>,
    collection_index_states: HashMap<(TenantId, String), IndexState>,
    collection_index_generations: HashMap<(TenantId, String), u64>,
    collection_index_versions: HashMap<(TenantId, String), u64>,
    collection_stale_segments: HashMap<(TenantId, String), BTreeSet<String>>,
    collection_write_counts: HashMap<(TenantId, String), usize>,
    index_builds: IndexBuildCoordinator,
    pending_index_builds: usize,
    auto_seal_threshold: usize,
    metrics: StorageMetrics,
    #[cfg(test)]
    fail_next_compaction: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CollectionLifecycleState {
    segment_state: SegmentState,
    #[serde(default)]
    index_state: IndexState,
    #[serde(default)]
    index_version: u64,
    #[serde(default)]
    stale_segments: Vec<String>,
}

#[derive(Debug)]
struct IndexBuildRequest {
    tenant: TenantId,
    collection: String,
    segment_state: SegmentState,
    planned_segments: Vec<String>,
    generation: u64,
    snapshot: IndexBuildSnapshot,
}

struct IndexBuildResult {
    tenant: TenantId,
    collection: String,
    segment_state: SegmentState,
    planned_segments: Vec<String>,
    generation: u64,
    index_type: IndexType,
    index: Result<Box<dyn VectorIndex>, String>,
}

struct IndexBuildCoordinator {
    request_tx: Sender<IndexBuildRequest>,
    result_rx: Receiver<IndexBuildResult>,
    #[cfg(test)]
    fail_next_build: Arc<AtomicBool>,
    #[cfg(test)]
    pause_before_build: Arc<Mutex<Option<Arc<PauseBeforeIndexBuild>>>>,
}

impl std::fmt::Debug for IndexBuildCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexBuildCoordinator")
            .finish_non_exhaustive()
    }
}

impl IndexBuildCoordinator {
    fn new() -> Self {
        let (request_tx, request_rx) = mpsc::channel::<IndexBuildRequest>();
        let (result_tx, result_rx) = mpsc::channel::<IndexBuildResult>();
        #[cfg(test)]
        let fail_next_build = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        let worker_fail_next_build = Arc::clone(&fail_next_build);
        #[cfg(test)]
        let pause_before_build = Arc::new(Mutex::new(None::<Arc<PauseBeforeIndexBuild>>));
        #[cfg(test)]
        let worker_pause_before_build = Arc::clone(&pause_before_build);
        thread::spawn(move || {
            while let Ok(request) = request_rx.recv() {
                #[cfg(test)]
                if let Some(hook) = worker_pause_before_build
                    .lock()
                    .expect("pause hook lock poisoned")
                    .take()
                {
                    hook.pause();
                }
                let index_type = request.snapshot.index_type.clone();
                #[cfg(test)]
                let should_fail = worker_fail_next_build.swap(false, Ordering::Relaxed);
                #[cfg(not(test))]
                let should_fail = false;
                let index = if should_fail {
                    Err("forced index build failure".to_string())
                } else {
                    request.snapshot.build().map_err(|err| err.to_string())
                };
                if result_tx
                    .send(IndexBuildResult {
                        tenant: request.tenant,
                        collection: request.collection,
                        segment_state: request.segment_state,
                        planned_segments: request.planned_segments,
                        generation: request.generation,
                        index_type,
                        index,
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        Self {
            request_tx,
            result_rx,
            #[cfg(test)]
            fail_next_build,
            #[cfg(test)]
            pause_before_build,
        }
    }

    #[cfg(test)]
    fn install_pause_before_build(&self) -> Arc<PauseBeforeIndexBuild> {
        let hook = Arc::new(PauseBeforeIndexBuild::default());
        *self
            .pause_before_build
            .lock()
            .expect("pause hook lock poisoned") = Some(Arc::clone(&hook));
        hook
    }

    #[cfg(test)]
    fn fail_next_build(&self) {
        self.fail_next_build.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
thread_local! {
    static PROCESS_ENV_LOCK_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[cfg(test)]
struct ProcessEnvLockGuard {
    _guard: Option<std::sync::MutexGuard<'static, ()>>,
}

#[cfg(test)]
fn process_env_lock() -> ProcessEnvLockGuard {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let mut outermost = false;
    PROCESS_ENV_LOCK_DEPTH.with(|depth| {
        let current = depth.get();
        outermost = current == 0;
        depth.set(current + 1);
    });

    let guard = if outermost {
        Some(LOCK.get_or_init(|| Mutex::new(())).lock().unwrap())
    } else {
        None
    };

    ProcessEnvLockGuard { _guard: guard }
}

#[cfg(test)]
impl Drop for ProcessEnvLockGuard {
    fn drop(&mut self) {
        PROCESS_ENV_LOCK_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

impl Storage {
    fn next_segment_suffix() -> u128 {
        static SEGMENT_COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let seq = SEGMENT_COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
        nanos.saturating_mul(1_000).saturating_add(seq)
    }

    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_with_options(root, StorageOptions::default())
    }

    pub fn open_with_options(
        root: impl AsRef<Path>,
        options: StorageOptions,
    ) -> Result<Self, StorageError> {
        #[cfg(test)]
        let _env_guard = process_env_lock();
        let root = root.as_ref().to_path_buf();
        let mut storage = Self {
            root,
            catalog: Catalog::new(),
            default_tenant: TenantId::default(),
            tenant_quotas: HashMap::new(),
            tenant_usage: HashMap::new(),
            tiering_manager: None,
            collection_segment_states: HashMap::new(),
            collection_index_states: HashMap::new(),
            collection_index_generations: HashMap::new(),
            collection_index_versions: HashMap::new(),
            collection_stale_segments: HashMap::new(),
            collection_write_counts: HashMap::new(),
            index_builds: IndexBuildCoordinator::new(),
            pending_index_builds: 0,
            auto_seal_threshold: options.auto_seal_threshold,
            metrics: StorageMetrics::default(),
            #[cfg(test)]
            fail_next_compaction: Arc::new(AtomicBool::new(false)),
        };

        if let Some(tm) = options.tiering_manager {
            storage.set_tiering_manager(tm);
        }

        storage.ensure_tenant_root(&storage.default_tenant)?;
        storage.load_collections()?;
        storage.recalculate_usage();
        storage.recalculate_usage();
        storage.recalculate_usage();
        storage.refresh_memory_metrics();
        Ok(storage)
    }

    pub fn set_tiering_manager(&mut self, manager: Arc<TieringManager>) {
        // Attempt to load existing tiering state
        let state_path = self.root.join("tiering_state.json");
        if let Err(e) = manager.load_state(&state_path) {
            eprintln!("Failed to load tiering state: {}", e); // Use proper logging
        }
        self.tiering_manager = Some(manager);
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    fn refresh_memory_metrics(&self) {
        self.metrics.refresh(self);
    }

    #[cfg(test)]
    fn memory_metrics_snapshot(&self) -> StorageMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[cfg(test)]
    fn fail_next_index_build(&self) {
        self.index_builds.fail_next_build();
    }

    #[cfg(test)]
    fn fail_next_background_compaction(&self) {
        self.fail_next_compaction.store(true, Ordering::Relaxed);
    }

    fn segment_file_state(path: &Path) -> SegmentState {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if file_name.starts_with("segment_compacted_") {
            SegmentState::Compacted
        } else {
            SegmentState::Sealed
        }
    }

    pub fn open_with_snapshot(
        root: impl AsRef<Path>,
        snapshot_dir: impl AsRef<Path>,
    ) -> Result<Self, StorageError> {
        let root = root.as_ref();
        let snapshot_dir = snapshot_dir.as_ref();
        Self::restore_snapshot(root, snapshot_dir)?;
        Self::open(root)
    }

    fn apply_entry(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        entry: WalEntry,
    ) -> Result<(), StorageError> {
        match entry.op {
            WalOp::Insert(doc) => {
                let coll = self.catalog.collection_mut(tenant, collection)?;
                coll.upsert(doc)?;
            }
            WalOp::Delete(id) => {
                let coll = self.catalog.collection_mut(tenant, collection)?;
                coll.delete(&id);
            }
        }
        Ok(())
    }

    pub fn get_document(
        &self,
        tenant: &TenantId,
        collection: &str,
        id: &DocumentId,
    ) -> Result<Option<Document>, StorageError> {
        let coll = self.catalog.collection(tenant, collection)?;
        Ok(coll.get(id))
    }

    fn ensure_tenant_state(&mut self, tenant: &TenantId) {
        self.tenant_quotas.entry(tenant.clone()).or_default();
        self.tenant_usage.entry(tenant.clone()).or_default();
    }

    fn collection_segment_state(&self, tenant: &TenantId, collection: &str) -> SegmentState {
        self.collection_segment_states
            .get(&(tenant.clone(), collection.to_string()))
            .copied()
            .unwrap_or(SegmentState::Growing)
    }

    fn set_collection_segment_state(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        state: SegmentState,
    ) -> Result<(), StorageError> {
        self.collection_segment_states
            .insert((tenant.clone(), collection.to_string()), state);
        self.persist_collection_lifecycle_state(tenant, collection)
    }

    fn collection_index_state(&self, tenant: &TenantId, collection: &str) -> IndexState {
        self.collection_index_states
            .get(&(tenant.clone(), collection.to_string()))
            .copied()
            .unwrap_or_default()
    }

    fn set_collection_index_state(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        state: IndexState,
    ) -> Result<(), StorageError> {
        self.collection_index_states
            .insert((tenant.clone(), collection.to_string()), state);
        self.persist_collection_lifecycle_state(tenant, collection)
    }

    fn transition_collection_index_state(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        state: IndexState,
    ) -> Result<(), StorageError> {
        if self.collection_index_state(tenant, collection) == state {
            return Ok(());
        }
        self.catalog
            .collection_mut(tenant, collection)?
            .set_index_state(state)?;
        self.set_collection_index_state(tenant, collection, state)
    }

    fn collection_index_generation(&self, tenant: &TenantId, collection: &str) -> u64 {
        self.collection_index_generations
            .get(&(tenant.clone(), collection.to_string()))
            .copied()
            .unwrap_or(0)
    }

    fn bump_collection_index_generation(&mut self, tenant: &TenantId, collection: &str) -> u64 {
        let key = (tenant.clone(), collection.to_string());
        let generation = self.collection_index_generations.entry(key).or_insert(0);
        *generation += 1;
        *generation
    }

    fn collection_index_version(&self, tenant: &TenantId, collection: &str) -> u64 {
        self.collection_index_versions
            .get(&(tenant.clone(), collection.to_string()))
            .copied()
            .unwrap_or(0)
    }

    fn collection_stale_segments(&self, tenant: &TenantId, collection: &str) -> BTreeSet<String> {
        self.collection_stale_segments
            .get(&(tenant.clone(), collection.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    fn insert_collection_stale_segment(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        file_name: &str,
    ) {
        if file_name.is_empty() {
            return;
        }
        self.collection_stale_segments
            .entry((tenant.clone(), collection.to_string()))
            .or_default()
            .insert(file_name.to_string());
    }

    fn clear_collection_stale_segments(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        file_names: &[String],
    ) {
        if let Some(stale) = self
            .collection_stale_segments
            .get_mut(&(tenant.clone(), collection.to_string()))
        {
            for file_name in file_names {
                stale.remove(file_name);
            }
        }
    }

    fn mark_collection_index_stale(
        &mut self,
        tenant: &TenantId,
        collection: &str,
    ) -> Result<(), StorageError> {
        self.bump_collection_index_generation(tenant, collection);
        self.transition_collection_index_state(tenant, collection, IndexState::Stale)
    }

    fn record_collection_write(
        &mut self,
        tenant: &TenantId,
        collection: &str,
    ) -> Result<(), StorageError> {
        self.mark_collection_index_stale(tenant, collection)?;
        let key = (tenant.clone(), collection.to_string());
        let writes = self.collection_write_counts.entry(key).or_default();
        *writes += 1;
        if *writes >= self.auto_seal_threshold {
            self.seal_current_segment_for_tenant(tenant, collection, true)?;
        }
        Ok(())
    }

    fn seal_current_segment_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        create_new_growing: bool,
    ) -> Result<SegmentMetadata, StorageError> {
        let current = self.collection_segment_state(tenant, collection);
        current.transition_to(SegmentState::Sealed)?;
        self.set_collection_segment_state(tenant, collection, SegmentState::Sealed)?;
        let metadata = self.flush_wal_to_segment(tenant, collection)?;
        self.schedule_index_build_for_sealed_segment(tenant, collection, &metadata)?;
        self.collection_write_counts
            .insert((tenant.clone(), collection.to_string()), 0);
        if create_new_growing {
            self.set_collection_segment_state(tenant, collection, SegmentState::Growing)?;
        }
        Ok(metadata)
    }

    fn schedule_index_build_for_sealed_segment(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        metadata: &SegmentMetadata,
    ) -> Result<(), StorageError> {
        self.schedule_collection_index_build(
            tenant,
            collection,
            None,
            metadata.file_name.clone(),
            metadata.state,
        )
    }

    fn schedule_collection_index_build(
        &mut self,
        tenant: &TenantId,
        collection: &str,
        index: Option<IndexType>,
        file_name: String,
        segment_state: SegmentState,
    ) -> Result<(), StorageError> {
        let snapshot = self
            .catalog
            .collection(tenant, collection)?
            .index_build_snapshot(index);
        let generation = self.collection_index_generation(tenant, collection);
        if !file_name.is_empty() {
            self.insert_collection_stale_segment(tenant, collection, &file_name);
        }
        let planned_segments: Vec<_> = if file_name.is_empty() {
            self.collection_stale_segments(tenant, collection)
                .into_iter()
                .collect()
        } else {
            vec![file_name.clone()]
        };
        self.transition_collection_index_state(tenant, collection, IndexState::Building)?;
        self.persist_segment_index_metadata_for_file(
            tenant,
            collection,
            &file_name,
            segment_state,
            IndexState::Building,
            self.collection_index_version(tenant, collection),
            false,
        )?;
        self.index_builds
            .request_tx
            .send(IndexBuildRequest {
                tenant: tenant.clone(),
                collection: collection.to_string(),
                segment_state,
                planned_segments,
                generation,
                snapshot,
            })
            .map_err(|_| StorageError::ObjectStore("index build worker is unavailable".into()))?;
        self.pending_index_builds += 1;
        Ok(())
    }

    fn apply_completed_index_builds(&mut self) -> Result<(), StorageError> {
        loop {
            match self.index_builds.result_rx.try_recv() {
                Ok(result) => self.apply_index_build_result(result)?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(StorageError::ObjectStore(
                        "index build worker disconnected".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn wait_for_pending_index_builds(&mut self) -> Result<(), StorageError> {
        while self.pending_index_builds > 0 {
            let result =
                self.index_builds.result_rx.recv().map_err(|_| {
                    StorageError::ObjectStore("index build worker disconnected".into())
                })?;
            self.apply_index_build_result(result)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn install_pause_before_index_build(&self) -> Arc<PauseBeforeIndexBuild> {
        self.index_builds.install_pause_before_build()
    }

    fn apply_index_build_result(&mut self, result: IndexBuildResult) -> Result<(), StorageError> {
        self.pending_index_builds = self.pending_index_builds.saturating_sub(1);
        if result.generation != self.collection_index_generation(&result.tenant, &result.collection)
        {
            return Ok(());
        }

        match result.index {
            Ok(index) => {
                let next_version =
                    self.collection_index_version(&result.tenant, &result.collection) + 1;
                let schema = {
                    let coll = self
                        .catalog
                        .collection_mut(&result.tenant, &result.collection)?;
                    coll.install_rebuilt_index(result.index_type.clone(), index)?;
                    coll.schema().clone()
                };
                self.collection_index_states.insert(
                    (result.tenant.clone(), result.collection.clone()),
                    IndexState::Ready,
                );
                self.collection_index_versions.insert(
                    (result.tenant.clone(), result.collection.clone()),
                    next_version,
                );
                self.clear_collection_stale_segments(
                    &result.tenant,
                    &result.collection,
                    &result.planned_segments,
                );
                self.persist_schema(&result.tenant, &schema)?;
                self.persist_collection_lifecycle_state(&result.tenant, &result.collection)?;
                for file_name in &result.planned_segments {
                    self.persist_segment_index_metadata_for_file(
                        &result.tenant,
                        &result.collection,
                        file_name,
                        result.segment_state,
                        IndexState::Ready,
                        next_version,
                        true,
                    )?;
                }
            }
            Err(_) => {
                self.transition_collection_index_state(
                    &result.tenant,
                    &result.collection,
                    IndexState::Stale,
                )?;
                self.persist_collection_lifecycle_state(&result.tenant, &result.collection)?;
                for file_name in &result.planned_segments {
                    self.persist_segment_index_metadata_for_file(
                        &result.tenant,
                        &result.collection,
                        file_name,
                        result.segment_state,
                        IndexState::Stale,
                        self.collection_index_version(&result.tenant, &result.collection),
                        false,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn persist_segment_index_metadata_for_file(
        &self,
        tenant: &TenantId,
        collection: &str,
        file_name: &str,
        segment_state: SegmentState,
        index_state: IndexState,
        index_version: u64,
        index_built: bool,
    ) -> Result<(), StorageError> {
        if file_name.is_empty() {
            return Ok(());
        }
        let meta_path = self
            .segments_dir(tenant, collection)
            .join(format!("{}.meta.json", file_name));
        let mut file = File::create(meta_path)?;
        serde_json::to_writer_pretty(
            &mut file,
            &SegmentIndexMetadata {
                file_name: file_name.to_string(),
                state: segment_state,
                index_state,
                index_version,
                index_built,
                indexed_at: Utc::now(),
            },
        )?;
        file.flush()?;
        Ok(())
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

    pub fn create_snapshot(
        &self,
        snapshot_dir: impl AsRef<Path>,
    ) -> Result<SnapshotManifest, StorageError> {
        let snapshot_dir = snapshot_dir.as_ref();
        if snapshot_dir.exists() {
            fs::remove_dir_all(snapshot_dir)?;
        }
        fs::create_dir_all(snapshot_dir)?;

        let manifest = SnapshotManifest {
            version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: Utc::now(),
        };

        let manifest_path = snapshot_dir.join("snapshot.json");
        let mut file = File::create(&manifest_path)?;
        serde_json::to_writer_pretty(&mut file, &manifest)?;
        file.flush()?;

        let tenants_root = self.tenants_root();
        if tenants_root.exists() {
            let snapshot_tenants = snapshot_dir.join("tenants");
            Self::copy_dir_recursively(&tenants_root, &snapshot_tenants)?;
        }

        Ok(manifest)
    }

    /// Create a point-in-time snapshot and upload it to an object store in the background.
    ///
    /// The snapshot is first materialized locally and then copied using the provided
    /// [`ObjectStore`] implementation. The returned thread handle can be `join`ed by the
    /// caller to ensure completion or inspected for upload failures.
    pub fn create_snapshot_and_upload<Store: ObjectStore + 'static + Send + Sync>(
        &self,
        snapshot_dir: impl AsRef<Path>,
        object_store: Store,
        remote_prefix: impl AsRef<Path>,
    ) -> Result<thread::JoinHandle<Result<(), StorageError>>, StorageError> {
        let snapshot_dir = snapshot_dir.as_ref().to_path_buf();
        let remote_prefix = remote_prefix.as_ref().to_path_buf();
        self.create_snapshot(&snapshot_dir)?;
        let handle = thread::spawn(move || {
            object_store
                .upload_dir(&snapshot_dir, &remote_prefix)
                .map_err(StorageError::from)
        });
        Ok(handle)
    }

    pub fn restore_snapshot(
        target_root: impl AsRef<Path>,
        snapshot_dir: impl AsRef<Path>,
    ) -> Result<(), StorageError> {
        let target_root = target_root.as_ref();
        let snapshot_dir = snapshot_dir.as_ref();
        fs::create_dir_all(target_root)?;
        let manifest_path = snapshot_dir.join("snapshot.json");
        if !manifest_path.exists() {
            return Err(StorageError::SnapshotNotFound(
                manifest_path.to_string_lossy().to_string(),
            ));
        }

        let manifest_file = File::open(&manifest_path)?;
        let _: SnapshotManifest = serde_json::from_reader(manifest_file)?;

        let tenants_src = snapshot_dir.join("tenants");
        let tenants_dest = target_root.join("tenants");
        if tenants_dest.exists() {
            fs::remove_dir_all(&tenants_dest)?;
        }
        if tenants_src.exists() {
            fs::create_dir_all(&tenants_dest.parent().unwrap_or(target_root))?;
            Self::copy_dir_recursively(&tenants_src, &tenants_dest)?;
        }

        Ok(())
    }

    /// Download a snapshot from an object store and restore it into the provided root.
    pub fn restore_snapshot_from_object_store<Store: ObjectStore + 'static + Send + Sync>(
        target_root: impl AsRef<Path>,
        snapshot_dir: impl AsRef<Path>,
        object_store: Store,
        remote_prefix: impl AsRef<Path>,
    ) -> Result<(), StorageError> {
        let snapshot_dir = snapshot_dir.as_ref();
        if snapshot_dir.exists() {
            fs::remove_dir_all(snapshot_dir)?;
        }
        fs::create_dir_all(snapshot_dir)?;
        object_store
            .download_dir(remote_prefix.as_ref(), snapshot_dir)
            .map_err(StorageError::from)?;
        Self::restore_snapshot(target_root, snapshot_dir)
    }

    /// Flush the current WAL for a collection into an immutable segment.
    /// This reduces startup replay time and prepares cold data for background upload.
    pub fn flush_wal_to_segment(
        &mut self,
        tenant: &TenantId,
        collection: &str,
    ) -> Result<SegmentMetadata, StorageError> {
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let wal_path = self.wal_path(tenant, collection);
        if !wal_path.exists() {
            return Ok(SegmentMetadata {
                file_name: String::new(),
                created_at: Utc::now(),
                entries: 0,
                state: SegmentState::Growing,
            });
        }

        let segments_dir = self.segments_dir(tenant, collection);
        fs::create_dir_all(&segments_dir)?;
        let file_name = format!("segment_{}.jsonl", Self::next_segment_suffix());
        let segment_path = segments_dir.join(&file_name);
        let mut segment_file = File::create(&segment_path)?;
        let wal_file = File::open(&wal_path)?;
        let reader = BufReader::new(wal_file);
        let mut entries = 0usize;
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            segment_file.write_all(line.as_bytes())?;
            segment_file.write_all(b"\n")?;
            entries += 1;
        }
        segment_file.flush()?;
        fs::remove_file(&wal_path)?;

        if let Some(tm) = &self.tiering_manager {
            if let Some(key) = self.get_tiering_key(&segment_path) {
                let size = segment_file.metadata()?.len();
                // best effort registration
                if let Err(e) = tm.register_existing(&key, size) {
                    // log error but don't fail flush
                    eprintln!("Failed to register segment with tiering manager: {}", e);
                }
            }
        }

        self.refresh_memory_metrics();

        Ok(SegmentMetadata {
            file_name,
            created_at: Utc::now(),
            entries,
            state: SegmentState::Sealed,
        })
    }

    /// Merge all existing immutable segments for a collection into a single compacted segment.
    ///
    /// Compaction keeps only the latest version of each document and discards tombstones,
    /// producing a smaller on-disk footprint for backups and uploads.
    pub fn compact_segments(
        &mut self,
        tenant: &TenantId,
        collection: &str,
    ) -> Result<SegmentMetadata, StorageError> {
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let segments = self.segment_files(tenant, collection)?;
        if segments.is_empty() {
            return Ok(SegmentMetadata {
                file_name: String::new(),
                created_at: Utc::now(),
                entries: 0,
                state: SegmentState::Growing,
            });
        }

        let mut latest: HashMap<DocumentId, WalOp> = HashMap::new();
        for segment in &segments {
            let file = File::open(segment)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let entry: WalEntry = serde_json::from_str(&line)?;
                match &entry.op {
                    WalOp::Insert(doc) => {
                        latest.insert(doc.id.clone(), WalOp::Insert(doc.clone()));
                    }
                    WalOp::Delete(id) => {
                        latest.insert(id.clone(), WalOp::Delete(id.clone()));
                    }
                }
            }
        }

        let segments_dir = self.segments_dir(tenant, collection);
        let file_name = format!("segment_compacted_{}.jsonl", Self::next_segment_suffix());
        let compacted_path = segments_dir.join(&file_name);
        fs::create_dir_all(&segments_dir)?;
        let mut compacted_file = File::create(&compacted_path)?;
        let mut entries = 0usize;
        for op in latest.values() {
            let entry = WalEntry { op: op.clone() };
            let line = serde_json::to_string(&entry)?;
            compacted_file.write_all(line.as_bytes())?;
            compacted_file.write_all(b"\n")?;
            entries += 1;
        }
        compacted_file.flush()?;

        if let Some(tm) = &self.tiering_manager {
            if let Some(key) = self.get_tiering_key(&compacted_path) {
                let size = compacted_file.metadata()?.len();
                if let Err(e) = tm.register_existing(&key, size) {
                    eprintln!("Failed to register compacted segment: {}", e);
                }
            }
        }

        for segment in segments {
            fs::remove_file(&segment)?;
            // TODO: Should we de-register old segments from TieringManager?
            // Yes, we should.
            if let Some(tm) = &self.tiering_manager {
                if let Some(key) = self.get_tiering_key(&segment) {
                    let _ = tm.delete(&key);
                }
            }
        }

        self.metrics.record_compaction();
        self.refresh_memory_metrics();

        Ok(SegmentMetadata {
            file_name,
            created_at: Utc::now(),
            entries,
            state: SegmentState::Compacted,
        })
    }

    /// Upload all immutable segments for a collection using the provided object store.
    ///
    /// This is useful for asynchronous offloading of cold data without blocking writes.
    pub fn upload_cold_segments<Store: ObjectStore + 'static + Send + Sync + Clone>(
        &self,
        tenant: &TenantId,
        collection: &str,
        object_store: Store,
        remote_prefix: impl AsRef<Path>,
    ) -> Result<thread::JoinHandle<Result<(), StorageError>>, StorageError> {
        let local_segments = self.segments_dir(tenant, collection);
        let remote_prefix = remote_prefix.as_ref().to_path_buf();
        fs::create_dir_all(&local_segments)?;
        let handle = thread::spawn(move || {
            object_store
                .upload_dir(&local_segments, &remote_prefix)
                .map_err(StorageError::from)
        });
        Ok(handle)
    }

    /// Runs one background compaction pass for collections with at least `min_segments` segments.
    pub fn run_background_compaction_once(
        &self,
        tenant: TenantId,
        min_segments: usize,
    ) -> thread::JoinHandle<Result<Vec<String>, StorageError>> {
        let root = self.root.clone();
        #[cfg(test)]
        let fail_next_compaction = Arc::clone(&self.fail_next_compaction);
        thread::spawn(move || {
            let mut storage = Storage::open(root)?;
            let collections_dir = storage.collections_root(&tenant);
            let mut compacted = Vec::new();
            if !collections_dir.exists() {
                return Ok(compacted);
            }
            for entry in fs::read_dir(collections_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let collection = entry.file_name().to_string_lossy().to_string();
                let segments = storage.segment_files(&tenant, &collection)?;
                if segments.len() < min_segments {
                    continue;
                }
                #[cfg(test)]
                if fail_next_compaction.swap(false, Ordering::Relaxed) {
                    return Err(StorageError::ObjectStore(
                        "simulated background compaction failure".to_string(),
                    ));
                }
                storage.compact_segments(&tenant, &collection)?;
                compacted.push(collection);
            }
            Ok(compacted)
        })
    }

    /// Runs a continuous background compaction worker until `stop_flag` is set.
    ///
    /// The worker performs one pass every `interval` and compacts collections with at least
    /// `min_segments` immutable segments.
    pub fn run_background_compaction_loop(
        &self,
        tenant: TenantId,
        min_segments: usize,
        interval: Duration,
        stop_flag: Arc<AtomicBool>,
    ) -> thread::JoinHandle<Result<Vec<String>, StorageError>> {
        let root = self.root.clone();
        thread::spawn(move || {
            let mut all_compacted = Vec::new();
            while !stop_flag.load(Ordering::Relaxed) {
                let mut storage = Storage::open(&root)?;
                let collections_dir = storage.collections_root(&tenant);
                if collections_dir.exists() {
                    for entry in fs::read_dir(collections_dir)? {
                        let entry = entry?;
                        if !entry.file_type()?.is_dir() {
                            continue;
                        }
                        let collection = entry.file_name().to_string_lossy().to_string();
                        let segments = storage.segment_files(&tenant, &collection)?;
                        if segments.len() < min_segments {
                            continue;
                        }
                        storage.compact_segments(&tenant, &collection)?;
                        all_compacted.push(collection);
                    }
                }
                thread::sleep(interval);
            }
            Ok(all_compacted)
        })
    }

    pub fn create_collection(&mut self, schema: CollectionSchema) -> Result<(), StorageError> {
        self.create_collection_for_tenant(self.default_tenant.clone(), schema)
    }

    pub fn create_collection_for_tenant(
        &mut self,
        tenant: impl Into<TenantId>,
        mut schema: CollectionSchema,
    ) -> Result<(), StorageError> {
        #[cfg(test)]
        let _env_guard = process_env_lock();
        self.apply_completed_index_builds()?;
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
        self.collection_index_states
            .insert((tenant.clone(), schema.name.clone()), IndexState::Ready);
        self.collection_index_generations
            .insert((tenant.clone(), schema.name.clone()), 0);
        self.collection_index_versions
            .insert((tenant.clone(), schema.name.clone()), 0);
        self.collection_stale_segments
            .insert((tenant.clone(), schema.name.clone()), BTreeSet::new());
        self.set_collection_segment_state(&tenant, &schema.name, SegmentState::Growing)?;
        self.collection_write_counts
            .insert((tenant.clone(), schema.name.clone()), 0);
        self.adjust_usage(&tenant, 1, 0, 0);
        self.refresh_memory_metrics();
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
        self.apply_completed_index_builds()?;
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
        self.collection_segment_states
            .remove(&(tenant.clone(), name.to_string()));
        self.collection_index_states
            .remove(&(tenant.clone(), name.to_string()));
        self.collection_index_generations
            .remove(&(tenant.clone(), name.to_string()));
        self.collection_index_versions
            .remove(&(tenant.clone(), name.to_string()));
        self.collection_stale_segments
            .remove(&(tenant.clone(), name.to_string()));
        self.collection_write_counts
            .remove(&(tenant.clone(), name.to_string()));
        self.adjust_usage(tenant, -1, -(docs as isize), -(bytes as isize));
        self.refresh_memory_metrics();
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
        self.apply_completed_index_builds()?;
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let segment_state = self.collection_segment_state(tenant, collection);
        if segment_state != SegmentState::Growing {
            return Err(StorageError::SegmentNotWritable {
                collection: collection.to_string(),
                state: segment_state,
            });
        }
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
        self.refresh_memory_metrics();
        self.append_wal(
            tenant,
            collection,
            WalEntry {
                op: WalOp::Insert(document),
            },
        )?;
        self.record_collection_write(tenant, collection)
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
        self.apply_completed_index_builds()?;
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let segment_state = self.collection_segment_state(tenant, collection);
        if segment_state != SegmentState::Growing {
            return Err(StorageError::SegmentNotWritable {
                collection: collection.to_string(),
                state: segment_state,
            });
        }
        let existing_bytes = self.document_size_bytes(tenant, collection, &id);
        let removed = {
            let coll = self.catalog.collection_mut(tenant, collection)?;
            coll.delete(&id)
        };
        if removed {
            self.adjust_usage(tenant, 0, -1, -(existing_bytes as isize));
            self.refresh_memory_metrics();
            self.append_wal(
                tenant,
                collection,
                WalEntry {
                    op: WalOp::Delete(id),
                },
            )?;
            self.record_collection_write(tenant, collection)?;
        }
        Ok(removed)
    }

    pub fn segment_state_for_tenant(&self, tenant: &TenantId, collection: &str) -> SegmentState {
        self.collection_segment_state(tenant, collection)
    }

    pub fn seal_segment_for_tenant(
        &mut self,
        tenant: &TenantId,
        collection: &str,
    ) -> Result<SegmentMetadata, StorageError> {
        self.apply_completed_index_builds()?;
        self.seal_current_segment_for_tenant(tenant, collection, false)
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
        self.apply_completed_index_builds()?;
        self.enforce_qps(tenant)?;
        let coll = self.catalog.collection(tenant, collection)?;
        let mut results = coll.search_with_filter(query, top_k, filter)?;
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(results)
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
        self.apply_completed_index_builds()?;
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
        self.apply_completed_index_builds()?;
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
        self.apply_completed_index_builds()?;
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
        self.apply_completed_index_builds()?;
        self.ensure_tenant_state(tenant);
        self.enforce_qps(tenant)?;
        let segment_state = self.collection_segment_state(tenant, collection);
        self.schedule_collection_index_build(
            tenant,
            collection,
            index,
            String::new(),
            segment_state,
        )
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

    /// Returns the current storage metrics snapshot as JSON for admin endpoints.
    pub fn metrics_report_json(&self) -> serde_json::Value {
        serde_json::to_value(self.metrics.snapshot())
            .expect("storage metrics snapshot serialization should succeed")
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

    pub(crate) fn copy_dir_recursively(src: &Path, dest: &Path) -> Result<(), StorageError> {
        fs::create_dir_all(dest)?;
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let file_name = entry.file_name();
            let dest_path = dest.join(file_name);
            if file_type.is_dir() {
                Self::copy_dir_recursively(&entry.path(), &dest_path)?;
            } else {
                fs::copy(entry.path(), dest_path)?;
            }
        }
        Ok(())
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
                let persisted_state = self
                    .load_collection_lifecycle_state(&tenant, &name)?
                    .unwrap_or(CollectionLifecycleState {
                        segment_state: SegmentState::Growing,
                        index_state: IndexState::Ready,
                        index_version: 0,
                        stale_segments: Vec::new(),
                    });
                self.collection_segment_states.insert(
                    (tenant.clone(), name.clone()),
                    persisted_state.segment_state,
                );
                self.collection_index_states
                    .insert((tenant.clone(), name.clone()), persisted_state.index_state);
                self.collection_index_generations
                    .insert((tenant.clone(), name.clone()), 0);
                self.collection_index_versions.insert(
                    (tenant.clone(), name.clone()),
                    persisted_state.index_version,
                );
                self.collection_stale_segments.insert(
                    (tenant.clone(), name.clone()),
                    persisted_state.stale_segments.into_iter().collect(),
                );
                self.collection_write_counts
                    .insert((tenant.clone(), name.clone()), 0);
                self.replay_segments(&tenant, &name)?;
                self.replay_wal(&tenant, &name)?;
                self.catalog
                    .collection_mut(&tenant, &name)?
                    .set_index_state(persisted_state.index_state)?;
            }
        }
        Ok(())
    }

    fn replay_segments(&mut self, tenant: &TenantId, collection: &str) -> Result<(), StorageError> {
        let segments = self.segment_files(tenant, collection)?;
        for segment in segments {
            if !segment.exists() {
                // Try to restore from TieringManager
                if let Some(tm) = &self.tiering_manager {
                    if let Some(key) = self.get_tiering_key(&segment) {
                        // This downloads to `segment` path (which is local hot path)
                        if let Err(e) = tm.download(&key, &segment) {
                            eprintln!("Failed to restore segment {}: {}", key, e);
                            // If we can't restore, we probably should fail or skip?
                            // Failing protects data consistency.
                            return Err(StorageError::ObjectStore(e.to_string()));
                        }
                    }
                }
            }
            let file = File::open(&segment)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let entry: WalEntry = serde_json::from_str(&line)?;
                self.apply_entry(tenant, collection, entry)?;
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
            // Use upsert semantics during replay to guarantee last write wins.
            self.apply_entry(tenant, collection, entry)?;
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
        self.metrics.record_wal_append((line.len() + 1) as u64);
        self.refresh_memory_metrics();
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

    fn collection_lifecycle_state_path(&self, tenant: &TenantId, name: &str) -> PathBuf {
        self.collection_dir(tenant, name)
            .join("lifecycle_state.json")
    }

    fn persist_collection_lifecycle_state(
        &self,
        tenant: &TenantId,
        collection: &str,
    ) -> Result<(), StorageError> {
        let collection_dir = self.collection_dir(tenant, collection);
        fs::create_dir_all(&collection_dir)?;
        let state_path = self.collection_lifecycle_state_path(tenant, collection);
        let mut file = File::create(state_path)?;
        serde_json::to_writer_pretty(
            &mut file,
            &CollectionLifecycleState {
                segment_state: self.collection_segment_state(tenant, collection),
                index_state: self.collection_index_state(tenant, collection),
                index_version: self.collection_index_version(tenant, collection),
                stale_segments: self
                    .collection_stale_segments(tenant, collection)
                    .into_iter()
                    .collect(),
            },
        )?;
        file.flush()?;
        Ok(())
    }

    fn load_collection_lifecycle_state(
        &self,
        tenant: &TenantId,
        collection: &str,
    ) -> Result<Option<CollectionLifecycleState>, StorageError> {
        let state_path = self.collection_lifecycle_state_path(tenant, collection);
        if !state_path.exists() {
            return Ok(None);
        }
        let file = File::open(state_path)?;
        let state: CollectionLifecycleState = serde_json::from_reader(file)?;
        Ok(Some(state))
    }

    fn wal_path(&self, tenant: &TenantId, name: &str) -> PathBuf {
        self.collection_dir(tenant, name).join("wal.jsonl")
    }

    fn segments_dir(&self, tenant: &TenantId, name: &str) -> PathBuf {
        self.collection_dir(tenant, name).join("segments")
    }

    fn segment_files(&self, tenant: &TenantId, name: &str) -> Result<Vec<PathBuf>, StorageError> {
        let dir = self.segments_dir(tenant, name);
        let mut files = std::collections::HashSet::new();

        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.file_type()?.is_file()
                    && entry
                        .path()
                        .file_name()
                        .and_then(|f| f.to_str())
                        .map(|name| name.ends_with(".jsonl"))
                        .unwrap_or(false)
                {
                    files.insert(entry.path());
                }
            }
        }

        if let Some(tm) = &self.tiering_manager {
            if let Some(prefix) = self.get_tiering_key(&dir) {
                // Ensure prefix ends with / to avoid partial matches on directory name if unrelated
                let search_prefix = if prefix.ends_with('/') {
                    prefix.clone()
                } else {
                    format!("{}/", prefix)
                };
                let remote_keys = tm.list_keys_with_prefix(&search_prefix);
                for key in remote_keys {
                    if key.ends_with(".jsonl") {
                        let abs_path = self.root.join(key);
                        files.insert(abs_path);
                    }
                }
            }
        }

        let mut sorted_files: Vec<PathBuf> = files.into_iter().collect();
        sorted_files.sort();
        Ok(sorted_files)
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

    fn get_tiering_key(&self, abs_path: &Path) -> Option<String> {
        abs_path
            .strip_prefix(&self.root)
            .ok()
            .map(|p| p.to_string_lossy().to_string())
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct PauseBeforeIndexBuild {
    state: Mutex<(bool, bool)>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl PauseBeforeIndexBuild {
    fn pause(&self) {
        let mut state = self.state.lock().expect("pause state lock poisoned");
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).expect("pause state wait poisoned");
        }
    }

    fn wait_until_reached(&self) {
        let mut state = self.state.lock().expect("pause state lock poisoned");
        while !state.0 {
            state = self.changed.wait(state).expect("pause state wait poisoned");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("pause state lock poisoned");
        state.1 = true;
        self.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barq_core::{FieldSchema, FieldType, PayloadValue};
    use barq_index::{DistanceMetric, DocumentId, HnswParams, IndexType};
    use proptest::prelude::*;
    use std::ffi::OsString;
    use std::path::Path;

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

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

    fn sample_document(id: u64) -> Document {
        Document {
            id: DocumentId::U64(id),
            vector: vec![1.0, 0.0, 0.0],
            payload: Some(PayloadValue::String("doc".into())),
        }
    }

    fn sample_large_document(id: u64, payload_size: usize) -> Document {
        Document {
            id: DocumentId::U64(id),
            vector: vec![1.0, 0.0, 0.0],
            payload: Some(PayloadValue::String("x".repeat(payload_size))),
        }
    }

    fn sample_wide_schema(name: &str, dimension: usize) -> CollectionSchema {
        CollectionSchema {
            name: name.to_string(),
            fields: vec![FieldSchema {
                name: "vector".to_string(),
                field_type: FieldType::Vector {
                    dimension,
                    metric: DistanceMetric::L2,
                    index: None,
                },
                required: true,
            }],
            bm25_config: None,
            tenant_id: TenantId::default(),
        }
    }

    fn sample_wide_document(id: u64, dimension: usize) -> Document {
        Document {
            id: DocumentId::U64(id),
            vector: vec![1.0; dimension],
            payload: None,
        }
    }

    fn read_segment_index_metadata(
        root: &tempfile::TempDir,
        collection: &str,
        file_name: &str,
    ) -> SegmentIndexMetadata {
        let metadata_path = root.path().join(format!(
            "tenants/default/collections/{collection}/segments/{file_name}.meta.json"
        ));
        serde_json::from_str(&std::fs::read_to_string(metadata_path).unwrap()).unwrap()
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
    fn snapshot_round_trip_restores_collections() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();

        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.create_collection(sample_schema("items")).unwrap();
            storage
                .insert(
                    "items",
                    Document {
                        id: DocumentId::U64(42),
                        vector: vec![0.0, 1.0, 0.0],
                        payload: Some(PayloadValue::String("snapshot".into())),
                    },
                    false,
                )
                .unwrap();
            storage.create_snapshot(snapshot_dir.path()).unwrap();
        }

        let new_root = tempfile::tempdir().unwrap();
        Storage::restore_snapshot(new_root.path(), snapshot_dir.path()).unwrap();
        let mut restored = Storage::open(new_root.path()).unwrap();
        let results = restored.search("items", &[0.0, 1.0, 0.0], 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(42));
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
        storage.wait_for_pending_index_builds().unwrap();

        let (_, _, index_type) = storage
            .collection_schema("items")
            .unwrap()
            .vector_config()
            .unwrap();
        assert!(matches!(index_type, IndexType::Hnsw(_)));
    }

    #[test]
    fn explicit_reindex_transitions_ready_to_building_to_ready() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage.create_collection(sample_schema("items")).unwrap();
        storage.insert("items", sample_document(1), false).unwrap();

        storage
            .rebuild_index("items", Some(IndexType::Flat))
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();
        assert_eq!(
            storage.collection_index_state(&TenantId::default(), "items"),
            IndexState::Ready
        );
        assert_eq!(
            storage.collection_index_version(&TenantId::default(), "items"),
            1
        );

        let hook = storage.install_pause_before_index_build();
        storage
            .rebuild_index("items", Some(IndexType::Hnsw(HnswParams::default())))
            .unwrap();
        hook.wait_until_reached();
        assert_eq!(
            storage.collection_index_state(&TenantId::default(), "items"),
            IndexState::Building
        );

        hook.release();
        storage.wait_for_pending_index_builds().unwrap();
        assert_eq!(
            storage.collection_index_state(&TenantId::default(), "items"),
            IndexState::Ready
        );
        assert_eq!(
            storage.collection_index_version(&TenantId::default(), "items"),
            2
        );

        let (_, _, index_type) = storage
            .collection_schema("items")
            .unwrap()
            .vector_config()
            .unwrap();
        assert!(matches!(index_type, IndexType::Hnsw(_)));
    }

    #[test]
    fn explicit_reindex_preserves_search_correctness() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage.create_collection(sample_schema("items")).unwrap();
        storage.insert("items", sample_document(1), false).unwrap();
        storage
            .insert(
                "items",
                Document {
                    id: DocumentId::U64(2),
                    vector: vec![0.0, 1.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();

        let baseline = storage.search("items", &[0.0, 1.0, 0.0], 2, None).unwrap();

        storage
            .rebuild_index("items", Some(IndexType::Flat))
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();

        let rebuilt = storage.search("items", &[0.0, 1.0, 0.0], 2, None).unwrap();
        assert_eq!(rebuilt, baseline);
    }

    #[test]
    fn index_version_increments_after_successful_rebuilds() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage
            .create_collection(sample_schema("versioned"))
            .unwrap();
        storage
            .insert("versioned", sample_document(1), false)
            .unwrap();

        storage
            .rebuild_index("versioned", Some(IndexType::Flat))
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();
        assert_eq!(
            storage.collection_index_version(&TenantId::default(), "versioned"),
            1
        );

        storage
            .rebuild_index("versioned", Some(IndexType::Hnsw(HnswParams::default())))
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();
        assert_eq!(
            storage.collection_index_version(&TenantId::default(), "versioned"),
            2
        );

        let lifecycle = storage
            .load_collection_lifecycle_state(&TenantId::default(), "versioned")
            .unwrap()
            .unwrap();
        assert_eq!(lifecycle.index_version, 2);
        assert_eq!(lifecycle.index_state, IndexState::Ready);
    }

    #[test]
    fn snapshot_uploads_to_object_store() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        let schema = sample_schema("upload");
        storage.create_collection(schema.clone()).unwrap();
        let doc = sample_document(1);
        storage.insert(&schema.name, doc.clone(), false).unwrap();

        let snapshot_dir = tempfile::tempdir().unwrap();
        let upload_root = tempfile::tempdir().unwrap();
        let object_store = LocalObjectStore::new(upload_root.path()).unwrap();
        let handle = storage
            .create_snapshot_and_upload(snapshot_dir.path(), object_store.clone(), "snapshots/test")
            .unwrap();
        handle.join().unwrap().unwrap();

        let uploaded_dir = upload_root.path().join("snapshots/test");
        assert!(uploaded_dir.exists());
        assert!(uploaded_dir.join("snapshot.json").exists());
        assert!(uploaded_dir.join("tenants").exists());
    }

    #[test]
    fn snapshot_can_be_restored_from_object_store() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        let schema = sample_schema("download");
        storage.create_collection(schema.clone()).unwrap();
        let doc = sample_document(5);
        storage.insert(&schema.name, doc.clone(), false).unwrap();

        let snapshot_dir = tempfile::tempdir().unwrap();
        let upload_root = tempfile::tempdir().unwrap();
        let object_store = LocalObjectStore::new(upload_root.path()).unwrap();
        storage
            .create_snapshot(snapshot_dir.path())
            .expect("snapshot should succeed");
        object_store
            .upload_dir(snapshot_dir.path(), Path::new("snapshots/restore"))
            .expect("upload should succeed");

        let restore_root = tempfile::tempdir().unwrap();
        Storage::restore_snapshot_from_object_store(
            restore_root.path(),
            tempfile::tempdir().unwrap().path(),
            object_store,
            Path::new("snapshots/restore"),
        )
        .expect("restore should succeed");

        let mut restored = Storage::open(restore_root.path()).unwrap();
        let results = restored.search(&schema.name, &doc.vector, 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, doc.id);
    }

    #[test]
    fn flushes_wal_to_segments_and_recovers() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        {
            let mut storage = Storage::open(root.path()).unwrap();
            storage.create_collection(sample_schema("items")).unwrap();
            storage
                .insert(
                    "items",
                    Document {
                        id: DocumentId::U64(7),
                        vector: vec![0.0, 0.0, 1.0],
                        payload: Some(PayloadValue::String("persisted".into())),
                    },
                    false,
                )
                .unwrap();
            storage
                .flush_wal_to_segment(&tenant, "items")
                .expect("flush should succeed");
        }

        let mut reopened = Storage::open(root.path()).unwrap();
        let results = reopened.search("items", &[0.0, 0.0, 1.0], 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(7));
    }

    #[test]
    fn compaction_keeps_latest_versions() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage.create_collection(sample_schema("items")).unwrap();

        storage
            .insert(
                "items",
                Document {
                    id: DocumentId::U64(1),
                    vector: vec![1.0, 0.0, 0.0],
                    payload: Some(PayloadValue::String("v1".into())),
                },
                false,
            )
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "items").unwrap();

        storage
            .insert(
                "items",
                Document {
                    id: DocumentId::U64(1),
                    vector: vec![1.0, 1.0, 0.0],
                    payload: Some(PayloadValue::String("v2".into())),
                },
                true,
            )
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "items").unwrap();

        let meta = storage
            .compact_segments(&tenant, "items")
            .expect("compaction should succeed");
        assert!(meta.entries >= 1);

        drop(storage);
        let mut restored = Storage::open(root.path()).unwrap();
        let results = restored.search("items", &[1.0, 1.0, 0.0], 1, None).unwrap();
        assert_eq!(results.len(), 1);
        let segments_dir = root
            .path()
            .join("tenants")
            .join(tenant.as_str())
            .join("collections/items/segments");
        let files: Vec<_> = std::fs::read_dir(&segments_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|f| f.is_file()).unwrap_or(false))
            .collect();
        assert_eq!(files.len(), 1, "compaction should leave one segment");
    }

    #[test]
    fn uploads_cold_segments_in_background() {
        let root = tempfile::tempdir().unwrap();
        let upload_root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        let schema = sample_schema("bg_upload");
        storage.create_collection(schema.clone()).unwrap();
        storage
            .insert(&schema.name, sample_document(9), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, &schema.name).unwrap();

        let object_store = LocalObjectStore::new(upload_root.path()).unwrap();
        let handle = storage
            .upload_cold_segments(
                &tenant,
                &schema.name,
                object_store.clone(),
                "segments/upload",
            )
            .expect("upload should spawn");
        handle.join().unwrap().unwrap();

        let uploaded_dir = upload_root.path().join("segments/upload");
        assert!(uploaded_dir.exists());
        let files: Vec<_> = std::fs::read_dir(&uploaded_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|f| f.is_file()).unwrap_or(false))
            .collect();
        assert!(!files.is_empty());
    }

    #[test]
    fn snapshot_round_trip_large_backup() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let upload_root = tempfile::tempdir().unwrap();

        let mut storage = Storage::open(dir.path()).unwrap();
        let schema = sample_schema("large");
        storage.create_collection(schema.clone()).unwrap();
        for id in 1..=200 {
            storage
                .insert(
                    &schema.name,
                    Document {
                        id: DocumentId::U64(id),
                        vector: vec![id as f32, 0.0, 0.0],
                        payload: Some(PayloadValue::String(format!("doc-{id}"))),
                    },
                    false,
                )
                .unwrap();
        }
        storage
            .flush_wal_to_segment(&TenantId::default(), &schema.name)
            .unwrap();
        storage
            .create_snapshot(snapshot_dir.path())
            .expect("snapshot should succeed");
        let object_store = LocalObjectStore::new(upload_root.path()).unwrap();
        object_store
            .upload_dir(snapshot_dir.path(), std::path::Path::new("snapshots/large"))
            .unwrap();

        let restore_root = tempfile::tempdir().unwrap();
        Storage::restore_snapshot_from_object_store(
            restore_root.path(),
            tempfile::tempdir().unwrap().path(),
            object_store,
            std::path::Path::new("snapshots/large"),
        )
        .expect("restore should succeed");

        let mut restored = Storage::open(restore_root.path()).unwrap();
        let results = restored
            .search(&schema.name, &[199.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(199));
    }

    #[test]
    fn segment_state_defaults_to_growing() {
        let metadata = SegmentMetadata {
            file_name: "segment_1.jsonl".to_string(),
            created_at: Utc::now(),
            entries: 0,
            state: SegmentState::default(),
        };
        assert_eq!(metadata.state, SegmentState::Growing);
    }

    #[test]
    fn segment_state_rejects_invalid_transitions() {
        assert!(SegmentState::Growing
            .transition_to(SegmentState::Compacted)
            .is_err());
        assert!(SegmentState::Compacted
            .transition_to(SegmentState::Sealed)
            .is_err());
        assert!(SegmentState::Sealed
            .transition_to(SegmentState::Growing)
            .is_err());
    }

    #[test]
    fn segment_state_serializes_and_deserializes() {
        let encoded = serde_json::to_string(&SegmentMetadata {
            file_name: "segment_2.jsonl".to_string(),
            created_at: Utc::now(),
            entries: 11,
            state: SegmentState::Sealed,
        })
        .expect("serialization should work");
        let decoded: SegmentMetadata =
            serde_json::from_str(&encoded).expect("deserialization should work");
        assert_eq!(decoded.state, SegmentState::Sealed);
    }

    #[test]
    fn growing_segment_accepts_writes_and_reads() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage.create_collection(sample_schema("grow")).unwrap();
        assert_eq!(
            storage.segment_state_for_tenant(&TenantId::default(), "grow"),
            SegmentState::Growing
        );

        storage
            .insert("grow", sample_document(1), false)
            .expect("write should be accepted for growing segment");

        let results = storage.search("grow", &[1.0, 0.0, 0.0], 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(1));
    }

    #[test]
    fn growing_segment_delete_update_and_duplicates_behave() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage.create_collection(sample_schema("grow2")).unwrap();

        storage.insert("grow2", sample_document(5), false).unwrap();
        storage
            .insert("grow2", sample_document(5), false)
            .expect("duplicate id insert should be accepted with current semantics");

        storage.insert("grow2", sample_document(5), true).unwrap();
        let deleted = storage.delete("grow2", DocumentId::U64(5)).unwrap();
        assert!(deleted, "existing document should be deleted");
        let _ = storage.delete("grow2", DocumentId::U64(5)).unwrap();
        let results = storage.search("grow2", &[5.0, 0.0, 0.0], 1, None).unwrap();
        assert!(
            results.is_empty(),
            "document should not remain after deletes"
        );
    }

    #[test]
    fn segment_auto_seals_when_threshold_reached() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open_with_options(
            root.path(),
            StorageOptions {
                auto_seal_threshold: 2,
                ..StorageOptions::default()
            },
        )
        .unwrap();
        storage
            .create_collection(sample_schema("seal_auto"))
            .unwrap();
        storage
            .insert("seal_auto", sample_document(1), false)
            .unwrap();
        assert_eq!(
            storage.segment_state_for_tenant(&TenantId::default(), "seal_auto"),
            SegmentState::Growing
        );
        storage
            .insert("seal_auto", sample_document(2), false)
            .unwrap();
        assert_eq!(
            storage.segment_state_for_tenant(&TenantId::default(), "seal_auto"),
            SegmentState::Growing,
            "auto seal rotates back to a new growing segment"
        );
    }

    #[test]
    fn segment_can_be_manually_sealed_and_becomes_read_only() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("seal_manual"))
            .unwrap();
        storage
            .insert("seal_manual", sample_document(3), false)
            .unwrap();
        storage
            .seal_segment_for_tenant(&TenantId::default(), "seal_manual")
            .unwrap();
        assert_eq!(
            storage.segment_state_for_tenant(&TenantId::default(), "seal_manual"),
            SegmentState::Sealed
        );

        let write_err = storage.insert("seal_manual", sample_document(4), false);
        assert!(matches!(
            write_err,
            Err(StorageError::SegmentNotWritable { .. })
        ));

        let results = storage
            .search("seal_manual", &[3.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1, "reads still work after sealing");
    }

    #[test]
    fn collection_lifecycle_persists_index_state_metadata() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("lifecycle"))
            .unwrap();

        storage
            .set_collection_index_state(&tenant, "lifecycle", IndexState::Stale)
            .unwrap();

        let lifecycle = storage
            .load_collection_lifecycle_state(&tenant, "lifecycle")
            .unwrap()
            .unwrap();
        assert_eq!(lifecycle.segment_state, SegmentState::Growing);
        assert_eq!(lifecycle.index_state, IndexState::Stale);
        assert_eq!(lifecycle.index_version, 0);
    }

    #[test]
    fn legacy_lifecycle_metadata_without_version_loads_safely() {
        let root = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(root.path()).unwrap();
            storage.create_collection(sample_schema("legacy")).unwrap();
        }

        let lifecycle_path = root
            .path()
            .join("tenants/default/collections/legacy/lifecycle_state.json");
        std::fs::write(
            &lifecycle_path,
            r#"{
  "segment_state": "Growing",
  "index_state": "Ready"
}"#,
        )
        .unwrap();

        let reopened = Storage::open(root.path()).unwrap();
        assert_eq!(
            reopened.collection_index_state(&TenantId::default(), "legacy"),
            IndexState::Ready
        );
        assert_eq!(
            reopened.collection_index_version(&TenantId::default(), "legacy"),
            0
        );
    }

    #[test]
    fn sealing_starts_async_index_build_and_persists_building_metadata() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage.create_collection(sample_schema("indexed")).unwrap();
        storage
            .insert("indexed", sample_document(8), false)
            .unwrap();
        let hook = storage.install_pause_before_index_build();
        let sealed = storage
            .seal_segment_for_tenant(&TenantId::default(), "indexed")
            .unwrap();
        assert_eq!(sealed.state, SegmentState::Sealed);
        hook.wait_until_reached();

        let metadata_path = root
            .path()
            .join("tenants/default/collections/indexed/segments")
            .join(format!("{}.meta.json", sealed.file_name));
        assert!(
            metadata_path.exists(),
            "sealed metadata should be persisted"
        );
        let data = std::fs::read_to_string(metadata_path).unwrap();
        let parsed: SegmentIndexMetadata = serde_json::from_str(&data).unwrap();
        assert!(!parsed.index_built, "async build should still be pending");
        assert_eq!(parsed.index_state, IndexState::Building);
        assert_eq!(
            storage.collection_index_state(&TenantId::default(), "indexed"),
            IndexState::Building
        );

        hook.release();
        storage.wait_for_pending_index_builds().unwrap();
    }

    #[test]
    fn segment_is_searchable_after_async_index_build_completes() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("indexed_ready"))
            .unwrap();
        storage
            .insert("indexed_ready", sample_document(12), false)
            .unwrap();
        storage
            .seal_segment_for_tenant(&TenantId::default(), "indexed_ready")
            .unwrap();

        storage.wait_for_pending_index_builds().unwrap();

        let results = storage
            .search("indexed_ready", &[1.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(12));
    }

    #[test]
    fn async_index_build_transitions_to_ready_after_success() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("indexed_ready_state"))
            .unwrap();
        storage
            .insert("indexed_ready_state", sample_document(14), false)
            .unwrap();
        let sealed = storage
            .seal_segment_for_tenant(&TenantId::default(), "indexed_ready_state")
            .unwrap();

        storage.wait_for_pending_index_builds().unwrap();

        let metadata_path = root
            .path()
            .join("tenants/default/collections/indexed_ready_state/segments")
            .join(format!("{}.meta.json", sealed.file_name));
        let parsed: SegmentIndexMetadata =
            serde_json::from_str(&std::fs::read_to_string(metadata_path).unwrap()).unwrap();
        assert!(parsed.index_built);
        assert_eq!(parsed.index_state, IndexState::Ready);
        assert_eq!(parsed.index_version, 1);
        assert_eq!(
            storage.collection_index_state(&TenantId::default(), "indexed_ready_state"),
            IndexState::Ready
        );
        assert_eq!(
            storage.collection_index_version(&TenantId::default(), "indexed_ready_state"),
            1
        );
    }

    #[test]
    fn interrupted_index_build_recovery_loads_persisted_building_state_safely() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let hook;
        let sealed_file_name;
        {
            let mut storage = Storage::open(root.path()).unwrap();
            storage
                .create_collection(sample_schema("indexed_restart"))
                .unwrap();
            storage
                .insert("indexed_restart", sample_document(22), false)
                .unwrap();
            hook = storage.install_pause_before_index_build();
            let sealed = storage
                .seal_segment_for_tenant(&tenant, "indexed_restart")
                .unwrap();
            sealed_file_name = sealed.file_name.clone();
            hook.wait_until_reached();

            let lifecycle = storage
                .load_collection_lifecycle_state(&tenant, "indexed_restart")
                .unwrap()
                .unwrap();
            assert_eq!(lifecycle.index_state, IndexState::Building);
            let metadata = read_segment_index_metadata(&root, "indexed_restart", &sealed_file_name);
            assert_eq!(metadata.index_state, IndexState::Building);
            assert!(!metadata.index_built);
        }

        let mut reopened = Storage::open(root.path()).unwrap();
        assert_eq!(
            reopened.collection_index_state(&tenant, "indexed_restart"),
            IndexState::Building
        );
        let results = reopened
            .search("indexed_restart", &[1.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(22));

        hook.release();
    }

    #[test]
    fn newly_sealed_segment_is_marked_stale_until_build_completes() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("stale_plan"))
            .unwrap();
        storage
            .insert("stale_plan", sample_document(1), false)
            .unwrap();
        let first = storage
            .seal_current_segment_for_tenant(&tenant, "stale_plan", true)
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();

        storage
            .insert(
                "stale_plan",
                Document {
                    id: DocumentId::U64(2),
                    vector: vec![0.0, 1.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();
        let hook = storage.install_pause_before_index_build();
        let second = storage
            .seal_segment_for_tenant(&tenant, "stale_plan")
            .unwrap();
        hook.wait_until_reached();

        let lifecycle = storage
            .load_collection_lifecycle_state(&tenant, "stale_plan")
            .unwrap()
            .unwrap();
        assert_eq!(lifecycle.stale_segments, vec![second.file_name.clone()]);

        let first_meta = read_segment_index_metadata(&root, "stale_plan", &first.file_name);
        let second_meta = read_segment_index_metadata(&root, "stale_plan", &second.file_name);
        assert_eq!(first_meta.index_state, IndexState::Ready);
        assert_eq!(first_meta.index_version, 1);
        assert_eq!(second_meta.index_state, IndexState::Building);
        assert_eq!(second_meta.index_version, 1);

        hook.release();
        storage.wait_for_pending_index_builds().unwrap();
    }

    #[test]
    fn only_stale_segment_metadata_is_advanced_on_rebuild_completion() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("stale_apply"))
            .unwrap();
        storage
            .insert("stale_apply", sample_document(1), false)
            .unwrap();
        let first = storage
            .seal_current_segment_for_tenant(&tenant, "stale_apply", true)
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();

        storage
            .insert(
                "stale_apply",
                Document {
                    id: DocumentId::U64(2),
                    vector: vec![0.0, 1.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();
        let second = storage
            .seal_segment_for_tenant(&tenant, "stale_apply")
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();

        let lifecycle = storage
            .load_collection_lifecycle_state(&tenant, "stale_apply")
            .unwrap()
            .unwrap();
        assert!(lifecycle.stale_segments.is_empty());

        let first_meta = read_segment_index_metadata(&root, "stale_apply", &first.file_name);
        let second_meta = read_segment_index_metadata(&root, "stale_apply", &second.file_name);
        assert_eq!(first_meta.index_version, 1);
        assert_eq!(first_meta.index_state, IndexState::Ready);
        assert_eq!(second_meta.index_version, 2);
        assert_eq!(second_meta.index_state, IndexState::Ready);
    }

    #[test]
    fn search_stays_correct_while_new_segment_build_is_in_flight() {
        let tenant = TenantId::default();
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("search_during_build"))
            .unwrap();
        storage
            .insert("search_during_build", sample_document(1), false)
            .unwrap();
        storage
            .seal_current_segment_for_tenant(&tenant, "search_during_build", true)
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();

        storage
            .insert(
                "search_during_build",
                Document {
                    id: DocumentId::U64(2),
                    vector: vec![0.0, 1.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();
        let hook = storage.install_pause_before_index_build();
        storage
            .seal_segment_for_tenant(&tenant, "search_during_build")
            .unwrap();
        hook.wait_until_reached();

        let results = storage
            .search("search_during_build", &[0.0, 1.0, 0.0], 2, None)
            .unwrap();
        let ids: Vec<_> = results.into_iter().map(|result| result.id).collect();
        assert_eq!(ids, vec![DocumentId::U64(2), DocumentId::U64(1)]);

        hook.release();
        storage.wait_for_pending_index_builds().unwrap();
    }

    #[test]
    fn index_build_failure_falls_back_without_breaking_search() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("indexed_fallback"))
            .unwrap();
        storage
            .insert("indexed_fallback", sample_document(10), false)
            .unwrap();
        storage.fail_next_index_build();
        let sealed = storage
            .seal_segment_for_tenant(&TenantId::default(), "indexed_fallback")
            .unwrap();
        storage.wait_for_pending_index_builds().unwrap();

        let metadata_path = root
            .path()
            .join("tenants/default/collections/indexed_fallback/segments")
            .join(format!("{}.meta.json", sealed.file_name));
        let parsed: SegmentIndexMetadata =
            serde_json::from_str(&std::fs::read_to_string(metadata_path).unwrap()).unwrap();
        assert!(!parsed.index_built);
        assert_eq!(parsed.index_state, IndexState::Stale);
        assert_eq!(parsed.index_version, 0);
        assert_eq!(
            storage.collection_index_state(&TenantId::default(), "indexed_fallback"),
            IndexState::Stale
        );
        assert_eq!(
            storage.collection_index_version(&TenantId::default(), "indexed_fallback"),
            0
        );

        let results = storage
            .search("indexed_fallback", &[10.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, DocumentId::U64(10));
    }

    #[test]
    fn compaction_drops_tombstones_and_keeps_live_documents() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("compact_live"))
            .unwrap();

        storage
            .insert("compact_live", sample_document(1), false)
            .unwrap();
        storage
            .insert("compact_live", sample_document(2), false)
            .unwrap();
        storage
            .flush_wal_to_segment(&tenant, "compact_live")
            .unwrap();
        storage.delete("compact_live", DocumentId::U64(1)).unwrap();
        storage
            .flush_wal_to_segment(&tenant, "compact_live")
            .unwrap();

        storage.compact_segments(&tenant, "compact_live").unwrap();
        let missing = storage
            .get_document(&tenant, "compact_live", &DocumentId::U64(1))
            .unwrap();
        let live = storage
            .search("compact_live", &[2.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert!(missing.is_none(), "deleted documents must not reappear");
        assert_eq!(live.len(), 1, "live documents must survive compaction");
    }

    #[test]
    fn compaction_handles_empty_segments_collection() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("compact_empty"))
            .unwrap();
        let metadata = storage.compact_segments(&tenant, "compact_empty").unwrap();
        assert_eq!(metadata.entries, 0);
        assert!(metadata.file_name.is_empty());
    }

    #[test]
    fn search_spans_growing_and_sealed_segments_deterministically() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("multi_seg"))
            .unwrap();

        storage
            .insert("multi_seg", sample_document(1), false)
            .unwrap();
        storage
            .insert("multi_seg", sample_document(2), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "multi_seg").unwrap();
        storage
            .insert("multi_seg", sample_document(3), false)
            .unwrap(); // growing

        let first = storage
            .search("multi_seg", &[2.5, 0.0, 0.0], 3, None)
            .unwrap();
        let second = storage
            .search("multi_seg", &[2.5, 0.0, 0.0], 3, None)
            .unwrap();
        assert_eq!(first.len(), 3);
        assert_eq!(first, second, "merged top-k ordering must be deterministic");
    }

    #[test]
    fn search_spans_multiple_sealed_segments() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("multi_sealed"))
            .unwrap();

        storage
            .insert("multi_sealed", sample_document(10), false)
            .unwrap();
        storage
            .flush_wal_to_segment(&tenant, "multi_sealed")
            .unwrap();
        storage
            .insert("multi_sealed", sample_document(11), false)
            .unwrap();
        storage
            .flush_wal_to_segment(&tenant, "multi_sealed")
            .unwrap();

        let results = storage
            .search("multi_sealed", &[11.0, 0.0, 0.0], 2, None)
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn search_across_segments_breaks_ties_by_document_id() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("tie_segments"))
            .unwrap();

        storage
            .insert(
                "tie_segments",
                Document {
                    id: DocumentId::U64(10),
                    vector: vec![1.0, 0.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();
        storage
            .flush_wal_to_segment(&tenant, "tie_segments")
            .unwrap();
        storage
            .insert(
                "tie_segments",
                Document {
                    id: DocumentId::U64(2),
                    vector: vec![1.0, 0.0, 0.0],
                    payload: None,
                },
                false,
            )
            .unwrap();

        let results = storage
            .search("tie_segments", &[1.0, 0.0, 0.0], 2, None)
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, DocumentId::U64(10));
        assert_eq!(results[1].id, DocumentId::U64(2));
    }

    #[test]
    fn background_worker_compacts_eligible_segments() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("bg_compact"))
            .unwrap();
        storage
            .insert("bg_compact", sample_document(1), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "bg_compact").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        storage
            .insert("bg_compact", sample_document(2), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "bg_compact").unwrap();

        let compacted = storage
            .run_background_compaction_once(tenant.clone(), 2)
            .join()
            .unwrap()
            .unwrap();
        assert_eq!(compacted, vec!["bg_compact".to_string()]);
    }

    #[test]
    fn background_worker_skips_ineligible_segments() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage.create_collection(sample_schema("bg_skip")).unwrap();
        storage
            .insert("bg_skip", sample_document(1), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "bg_skip").unwrap();

        let compacted = storage
            .run_background_compaction_once(tenant, 2)
            .join()
            .unwrap()
            .unwrap();
        assert!(compacted.is_empty());
    }

    #[test]
    fn concurrent_reads_during_background_compaction_remain_correct() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("bg_concurrent"))
            .unwrap();
        for id in 1..=4 {
            storage
                .insert("bg_concurrent", sample_document(id), false)
                .unwrap();
            storage
                .flush_wal_to_segment(&tenant, "bg_concurrent")
                .unwrap();
        }

        let path = root.path().to_path_buf();
        let reader = std::thread::spawn(move || {
            let mut reader_storage = Storage::open(&path).unwrap();
            let res = reader_storage
                .search("bg_concurrent", &[4.0, 0.0, 0.0], 1, None)
                .unwrap();
            assert!(!res.is_empty(), "reads should succeed during compaction");
        });

        storage
            .run_background_compaction_once(tenant, 2)
            .join()
            .unwrap()
            .unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn background_compaction_failure_keeps_original_segments() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage.create_collection(sample_schema("bg_fail")).unwrap();
        storage
            .insert("bg_fail", sample_document(1), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "bg_fail").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        storage
            .insert("bg_fail", sample_document(2), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "bg_fail").unwrap();

        let before = storage.segment_files(&tenant, "bg_fail").unwrap().len();
        assert!(before >= 2, "test requires at least two segments");
        storage.fail_next_background_compaction();
        let outcome = storage
            .run_background_compaction_once(tenant.clone(), 2)
            .join()
            .unwrap();
        assert!(outcome.is_err());
        let after = storage.segment_files(&tenant, "bg_fail").unwrap().len();
        assert_eq!(
            before, after,
            "failed compaction should not delete originals"
        );
    }

    #[test]
    fn background_compaction_loop_compacts_until_stopped() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage.create_collection(sample_schema("bg_loop")).unwrap();
        storage
            .insert("bg_loop", sample_document(1), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "bg_loop").unwrap();
        storage
            .insert("bg_loop", sample_document(2), false)
            .unwrap();
        storage.flush_wal_to_segment(&tenant, "bg_loop").unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let handle = storage.run_background_compaction_loop(
            tenant.clone(),
            2,
            Duration::from_millis(10),
            stop.clone(),
        );
        std::thread::sleep(Duration::from_millis(40));
        stop.store(true, Ordering::Relaxed);
        let compacted = handle.join().unwrap().unwrap();
        assert!(
            compacted.iter().any(|name| name == "bg_loop"),
            "loop worker should compact eligible collection"
        );
    }

    #[test]
    fn background_compaction_loop_skips_ineligible_collections() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("bg_loop_skip"))
            .unwrap();
        storage
            .insert("bg_loop_skip", sample_document(1), false)
            .unwrap();
        storage
            .flush_wal_to_segment(&tenant, "bg_loop_skip")
            .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let handle = storage.run_background_compaction_loop(
            tenant,
            2,
            Duration::from_millis(10),
            stop.clone(),
        );
        std::thread::sleep(Duration::from_millis(30));
        stop.store(true, Ordering::Relaxed);
        let compacted = handle.join().unwrap().unwrap();
        assert!(
            !compacted.iter().any(|name| name == "bg_loop_skip"),
            "loop worker should skip collections under threshold"
        );
    }

    #[test]
    fn lifecycle_state_replays_across_restart_after_mixed_transitions() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        {
            let mut storage = Storage::open_with_options(
                root.path(),
                StorageOptions {
                    auto_seal_threshold: 2,
                    ..StorageOptions::default()
                },
            )
            .unwrap();
            storage.create_collection(sample_schema("mixed")).unwrap();
            storage.insert("mixed", sample_document(1), false).unwrap();
            storage.insert("mixed", sample_document(2), false).unwrap(); // auto seal + rotate
            storage.insert("mixed", sample_document(3), false).unwrap();
            storage.seal_segment_for_tenant(&tenant, "mixed").unwrap();
            let compacted = storage.compact_segments(&tenant, "mixed").unwrap();
            assert_eq!(compacted.state, SegmentState::Compacted);
        }

        let mut reopened = Storage::open(root.path()).unwrap();
        assert_eq!(
            reopened.segment_state_for_tenant(&tenant, "mixed"),
            SegmentState::Sealed
        );
        let write = reopened.insert("mixed", sample_document(99), false);
        assert!(matches!(
            write,
            Err(StorageError::SegmentNotWritable { .. })
        ));

        let results = reopened.search("mixed", &[3.0, 0.0, 0.0], 3, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn memory_limit_enforced_across_lifecycle_rotations() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open_with_options(
            root.path(),
            StorageOptions {
                auto_seal_threshold: 1,
                ..StorageOptions::default()
            },
        )
        .unwrap();
        storage
            .create_collection(sample_schema("mem_limit"))
            .unwrap();
        storage.set_tenant_quota(
            tenant.clone(),
            TenantQuota {
                max_memory_bytes: Some(1_500),
                ..TenantQuota::default()
            },
        );

        storage
            .insert("mem_limit", sample_large_document(1, 900), false)
            .unwrap();
        assert_eq!(
            storage.segment_state_for_tenant(&tenant, "mem_limit"),
            SegmentState::Growing,
            "auto-seal should rotate back to growing"
        );

        let second = storage.insert("mem_limit", sample_large_document(2, 900), false);
        assert!(matches!(second, Err(StorageError::QuotaExceeded { .. })));
    }

    #[test]
    fn memory_gauge_updates_on_insert() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("memory_insert"))
            .unwrap();

        let before = storage.memory_metrics_snapshot();
        storage
            .insert("memory_insert", sample_document(1), false)
            .unwrap();
        let after = storage.memory_metrics_snapshot();

        assert!(after.refresh_count > before.refresh_count);
        assert_eq!(
            after
                .collection_memory_bytes("default", "memory_insert")
                .unwrap(),
            3 * std::mem::size_of::<f32>() as u64
        );
        assert_eq!(
            after.tenant_memory_bytes("default").unwrap(),
            after.total_resident_vector_memory_bytes
        );
    }

    #[test]
    fn memory_gauge_refreshes_on_wal_flush() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("memory_flush"))
            .unwrap();
        storage
            .insert("memory_flush", sample_document(1), false)
            .unwrap();

        let before = storage.memory_metrics_snapshot();
        storage
            .flush_wal_to_segment(&tenant, "memory_flush")
            .unwrap();
        let after = storage.memory_metrics_snapshot();

        assert!(after.refresh_count > before.refresh_count);
        assert_eq!(
            after.collection_memory_bytes("default", "memory_flush"),
            before.collection_memory_bytes("default", "memory_flush")
        );
    }

    #[test]
    fn memory_gauge_tracks_budgeted_vector_eviction() {
        let _guard = process_env_lock();
        let root = tempfile::tempdir().unwrap();
        let _mode = EnvVarGuard::set("BARQ_VECTOR_STORE_MODE", "mmap");
        let _path = EnvVarGuard::set("BARQ_VECTOR_STORE_PATH", root.path());
        let _memory = EnvVarGuard::set("BARQ_MAX_MEMORY_MB", "1");

        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_wide_schema("memory_evict", 150_000))
            .unwrap();
        storage
            .insert("memory_evict", sample_wide_document(1, 150_000), false)
            .unwrap();
        storage
            .insert("memory_evict", sample_wide_document(2, 150_000), false)
            .unwrap();
        let snapshot = storage.memory_metrics_snapshot();

        let resident = snapshot
            .collection_memory_bytes("default", "memory_evict")
            .unwrap();
        let inserted_bytes = 2 * 150_000 * std::mem::size_of::<f32>() as u64;
        assert!(resident > 0);
        assert!(resident < inserted_bytes);
        assert!(resident <= 1_024 * 1_024);
    }

    #[test]
    fn wal_metrics_increment_on_append() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("wal_metrics"))
            .unwrap();

        let before = storage.memory_metrics_snapshot();
        storage
            .insert("wal_metrics", sample_document(1), false)
            .unwrap();
        let after = storage.memory_metrics_snapshot();

        assert_eq!(after.wal_appends_total, before.wal_appends_total + 1);
        assert!(
            after.wal_bytes_written_total > before.wal_bytes_written_total,
            "WAL byte counter should grow after an append"
        );
        assert_eq!(
            after.collection_wal_entries("default", "wal_metrics"),
            Some(1)
        );
        assert!(
            after
                .collection_wal_bytes("default", "wal_metrics")
                .unwrap_or_default()
                > 0
        );
    }

    #[test]
    fn segment_metrics_track_file_counts_and_current_state() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("segment_metrics"))
            .unwrap();

        let created = storage.memory_metrics_snapshot();
        assert!(created.collection_segment_state_active(
            "default",
            "segment_metrics",
            SegmentState::Growing,
        ));
        assert_eq!(
            created.collection_segment_file_count(
                "default",
                "segment_metrics",
                SegmentState::Sealed,
            ),
            Some(0)
        );

        storage
            .insert("segment_metrics", sample_document(1), false)
            .unwrap();
        storage
            .seal_segment_for_tenant(&tenant, "segment_metrics")
            .unwrap();
        let sealed = storage.memory_metrics_snapshot();

        assert!(sealed.collection_segment_state_active(
            "default",
            "segment_metrics",
            SegmentState::Sealed,
        ));
        assert!(!sealed.collection_segment_state_active(
            "default",
            "segment_metrics",
            SegmentState::Growing,
        ));
        assert_eq!(
            sealed.collection_segment_file_count(
                "default",
                "segment_metrics",
                SegmentState::Sealed,
            ),
            Some(1)
        );
    }

    #[test]
    fn compaction_metrics_increment_and_update_segment_counts() {
        let root = tempfile::tempdir().unwrap();
        let tenant = TenantId::default();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("compaction_metrics"))
            .unwrap();
        storage
            .insert("compaction_metrics", sample_document(1), false)
            .unwrap();
        storage
            .flush_wal_to_segment(&tenant, "compaction_metrics")
            .unwrap();
        storage
            .insert("compaction_metrics", sample_document(2), false)
            .unwrap();
        storage
            .flush_wal_to_segment(&tenant, "compaction_metrics")
            .unwrap();

        let before = storage.memory_metrics_snapshot();
        storage
            .compact_segments(&tenant, "compaction_metrics")
            .unwrap();
        let after = storage.memory_metrics_snapshot();

        assert_eq!(after.compactions_total, before.compactions_total + 1);
        assert_eq!(
            after.collection_segment_file_count(
                "default",
                "compaction_metrics",
                SegmentState::Sealed,
            ),
            Some(0)
        );
        assert_eq!(
            after.collection_segment_file_count(
                "default",
                "compaction_metrics",
                SegmentState::Compacted,
            ),
            Some(1)
        );
    }

    #[test]
    fn metrics_report_json_includes_expected_storage_fields() {
        let root = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(root.path()).unwrap();
        storage
            .create_collection(sample_schema("report_metrics"))
            .unwrap();
        storage
            .insert("report_metrics", sample_document(1), false)
            .unwrap();

        let report = storage.metrics_report_json();
        assert_eq!(report["wal_appends_total"], serde_json::json!(1));
        assert!(
            report["total_resident_vector_memory_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(report["collection_memory_bytes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|sample| {
                sample["tenant"] == serde_json::json!("default")
                    && sample["collection"] == serde_json::json!("report_metrics")
                    && sample["resident_vector_memory_bytes"].as_u64().unwrap() > 0
            }));
        assert!(report["collection_wal"]
            .as_array()
            .unwrap()
            .iter()
            .any(|sample| {
                sample["tenant"] == serde_json::json!("default")
                    && sample["collection"] == serde_json::json!("report_metrics")
                    && sample["entries"].as_u64().unwrap() >= 1
            }));
    }

    proptest! {
        #[test]
        fn compaction_property_latest_write_wins(ops in proptest::collection::vec((1u8..16u8, any::<bool>()), 1..64)) {
            let root = tempfile::tempdir().unwrap();
            let tenant = TenantId::default();
            let mut storage = Storage::open(root.path()).unwrap();
            storage.create_collection(sample_schema("prop_compact")).unwrap();

            for (idx, (doc_id, is_insert)) in ops.iter().copied().enumerate() {
                if is_insert {
                    storage.insert("prop_compact", sample_document(doc_id as u64), true).unwrap();
                } else {
                    let _ = storage.delete("prop_compact", DocumentId::U64(doc_id as u64)).unwrap();
                }

                if idx % 5 == 0 {
                    storage.flush_wal_to_segment(&tenant, "prop_compact").unwrap();
                }
            }
            storage.flush_wal_to_segment(&tenant, "prop_compact").unwrap();
            let expected: Vec<(u8, bool)> = (1u8..16u8)
                .map(|doc_id| {
                    let present = storage
                        .get_document(&tenant, "prop_compact", &DocumentId::U64(doc_id as u64))
                        .unwrap()
                        .is_some();
                    (doc_id, present)
                })
                .collect();
            storage.compact_segments(&tenant, "prop_compact").unwrap();

            drop(storage);
            let reopened = Storage::open(root.path()).unwrap();
            for (doc_id, present) in expected {
                let found = reopened
                    .get_document(&tenant, "prop_compact", &DocumentId::U64(doc_id as u64))
                    .unwrap()
                    .is_some();
                prop_assert_eq!(found, present);
            }
        }
    }
}
