use barq_core::{CollectionSchema, Document, FieldType, PayloadValue, TenantId};
use barq_metrics::{MetricDefinition, MetricKind};
use barq_storage::{Storage, StorageError};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{
    oneshot, Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError,
};

pub(crate) const DEFAULT_INGEST_BATCH_SIZE: usize = 16;
pub(crate) const DEFAULT_INGEST_QUEUE_CAPACITY: usize = 64;

/// Runtime configuration for the asynchronous ingestion worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IngestionConfig {
    pub(crate) batch_size: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) backpressure_policy: BackpressurePolicy,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_INGEST_BATCH_SIZE,
            queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
            backpressure_policy: BackpressurePolicy::Reject,
        }
    }
}

impl IngestionConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            batch_size: parse_env_usize("BARQ_INGEST_BATCH_SIZE", DEFAULT_INGEST_BATCH_SIZE),
            queue_capacity: parse_env_usize(
                "BARQ_INGEST_QUEUE_CAPACITY",
                DEFAULT_INGEST_QUEUE_CAPACITY,
            ),
            backpressure_policy: parse_backpressure_policy("BARQ_INGEST_BACKPRESSURE_POLICY"),
        }
    }
}

pub(crate) fn ingestion_metric_definitions() -> Vec<MetricDefinition> {
    vec![
        MetricDefinition::new(
            "ingestion_batch_count_total",
            MetricKind::Counter,
            "Total number of ingestion batches flushed by the background worker",
        ),
        MetricDefinition::new(
            "ingestion_lag_seconds",
            MetricKind::Gauge,
            "Observed lag between enqueue time and batch flush for the oldest request",
        )
        .with_unit("seconds"),
        MetricDefinition::new(
            "ingestion_queue_size",
            MetricKind::Gauge,
            "Current number of queued ingestion requests awaiting flush",
        ),
        MetricDefinition::new(
            "ingestion_requests_failed_total",
            MetricKind::Counter,
            "Total number of ingestion requests that failed during background flush",
        ),
        MetricDefinition::new(
            "ingestion_requests_succeeded_total",
            MetricKind::Counter,
            "Total number of ingestion requests successfully flushed by the worker",
        ),
    ]
}

/// Saturation policy for the ingestion queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackpressurePolicy {
    Block,
    Reject,
    Drop,
}

/// Snapshot of ingestion metrics used for deterministic tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct IngestionMetricsSnapshot {
    pub(crate) queue_depth: usize,
    pub(crate) batches_total: u64,
    pub(crate) successes_total: u64,
    pub(crate) failures_total: u64,
    pub(crate) last_lag_micros: u64,
}

/// Bounded FIFO queue used by the asynchronous ingestion pipeline.
#[derive(Debug)]
pub struct IngestionQueue<T> {
    capacity: usize,
    items: VecDeque<T>,
}

impl<T> IngestionQueue<T> {
    /// Creates an empty queue with a fixed maximum item count.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ingestion queue capacity must be positive");
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
        }
    }

    /// Attempts to append an item to the tail of the queue.
    pub fn enqueue(&mut self, item: T) -> Result<(), T> {
        if self.is_full() {
            return Err(item);
        }
        self.items.push_back(item);
        Ok(())
    }

    /// Removes and returns the oldest enqueued item.
    pub fn dequeue(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    /// Returns the number of queued items.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true when the queue contains no items.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns true when the queue has reached its configured capacity.
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Returns the configured queue capacity.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Insert request payload accepted by the ingestion worker.
#[derive(Debug, Clone)]
pub(crate) struct IngestionInsertRequest {
    pub(crate) tenant: TenantId,
    pub(crate) collection: String,
    pub(crate) document: Document,
    pub(crate) upsert: bool,
}

/// Queue admission failure reported before a write reaches storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueueAdmissionError {
    Full { capacity: usize },
    Dropped { capacity: usize },
    Closed,
}

impl std::fmt::Display for QueueAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueAdmissionError::Full { capacity } => {
                write!(f, "ingestion queue is full (capacity {capacity})")
            }
            QueueAdmissionError::Dropped { capacity } => {
                write!(
                    f,
                    "ingestion queue dropped the write because policy=drop and capacity {capacity} is exhausted"
                )
            }
            QueueAdmissionError::Closed => write!(f, "ingestion queue is closed"),
        }
    }
}

impl std::error::Error for QueueAdmissionError {}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_backpressure_policy(name: &str) -> BackpressurePolicy {
    match std::env::var(name).ok().as_deref() {
        Some("block") => BackpressurePolicy::Block,
        Some("drop") => BackpressurePolicy::Drop,
        Some("reject") => BackpressurePolicy::Reject,
        _ => BackpressurePolicy::Reject,
    }
}

#[derive(Debug)]
struct PendingInsert {
    request: IngestionInsertRequest,
    enqueued_at: Instant,
    completion: oneshot::Sender<Result<(), StorageError>>,
    permit: OwnedSemaphorePermit,
}

#[derive(Debug, Default)]
struct IngestionMetrics {
    queue_depth: AtomicUsize,
    batches_total: AtomicU64,
    successes_total: AtomicU64,
    failures_total: AtomicU64,
    last_lag_micros: AtomicU64,
}

impl IngestionMetrics {
    fn update_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::SeqCst);
        metrics::gauge!("ingestion_queue_size").set(depth as f64);
    }

    fn record_batch(&self) {
        self.batches_total.fetch_add(1, Ordering::SeqCst);
        metrics::counter!("ingestion_batch_count_total").increment(1);
    }

    fn record_lag(&self, lag: Duration) {
        let micros = lag.as_micros().min(u64::MAX as u128) as u64;
        self.last_lag_micros.store(micros, Ordering::SeqCst);
        metrics::gauge!("ingestion_lag_seconds").set(lag.as_secs_f64());
    }

    fn record_success(&self) {
        self.successes_total.fetch_add(1, Ordering::SeqCst);
        metrics::counter!("ingestion_requests_succeeded_total").increment(1);
    }

    fn record_failure(&self) {
        self.failures_total.fetch_add(1, Ordering::SeqCst);
        metrics::counter!("ingestion_requests_failed_total").increment(1);
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn snapshot(&self) -> IngestionMetricsSnapshot {
        IngestionMetricsSnapshot {
            queue_depth: self.queue_depth.load(Ordering::SeqCst),
            batches_total: self.batches_total.load(Ordering::SeqCst),
            successes_total: self.successes_total.load(Ordering::SeqCst),
            failures_total: self.failures_total.load(Ordering::SeqCst),
            last_lag_micros: self.last_lag_micros.load(Ordering::SeqCst),
        }
    }
}

/// Shared asynchronous ingestion service used by HTTP and gRPC writes.
#[derive(Debug)]
pub(crate) struct IngestionService {
    storage: Arc<AsyncMutex<Storage>>,
    queue: Mutex<IngestionQueue<PendingInsert>>,
    queue_slots: Arc<Semaphore>,
    batch_size: usize,
    queue_capacity: usize,
    backpressure_policy: BackpressurePolicy,
    pending_jobs: AtomicUsize,
    shutdown_requested: AtomicBool,
    item_available: Notify,
    drained: Notify,
    metrics: IngestionMetrics,
    worker_handle: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(test)]
    pause_before_dequeue: Mutex<Option<Arc<PauseBeforeDequeue>>>,
    #[cfg(test)]
    observed_batch_sizes: Mutex<Vec<usize>>,
    #[cfg(test)]
    observed_document_ids: Mutex<Vec<barq_index::DocumentId>>,
}

impl IngestionService {
    /// Creates a new ingestion service backed by the provided storage engine.
    pub(crate) fn new(storage: Arc<AsyncMutex<Storage>>, config: IngestionConfig) -> Arc<Self> {
        assert!(
            config.queue_capacity > 0,
            "ingestion queue capacity must be positive"
        );
        assert!(
            config.batch_size > 0,
            "ingestion batch size must be positive"
        );
        Arc::new(Self {
            storage,
            queue: Mutex::new(IngestionQueue::new(config.queue_capacity)),
            queue_slots: Arc::new(Semaphore::new(config.queue_capacity)),
            batch_size: config.batch_size,
            queue_capacity: config.queue_capacity,
            backpressure_policy: config.backpressure_policy,
            pending_jobs: AtomicUsize::new(0),
            shutdown_requested: AtomicBool::new(false),
            item_available: Notify::new(),
            drained: Notify::new(),
            metrics: IngestionMetrics::default(),
            worker_handle: AsyncMutex::new(None),
            #[cfg(test)]
            pause_before_dequeue: Mutex::new(None),
            #[cfg(test)]
            observed_batch_sizes: Mutex::new(Vec::new()),
            #[cfg(test)]
            observed_document_ids: Mutex::new(Vec::new()),
        })
    }

    pub(crate) async fn submit(
        self: &Arc<Self>,
        request: IngestionInsertRequest,
    ) -> Result<oneshot::Receiver<Result<(), StorageError>>, QueueAdmissionError> {
        if self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(QueueAdmissionError::Closed);
        }
        self.ensure_worker_started().await;

        let permit = match self.backpressure_policy {
            BackpressurePolicy::Block => self
                .queue_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| QueueAdmissionError::Closed)?,
            BackpressurePolicy::Reject => {
                self.queue_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|err| match err {
                        TryAcquireError::NoPermits => QueueAdmissionError::Full {
                            capacity: self.queue_capacity,
                        },
                        TryAcquireError::Closed => QueueAdmissionError::Closed,
                    })?
            }
            BackpressurePolicy::Drop => {
                self.queue_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|err| match err {
                        TryAcquireError::NoPermits => QueueAdmissionError::Dropped {
                            capacity: self.queue_capacity,
                        },
                        TryAcquireError::Closed => QueueAdmissionError::Closed,
                    })?
            }
        };

        let (completion_tx, completion_rx) = oneshot::channel();
        let pending = PendingInsert {
            request,
            enqueued_at: Instant::now(),
            completion: completion_tx,
            permit,
        };

        self.pending_jobs.fetch_add(1, Ordering::SeqCst);
        {
            let mut queue = self.queue.lock().expect("ingestion queue lock poisoned");
            queue
                .enqueue(pending)
                .expect("semaphore permit must reserve queue capacity");
            self.metrics.update_queue_depth(queue.len());
        }
        self.item_available.notify_one();
        Ok(completion_rx)
    }

    async fn ensure_worker_started(self: &Arc<Self>) {
        let mut handle = self.worker_handle.lock().await;
        if handle.is_none() {
            let service = Arc::clone(self);
            *handle = Some(tokio::spawn(async move {
                service.worker_loop().await;
            }));
        }
    }

    async fn worker_loop(self: Arc<Self>) {
        loop {
            if self.shutdown_requested.load(Ordering::SeqCst)
                && self.pending_jobs.load(Ordering::SeqCst) == 0
            {
                break;
            }
            let notified = self.item_available.notified();
            if let Some(batch) = self.take_batch().await {
                self.apply_batch(batch).await;
                continue;
            }
            if self.shutdown_requested.load(Ordering::SeqCst)
                && self.pending_jobs.load(Ordering::SeqCst) == 0
            {
                break;
            }
            notified.await;
        }
    }

    async fn take_batch(&self) -> Option<Vec<PendingInsert>> {
        if self.queue_len() == 0 {
            return None;
        }

        #[cfg(test)]
        let hook = {
            self.pause_before_dequeue
                .lock()
                .expect("pause hook lock poisoned")
                .take()
        };
        #[cfg(test)]
        if let Some(hook) = hook {
            hook.pause().await;
        }

        let mut queue = self.queue.lock().expect("ingestion queue lock poisoned");
        let mut batch = Vec::with_capacity(self.batch_size);
        while batch.len() < self.batch_size {
            match queue.dequeue() {
                Some(pending) => batch.push(pending),
                None => break,
            }
        }
        self.metrics.update_queue_depth(queue.len());
        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }

    async fn apply_batch(&self, batch: Vec<PendingInsert>) {
        #[cfg(test)]
        self.observed_batch_sizes
            .lock()
            .expect("observed batch sizes lock poisoned")
            .push(batch.len());

        self.metrics.record_batch();
        if let Some(oldest) = batch.first() {
            self.metrics.record_lag(oldest.enqueued_at.elapsed());
        }

        let mut storage = self.storage.lock().await;
        let mut completions = Vec::with_capacity(batch.len());

        for pending in batch {
            let _lag = pending.enqueued_at.elapsed();
            #[cfg(test)]
            self.observed_document_ids
                .lock()
                .expect("observed document ids lock poisoned")
                .push(pending.request.document.id.clone());
            let _permit = pending.permit;
            let result = storage.insert_for_tenant(
                &pending.request.tenant,
                &pending.request.collection,
                pending.request.document,
                pending.request.upsert,
            );
            match &result {
                Ok(()) => self.metrics.record_success(),
                Err(_) => self.metrics.record_failure(),
            }
            completions.push((pending.completion, result));
        }

        drop(storage);

        for (completion, result) in completions {
            let _ = completion.send(result);
            if self.pending_jobs.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.drained.notify_waiters();
            }
        }
    }

    pub(crate) fn queue_len(&self) -> usize {
        self.queue
            .lock()
            .expect("ingestion queue lock poisoned")
            .len()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn metrics_snapshot(&self) -> IngestionMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn drain(&self) {
        while self.pending_jobs.load(Ordering::SeqCst) > 0 {
            let notified = self.drained.notified();
            if self.pending_jobs.load(Ordering::SeqCst) == 0 {
                break;
            }
            notified.await;
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn shutdown(self: &Arc<Self>) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.queue_slots.close();
        self.item_available.notify_waiters();
        self.drain().await;
        if let Some(handle) = self.worker_handle.lock().await.take() {
            let _ = handle.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn install_pause_before_dequeue(&self) -> Arc<PauseBeforeDequeue> {
        let hook = Arc::new(PauseBeforeDequeue::default());
        *self
            .pause_before_dequeue
            .lock()
            .expect("pause hook lock poisoned") = Some(Arc::clone(&hook));
        hook
    }

    #[cfg(test)]
    fn observed_batch_sizes(&self) -> Vec<usize> {
        self.observed_batch_sizes
            .lock()
            .expect("observed batch sizes lock poisoned")
            .clone()
    }

    #[cfg(test)]
    fn observed_document_ids(&self) -> Vec<barq_index::DocumentId> {
        self.observed_document_ids
            .lock()
            .expect("observed document ids lock poisoned")
            .clone()
    }
}

pub(crate) fn validate_insert_document(
    schema: &CollectionSchema,
    dimension: usize,
    document: &Document,
) -> Result<(), String> {
    document.id.validate().map_err(|err| err.to_string())?;
    if document.vector.len() != dimension {
        return Err(format!(
            "vector dimension mismatch: expected {}, got {}",
            dimension,
            document.vector.len()
        ));
    }

    let text_fields: Vec<_> = schema
        .fields
        .iter()
        .filter_map(|field| match field.field_type {
            FieldType::Text { .. } => Some((field.name.as_str(), field.required)),
            _ => None,
        })
        .collect();

    if text_fields.is_empty() {
        return Ok(());
    }

    let payload = match &document.payload {
        Some(PayloadValue::Object(payload)) => Some(payload),
        Some(_) => None,
        None => None,
    };

    for (field_name, required) in text_fields {
        match payload.and_then(|payload| payload.get(field_name)) {
            Some(PayloadValue::String(_)) => {}
            Some(_) => {
                return Err(format!("text field {field_name} must be a string"));
            }
            None if required => {
                return Err(format!("missing required text field {field_name}"));
            }
            None => {}
        }
    }

    Ok(())
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct PauseBeforeDequeue {
    reached: Notify,
    release: Notify,
}

#[cfg(test)]
impl PauseBeforeDequeue {
    pub(crate) async fn pause(&self) {
        self.reached.notify_one();
        self.release.notified().await;
    }

    pub(crate) async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ingestion_metric_definitions, validate_insert_document, BackpressurePolicy,
        IngestionConfig, IngestionMetricsSnapshot, IngestionQueue, DEFAULT_INGEST_BATCH_SIZE,
        DEFAULT_INGEST_QUEUE_CAPACITY,
    };
    use crate::{
        insert_document, search_collection, ApiAuth, ApiError, ApiRole, AppState, ClusterConfig,
        ClusterRouter, DocumentIdInput, InsertDocumentRequest, SearchRequest,
    };
    use axum::{
        extract::{Path as AxumPath, State},
        http::{HeaderMap, HeaderValue, StatusCode},
        Json,
    };
    use barq_core::{CollectionSchema, DistanceMetric, FieldSchema, TenantId};
    use barq_core::{Document, FieldType};
    use barq_storage::{Storage, StorageError};
    use proptest::prelude::*;
    use std::sync::Mutex;
    use std::time::Instant;
    use tempfile::TempDir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn build_state(queue_capacity: usize, batch_size: usize) -> (TempDir, AppState, TenantId) {
        build_state_with_policy(queue_capacity, batch_size, BackpressurePolicy::Reject)
    }

    fn build_state_with_policy(
        queue_capacity: usize,
        batch_size: usize,
        backpressure_policy: BackpressurePolicy,
    ) -> (TempDir, AppState, TenantId) {
        let dir = TempDir::new().unwrap();
        let tenant = TenantId::new("tenant-ingest");
        let mut storage = Storage::open(dir.path()).unwrap();
        storage
            .create_collection_for_tenant(
                tenant.clone(),
                CollectionSchema {
                    name: "docs".to_string(),
                    fields: vec![FieldSchema {
                        name: "vector".to_string(),
                        field_type: FieldType::Vector {
                            dimension: 2,
                            metric: DistanceMetric::Cosine,
                            index: None,
                        },
                        required: true,
                    }],
                    bm25_config: None,
                    tenant_id: tenant.clone(),
                },
            )
            .unwrap();

        let auth = ApiAuth::new().require_keys();
        auth.insert("writer", tenant.clone(), ApiRole::Writer);
        let cluster = ClusterRouter::from_config(ClusterConfig::single_node()).unwrap();
        let state = AppState::new_with_ingestion_config(
            storage,
            auth,
            cluster,
            IngestionConfig {
                batch_size,
                queue_capacity,
                backpressure_policy,
            },
        );
        (dir, state, tenant)
    }

    fn auth_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("writer"));
        headers
    }

    fn insert_request(id: u64, vector: Vec<f32>) -> InsertDocumentRequest {
        InsertDocumentRequest {
            id: DocumentIdInput::U64(id),
            vector,
            payload: None,
            upsert: false,
        }
    }

    fn spin_until_clock_moves() {
        let start = Instant::now();
        while start.elapsed().as_micros() == 0 {
            std::hint::spin_loop();
        }
    }

    #[test]
    fn ingestion_metric_catalog_registers_queue_lag_batch_and_outcome_metrics() {
        let (_, state, _) = build_state(4, 2);
        let registered_names: Vec<_> = state
            .metric_definitions()
            .into_iter()
            .map(|definition| definition.name)
            .collect();
        let static_names: Vec<_> = ingestion_metric_definitions()
            .into_iter()
            .map(|definition| definition.name)
            .collect();

        for expected_name in static_names {
            assert!(
                registered_names.contains(&expected_name),
                "missing ingestion metric {expected_name} from registry"
            );
        }
    }

    #[test]
    fn queue_preserves_enqueue_order() {
        let mut queue = IngestionQueue::new(4);
        queue.enqueue("first").unwrap();
        queue.enqueue("second").unwrap();
        queue.enqueue("third").unwrap();

        assert_eq!(queue.dequeue(), Some("first"));
        assert_eq!(queue.dequeue(), Some("second"));
        assert_eq!(queue.dequeue(), Some("third"));
    }

    #[test]
    fn queue_reports_empty_when_drained() {
        let mut queue = IngestionQueue::<u64>::new(2);

        assert!(queue.is_empty());
        assert_eq!(queue.dequeue(), None);

        queue.enqueue(10).unwrap();
        assert!(!queue.is_empty());

        assert_eq!(queue.dequeue(), Some(10));
        assert_eq!(queue.dequeue(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn queue_rejects_items_beyond_capacity() {
        let mut queue = IngestionQueue::new(2);
        queue.enqueue(1).unwrap();
        queue.enqueue(2).unwrap();

        let rejected = queue.enqueue(3).unwrap_err();

        assert_eq!(rejected, 3);
        assert!(queue.is_full());
        assert_eq!(queue.capacity(), 2);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.dequeue(), Some(1));
        assert_eq!(queue.dequeue(), Some(2));
    }

    #[test]
    fn insert_validation_checks_required_text_fields() {
        let schema = CollectionSchema {
            name: "docs".to_string(),
            fields: vec![
                FieldSchema {
                    name: "vector".to_string(),
                    field_type: FieldType::Vector {
                        dimension: 2,
                        metric: DistanceMetric::Cosine,
                        index: None,
                    },
                    required: true,
                },
                FieldSchema {
                    name: "title".to_string(),
                    field_type: FieldType::Text { indexed: true },
                    required: true,
                },
            ],
            bm25_config: None,
            tenant_id: TenantId::new("tenant-a"),
        };

        let err = validate_insert_document(
            &schema,
            2,
            &Document {
                id: barq_index::DocumentId::U64(1),
                vector: vec![1.0, 0.0],
                payload: None,
            },
        )
        .unwrap_err();

        assert!(err.contains("missing required text field title"));
    }

    fn with_ingestion_env<F>(vars: &[(&str, Option<&str>)], f: F)
    where
        F: FnOnce(),
    {
        let _guard = ENV_LOCK.lock().unwrap();
        let saved: Vec<_> = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();

        for (key, value) in vars {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }

        f();

        for (key, value) in saved {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    #[test]
    fn ingestion_config_uses_defaults_when_env_is_missing() {
        with_ingestion_env(
            &[
                ("BARQ_INGEST_BATCH_SIZE", None),
                ("BARQ_INGEST_QUEUE_CAPACITY", None),
                ("BARQ_INGEST_BACKPRESSURE_POLICY", None),
            ],
            || {
                assert_eq!(
                    IngestionConfig::from_env(),
                    IngestionConfig {
                        batch_size: DEFAULT_INGEST_BATCH_SIZE,
                        queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
                        backpressure_policy: BackpressurePolicy::Reject,
                    }
                );
            },
        );
    }

    #[test]
    fn ingestion_config_reads_custom_env_values() {
        with_ingestion_env(
            &[
                ("BARQ_INGEST_BATCH_SIZE", Some("7")),
                ("BARQ_INGEST_QUEUE_CAPACITY", Some("19")),
                ("BARQ_INGEST_BACKPRESSURE_POLICY", Some("block")),
            ],
            || {
                assert_eq!(
                    IngestionConfig::from_env(),
                    IngestionConfig {
                        batch_size: 7,
                        queue_capacity: 19,
                        backpressure_policy: BackpressurePolicy::Block,
                    }
                );
            },
        );
    }

    #[test]
    fn ingestion_config_invalid_values_fall_back_safely() {
        with_ingestion_env(
            &[
                ("BARQ_INGEST_BATCH_SIZE", Some("0")),
                ("BARQ_INGEST_QUEUE_CAPACITY", Some("not-a-number")),
                ("BARQ_INGEST_BACKPRESSURE_POLICY", None),
            ],
            || {
                assert_eq!(
                    IngestionConfig::from_env(),
                    IngestionConfig {
                        batch_size: DEFAULT_INGEST_BATCH_SIZE,
                        queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
                        backpressure_policy: BackpressurePolicy::Reject,
                    }
                );
            },
        );
    }

    #[test]
    fn ingestion_config_invalid_policy_falls_back_to_reject() {
        with_ingestion_env(
            &[(
                "BARQ_INGEST_BACKPRESSURE_POLICY",
                Some("unsupported-policy"),
            )],
            || {
                assert_eq!(
                    IngestionConfig::from_env(),
                    IngestionConfig {
                        batch_size: DEFAULT_INGEST_BATCH_SIZE,
                        queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
                        backpressure_policy: BackpressurePolicy::Reject,
                    }
                );
            },
        );
    }

    #[tokio::test]
    async fn insert_handler_accepts_and_queues_before_worker_dequeues() {
        let (_dir, state, tenant) = build_state(DEFAULT_INGEST_QUEUE_CAPACITY, 1);
        let hook = state.ingestion.install_pause_before_dequeue();

        let state_for_task = state.clone();
        let insert_task = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(state_for_task),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;
        assert_eq!(state.ingestion.queue_len(), 1);
        let storage = state.storage.lock().await;
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(1))
            .unwrap()
            .is_none());
        drop(storage);

        hook.release();
        assert_eq!(insert_task.await.unwrap().unwrap(), StatusCode::CREATED);
        let storage = state.storage.lock().await;
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(1))
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn insert_handler_rejects_when_queue_is_full() {
        let (_dir, state, _tenant) = build_state(1, 1);
        let hook = state.ingestion.install_pause_before_dequeue();

        let state_for_task = state.clone();
        let first_insert = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(state_for_task),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;
        assert_eq!(state.ingestion.queue_len(), 1);

        let err = insert_document(
            AxumPath("docs".to_string()),
            State(state.clone()),
            auth_headers(),
            Json(insert_request(2, vec![0.0, 1.0])),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ApiError::Busy(message) if message.contains("queue is full")));

        hook.release();
        assert_eq!(first_insert.await.unwrap().unwrap(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn insert_handler_validates_input_before_reporting_queue_pressure() {
        let (_dir, state, _tenant) = build_state(1, 1);
        let hook = state.ingestion.install_pause_before_dequeue();

        let state_for_task = state.clone();
        let first_insert = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(state_for_task),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;
        assert_eq!(state.ingestion.queue_len(), 1);

        let err = insert_document(
            AxumPath("docs".to_string()),
            State(state.clone()),
            auth_headers(),
            Json(insert_request(2, vec![1.0])),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, ApiError::BadRequest(message) if message.contains("vector dimension mismatch"))
        );

        hook.release();
        assert_eq!(first_insert.await.unwrap().unwrap(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn worker_flushes_expected_docs_for_full_batch() {
        let (_dir, state, tenant) = build_state(4, 2);
        let hook = state.ingestion.install_pause_before_dequeue();

        let first_state = state.clone();
        let first = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(first_state),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;

        let second_state = state.clone();
        let second = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(second_state),
                auth_headers(),
                Json(insert_request(2, vec![0.0, 1.0])),
            )
            .await
        });

        while state.ingestion.queue_len() < 2 {
            tokio::task::yield_now().await;
        }

        hook.release();
        assert_eq!(first.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(second.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(state.ingestion.observed_batch_sizes(), vec![2]);

        let storage = state.storage.lock().await;
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(1))
            .unwrap()
            .is_some());
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(2))
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn worker_respects_batch_boundaries() {
        let (_dir, state, tenant) = build_state(8, 2);
        let hook = state.ingestion.install_pause_before_dequeue();

        let first_state = state.clone();
        let first = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(first_state),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;

        let second_state = state.clone();
        let second = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(second_state),
                auth_headers(),
                Json(insert_request(2, vec![0.0, 1.0])),
            )
            .await
        });

        let third_state = state.clone();
        let third = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(third_state),
                auth_headers(),
                Json(insert_request(3, vec![0.5, 0.5])),
            )
            .await
        });

        while state.ingestion.queue_len() < 3 {
            tokio::task::yield_now().await;
        }

        hook.release();
        assert_eq!(first.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(second.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(third.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(state.ingestion.observed_batch_sizes(), vec![2, 1]);

        let storage = state.storage.lock().await;
        for id in 1..=3 {
            assert!(storage
                .get_document(&tenant, "docs", &barq_index::DocumentId::U64(id))
                .unwrap()
                .is_some());
        }
    }

    #[tokio::test]
    async fn worker_flushes_partial_batch_when_queue_does_not_fill() {
        let (_dir, state, tenant) = build_state(4, 4);

        assert_eq!(
            insert_document(
                AxumPath("docs".to_string()),
                State(state.clone()),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
            .unwrap(),
            StatusCode::CREATED
        );

        assert_eq!(state.ingestion.observed_batch_sizes(), vec![1]);
        let storage = state.storage.lock().await;
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(1))
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn block_policy_waits_for_capacity_and_preserves_order() {
        let (_dir, state, tenant) = build_state_with_policy(1, 1, BackpressurePolicy::Block);
        let hook = state.ingestion.install_pause_before_dequeue();

        let first_state = state.clone();
        let first = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(first_state),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;
        assert_eq!(state.ingestion.queue_len(), 1);

        let second_state = state.clone();
        let second = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(second_state),
                auth_headers(),
                Json(insert_request(2, vec![0.0, 1.0])),
            )
            .await
        });

        let third_state = state.clone();
        let third = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(third_state),
                auth_headers(),
                Json(insert_request(3, vec![0.5, 0.5])),
            )
            .await
        });

        hook.release();
        assert_eq!(first.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(second.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(third.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(
            state.ingestion.observed_document_ids(),
            vec![
                barq_index::DocumentId::U64(1),
                barq_index::DocumentId::U64(2),
                barq_index::DocumentId::U64(3),
            ]
        );

        let storage = state.storage.lock().await;
        for id in 1..=3 {
            assert!(storage
                .get_document(&tenant, "docs", &barq_index::DocumentId::U64(id))
                .unwrap()
                .is_some());
        }
    }

    #[tokio::test]
    async fn reject_policy_reports_saturation() {
        let (_dir, state, _tenant) = build_state_with_policy(1, 1, BackpressurePolicy::Reject);
        let hook = state.ingestion.install_pause_before_dequeue();

        let first_state = state.clone();
        let first = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(first_state),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;

        let err = insert_document(
            AxumPath("docs".to_string()),
            State(state.clone()),
            auth_headers(),
            Json(insert_request(2, vec![0.0, 1.0])),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ApiError::Busy(message) if message.contains("queue is full")));

        hook.release();
        assert_eq!(first.await.unwrap().unwrap(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn drop_policy_reports_explicit_drop_without_silent_loss() {
        let (_dir, state, tenant) = build_state_with_policy(1, 1, BackpressurePolicy::Drop);
        let hook = state.ingestion.install_pause_before_dequeue();

        let first_state = state.clone();
        let first = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(first_state),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;

        let err = insert_document(
            AxumPath("docs".to_string()),
            State(state.clone()),
            auth_headers(),
            Json(insert_request(2, vec![0.0, 1.0])),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ApiError::Busy(message) if message.contains("dropped")));

        hook.release();
        assert_eq!(first.await.unwrap().unwrap(), StatusCode::CREATED);

        let storage = state.storage.lock().await;
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(1))
            .unwrap()
            .is_some());
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(2))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn metrics_update_after_successful_batch() {
        let (_dir, state, tenant) = build_state(4, 2);
        let hook = state.ingestion.install_pause_before_dequeue();

        let first_state = state.clone();
        let first = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(first_state),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;

        let second_state = state.clone();
        let second = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(second_state),
                auth_headers(),
                Json(insert_request(2, vec![0.0, 1.0])),
            )
            .await
        });

        while state.ingestion.queue_len() < 2 {
            tokio::task::yield_now().await;
        }

        let queued = state.ingestion.metrics_snapshot();
        assert_eq!(queued.queue_depth, 2);

        spin_until_clock_moves();
        hook.release();
        assert_eq!(first.await.unwrap().unwrap(), StatusCode::CREATED);
        assert_eq!(second.await.unwrap().unwrap(), StatusCode::CREATED);

        let snapshot = state.ingestion.metrics_snapshot();
        assert_eq!(
            snapshot,
            IngestionMetricsSnapshot {
                queue_depth: 0,
                batches_total: 1,
                successes_total: 2,
                failures_total: 0,
                last_lag_micros: snapshot.last_lag_micros,
            }
        );
        assert!(snapshot.last_lag_micros > 0);

        let rendered = state.metrics.render();
        assert!(rendered.contains("ingestion_queue_size"));
        assert!(rendered.contains("ingestion_batch_count_total"));
        assert!(rendered.contains("ingestion_lag_seconds"));

        let storage = state.storage.lock().await;
        for id in 1..=2 {
            assert!(storage
                .get_document(&tenant, "docs", &barq_index::DocumentId::U64(id))
                .unwrap()
                .is_some());
        }
    }

    #[tokio::test]
    async fn metrics_record_worker_failure() {
        let (_dir, state, tenant) = build_state(4, 1);
        let hook = state.ingestion.install_pause_before_dequeue();

        let state_for_task = state.clone();
        let insert = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(state_for_task),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;
        {
            let mut storage = state.storage.lock().await;
            let _ = storage.seal_segment_for_tenant(&tenant, "docs").unwrap();
        }
        hook.release();

        let err = insert.await.unwrap().unwrap_err();
        assert!(matches!(
            err,
            ApiError::Storage(StorageError::SegmentNotWritable { .. })
        ));

        let snapshot = state.ingestion.metrics_snapshot();
        assert_eq!(snapshot.batches_total, 1);
        assert_eq!(snapshot.successes_total, 0);
        assert_eq!(snapshot.failures_total, 1);
    }

    #[tokio::test]
    async fn reads_continue_while_ingestion_is_backlogged() {
        let (_dir, state, tenant) = build_state(4, 2);
        {
            let mut storage = state.storage.lock().await;
            storage
                .insert_for_tenant(
                    &tenant,
                    "docs",
                    Document {
                        id: barq_index::DocumentId::U64(99),
                        vector: vec![1.0, 0.0],
                        payload: None,
                    },
                    false,
                )
                .unwrap();
        }

        let hook = state.ingestion.install_pause_before_dequeue();
        let state_for_task = state.clone();
        let insert = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(state_for_task),
                auth_headers(),
                Json(insert_request(1, vec![0.0, 1.0])),
            )
            .await
        });

        hook.wait_until_reached().await;

        let response = search_collection(
            AxumPath("docs".to_string()),
            State(state.clone()),
            auth_headers(),
            Json(SearchRequest {
                vector: vec![1.0, 0.0],
                top_k: 1,
                filter: None,
            }),
        )
        .await
        .unwrap();

        let results = response.0.results;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, barq_index::DocumentId::U64(99));

        hook.release();
        assert_eq!(insert.await.unwrap().unwrap(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn shutdown_waits_for_drain_and_closes_new_submissions() {
        let (_dir, state, tenant) = build_state(4, 1);
        let hook = state.ingestion.install_pause_before_dequeue();

        let state_for_task = state.clone();
        let insert = tokio::spawn(async move {
            insert_document(
                AxumPath("docs".to_string()),
                State(state_for_task),
                auth_headers(),
                Json(insert_request(1, vec![1.0, 0.0])),
            )
            .await
        });

        hook.wait_until_reached().await;

        let ingestion = state.ingestion.clone();
        let shutdown = tokio::spawn(async move {
            ingestion.shutdown().await;
        });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());

        hook.release();
        assert_eq!(insert.await.unwrap().unwrap(), StatusCode::CREATED);
        shutdown.await.unwrap();

        let err = insert_document(
            AxumPath("docs".to_string()),
            State(state.clone()),
            auth_headers(),
            Json(insert_request(2, vec![0.0, 1.0])),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApiError::Busy(message) if message.contains("unavailable")));

        let snapshot = state.ingestion.metrics_snapshot();
        assert_eq!(snapshot.queue_depth, 0);
        let storage = state.storage.lock().await;
        assert!(storage
            .get_document(&tenant, "docs", &barq_index::DocumentId::U64(1))
            .unwrap()
            .is_some());
    }

    proptest! {
        #[test]
        fn queue_fifo_property(values in proptest::collection::vec(0u16..1000, 0..32)) {
            let capacity = values.len().max(1);
            let mut queue = IngestionQueue::new(capacity);
            for value in &values {
                queue.enqueue(*value).unwrap();
            }

            let drained: Vec<_> = std::iter::from_fn(|| queue.dequeue()).collect();
            prop_assert_eq!(drained, values);
        }
    }
}
