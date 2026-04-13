use barq_core::{CollectionSchema, Document, FieldType, PayloadValue, TenantId};
use barq_storage::{Storage, StorageError};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{
    oneshot, Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError,
};

pub(crate) const DEFAULT_INGEST_QUEUE_CAPACITY: usize = 64;

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
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns true when the queue has reached its configured capacity.
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Returns the configured queue capacity.
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
    Closed,
}

impl std::fmt::Display for QueueAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueAdmissionError::Full { capacity } => {
                write!(f, "ingestion queue is full (capacity {capacity})")
            }
            QueueAdmissionError::Closed => write!(f, "ingestion queue is closed"),
        }
    }
}

impl std::error::Error for QueueAdmissionError {}

#[derive(Debug)]
struct PendingInsert {
    request: IngestionInsertRequest,
    enqueued_at: Instant,
    completion: oneshot::Sender<Result<(), StorageError>>,
    permit: OwnedSemaphorePermit,
}

/// Shared asynchronous ingestion service used by HTTP and gRPC writes.
#[derive(Debug)]
pub(crate) struct IngestionService {
    storage: Arc<AsyncMutex<Storage>>,
    queue: Mutex<IngestionQueue<PendingInsert>>,
    queue_slots: Arc<Semaphore>,
    queue_capacity: usize,
    pending_jobs: AtomicUsize,
    item_available: Notify,
    worker_handle: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(test)]
    pause_before_dequeue: Mutex<Option<Arc<PauseBeforeDequeue>>>,
}

impl IngestionService {
    /// Creates a new ingestion service backed by the provided storage engine.
    pub(crate) fn new(storage: Arc<AsyncMutex<Storage>>, queue_capacity: usize) -> Arc<Self> {
        assert!(
            queue_capacity > 0,
            "ingestion queue capacity must be positive"
        );
        Arc::new(Self {
            storage,
            queue: Mutex::new(IngestionQueue::new(queue_capacity)),
            queue_slots: Arc::new(Semaphore::new(queue_capacity)),
            queue_capacity,
            pending_jobs: AtomicUsize::new(0),
            item_available: Notify::new(),
            worker_handle: AsyncMutex::new(None),
            #[cfg(test)]
            pause_before_dequeue: Mutex::new(None),
        })
    }

    pub(crate) async fn submit(
        self: &Arc<Self>,
        request: IngestionInsertRequest,
    ) -> Result<oneshot::Receiver<Result<(), StorageError>>, QueueAdmissionError> {
        self.ensure_worker_started().await;

        let permit = self
            .queue_slots
            .clone()
            .try_acquire_owned()
            .map_err(|err| match err {
                TryAcquireError::NoPermits => QueueAdmissionError::Full {
                    capacity: self.queue_capacity,
                },
                TryAcquireError::Closed => QueueAdmissionError::Closed,
            })?;

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
            let notified = self.item_available.notified();
            if let Some(pending) = self.take_next().await {
                self.apply_one(pending).await;
                continue;
            }
            notified.await;
        }
    }

    async fn take_next(&self) -> Option<PendingInsert> {
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

        self.queue
            .lock()
            .expect("ingestion queue lock poisoned")
            .dequeue()
    }

    async fn apply_one(&self, pending: PendingInsert) {
        let _lag = pending.enqueued_at.elapsed();
        let _permit = pending.permit;
        let result = {
            let mut storage = self.storage.lock().await;
            storage.insert_for_tenant(
                &pending.request.tenant,
                &pending.request.collection,
                pending.request.document,
                pending.request.upsert,
            )
        };
        let _ = pending.completion.send(result);
        self.pending_jobs.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn queue_len(&self) -> usize {
        self.queue
            .lock()
            .expect("ingestion queue lock poisoned")
            .len()
    }

    #[cfg(test)]
    fn install_pause_before_dequeue(&self) -> Arc<PauseBeforeDequeue> {
        let hook = Arc::new(PauseBeforeDequeue::default());
        *self
            .pause_before_dequeue
            .lock()
            .expect("pause hook lock poisoned") = Some(Arc::clone(&hook));
        hook
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
struct PauseBeforeDequeue {
    reached: Notify,
    release: Notify,
}

#[cfg(test)]
impl PauseBeforeDequeue {
    async fn pause(&self) {
        self.reached.notify_one();
        self.release.notified().await;
    }

    async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_insert_document, IngestionQueue, DEFAULT_INGEST_QUEUE_CAPACITY};
    use crate::{
        insert_document, ApiAuth, ApiError, ApiRole, AppState, ClusterConfig, ClusterRouter,
        DocumentIdInput, InsertDocumentRequest,
    };
    use axum::{
        extract::{Path as AxumPath, State},
        http::{HeaderMap, HeaderValue, StatusCode},
        Json,
    };
    use barq_core::{CollectionSchema, DistanceMetric, FieldSchema, TenantId};
    use barq_core::{Document, FieldType};
    use barq_storage::Storage;
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn build_state(queue_capacity: usize) -> (TempDir, AppState, TenantId) {
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
        let state = AppState::new_with_ingestion_queue_capacity(storage, auth, cluster, queue_capacity);
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

    #[tokio::test]
    async fn insert_handler_accepts_and_queues_before_worker_dequeues() {
        let (_dir, state, tenant) = build_state(DEFAULT_INGEST_QUEUE_CAPACITY);
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
        let (_dir, state, _tenant) = build_state(1);
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
        let (_dir, state, _tenant) = build_state(1);
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

        assert!(matches!(err, ApiError::BadRequest(message) if message.contains("vector dimension mismatch")));

        hook.release();
        assert_eq!(first_insert.await.unwrap().unwrap(), StatusCode::CREATED);
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
