# Phase 3: Object Storage Tiering (S3/GCS Integration)

## Overview
This phase focuses on implementing a multi-tiered storage architecture for Barq DB, allowing data to be offloaded to cheaper object storage (S3/GCS/Azure) as it ages or based on policy. This is critical for scaling data volume cost-effectively.

**Branch**: `phase-3-storage-tiering`
**Priority**: High
**Dependencies**: `barq-storage` crate

---

## Task 3.1: Tiering Manager and Policies

### Description
Implement a `TieringManager` that orchestrates data movement between "Hot" (local SSD/RAM), "Warm" (local HDD/network storage), and "Cold" (Object Storage) tiers.

### Implementation Details

#### Files to Create/Modify
- `barq-storage/src/object_store/mod.rs` (Enhance)
- `barq-storage/src/object_store/tiering.rs` (NEW)
- `barq-storage/src/lib.rs` (Expose modules)

#### Code Structure
```rust
// barq-storage/src/object_store/tiering.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    Hot,  // Local, fast access (current default)
    Warm, // Local, high capacity (optional)
    Cold, // Object storage (S3/GCS)
}

#[derive(Debug, Clone)]
pub struct TieringPolicy {
    /// Move to Cold tier after this duration of inactivity
    pub move_to_cold_after: std::time::Duration,
    /// Maximum size of Hot tier before eviction
    pub max_hot_size_bytes: u64,
}

pub struct TieringManager {
    local_store: Arc<LocalObjectStore>, // Existing local impl
    remote_store: Option<Arc<dyn ObjectStore>>, // S3, GCS, etc.
    policy: TieringPolicy,
    // Index tracking where each segment lives
    segment_location: RwLock<HashMap<SegmentId, StorageTier>>,
}

impl TieringManager {
    /// Check policies and move segments if necessary
    pub async fn run_maintenance(&self);
    
    /// Retrieve a segment, fetching from remote if needed
    pub async fn get_segment(&self, id: SegmentId) -> Result<SegmentReader>;
    
    /// Offload a specific segment to cold storage
    pub async fn offload_segment(&self, id: SegmentId) -> Result<()>;
    
    /// Hydrate a segment back to hot storage (prefetch)
    pub async fn hydrate_segment(&self, id: SegmentId) -> Result<()>;
}
```

### Acceptance Criteria
- [ ] `TieringManager` logic correctly identifies segments for offloading.
- [ ] `offload_segment` successfully uploads to `ObjectStore` and deletes local file.
- [ ] `get_segment` transparently fetches remote data if needed.
- [ ] `maintenance` loop runs periodically.

---

## Task 3.2: S3 and GCS Integration Glue

### Description
Ensure the existing `s3.rs` and `gcs.rs` implementations in `barq-storage` are fully functional and integrated with the new `TieringManager`. Refine configuration loading from environment variables.

### Implementation Details

#### Config Updates
- `barq-config/src/lib.rs` (or equivalent config struct): Add Storage config.
    ```rust
    pub struct StorageConfig {
        pub provider: StorageProvider, // Local, S3, GCS
        pub bucket: String,
        pub region: Option<String>,
        // ... creds
    }
    ```

#### Verification
- Verify `aws-sdk-s3` and `google-cloud-storage` crate dependencies are correct.

### Acceptance Criteria
- [ ] Can configure S3 backend via env vars (`BARQ_STORAGE_TYPE=s3`, `BARQ_S3_BUCKET=...`).
- [ ] Can configure GCS backend via env vars.
- [ ] Integration tests using `minio` (for S3) or mock.

---

## Task 3.3: Integration with Write-Ahead-Log (WAL) and Core

### Description
The core database engine must verify it can still read/write data when the underlying storage changes. The WAL should remain on Hot storage always. Only compacted data segments (SSTables / Index files) should move to Cold storage.

### Implementation Details
- Ensure `flush` operations write locally first.
- Only "closed" and "immutable" segments are eligible for tiering.

### Test Plan
- **Unit Tests**: Test `TieringManager` state transitions.
- **Integration Test**:
    1. Start Barq with Minio (S3 compatible).
    2. Write data.
    3. Trigger manual offload.
    4. Verify data exists in Minio.
    5. Verify data is gone from local `data` dir (except metadata).
    6. Read data and verify correctness.

---

## Future Phase (Post-Phase 3)
- Tiering policies based on access frequency (LRU).
- Async hydration (lazy loading).
