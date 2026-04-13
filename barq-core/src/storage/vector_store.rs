use memmap2::{Mmap, MmapOptions};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MMAP_MAGIC: &[u8; 4] = b"BVS1";
const HEADER_SIZE: usize = 16;
const DEFAULT_MAX_MEMORY_MB: u64 = 256;
const COMPACTION_STALE_RATIO_NUM: usize = 1;
const COMPACTION_STALE_RATIO_DEN: usize = 3;
const COMPACTION_MIN_STALE_BYTES: usize = 4 * 1024 * 1024;

/// Runtime configuration for vector stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorStoreConfig {
    pub max_memory_bytes: u64,
}

impl Default for VectorStoreConfig {
    fn default() -> Self {
        let mb = std::env::var("BARQ_MAX_MEMORY_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_MAX_MEMORY_MB);
        Self {
            max_memory_bytes: mb * 1024 * 1024,
        }
    }
}


/// Result type used by vector store implementations.
pub type Result<T> = std::result::Result<T, VectorStoreError>;

/// Errors produced by vector store implementations.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VectorStoreError {
    #[error("vector is empty")]
    EmptyVector,

    #[error("vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("vector id {0} already exists")]
    DuplicateId(u64),

    #[error("io error: {0}")]
    Io(String),

    #[error("corrupt data: {0}")]
    CorruptData(String),
}

/// Pluggable storage for vectors keyed by `u64`.
pub trait VectorStore {
    fn get(&self, id: u64) -> Option<&[f32]>;
    fn insert(&mut self, id: u64, vector: &[f32]) -> Result<()>;
    fn delete(&mut self, id: u64) -> bool;
    fn contains(&self, id: u64) -> bool;
    fn len(&self) -> usize;
    fn for_each_id(&self, f: &mut dyn FnMut(u64));
}

/// In-memory vector store implementation backed by `HashMap`.
#[derive(Debug, Default)]
pub struct InMemoryVectorStore {
    vectors: HashMap<u64, Vec<f32>>,
    dimension: Option<usize>,
    allow_overwrite: bool,
    memory_bytes: usize,
}

impl InMemoryVectorStore {
    /// Creates a new in-memory store.
    pub fn new(dimension: Option<usize>, allow_overwrite: bool) -> Self {
        Self {
            vectors: HashMap::new(),
            dimension,
            allow_overwrite,
            memory_bytes: 0,
        }
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.is_empty() {
            return Err(VectorStoreError::EmptyVector);
        }
        if let Some(expected) = self.dimension {
            if vector.len() != expected {
                return Err(VectorStoreError::DimensionMismatch {
                    expected,
                    actual: vector.len(),
                });
            }
        }
        Ok(())
    }

    /// Estimated in-process memory used by vectors.
    pub fn estimated_memory_bytes(&self) -> usize {
        self.memory_bytes
    }
}

impl VectorStore for InMemoryVectorStore {
    fn get(&self, id: u64) -> Option<&[f32]> {
        self.vectors.get(&id).map(Vec::as_slice)
    }

    fn insert(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        self.validate_vector(vector)?;
        if !self.allow_overwrite && self.vectors.contains_key(&id) {
            return Err(VectorStoreError::DuplicateId(id));
        }
        let old_bytes = self.vectors.get(&id).map(|v| v.len() * std::mem::size_of::<f32>()).unwrap_or(0);
        self.vectors.insert(id, vector.to_vec());
        let new_bytes = vector.len() * std::mem::size_of::<f32>();
        self.memory_bytes = self.memory_bytes + new_bytes - old_bytes;
        Ok(())
    }

    fn delete(&mut self, id: u64) -> bool {
        if let Some(v) = self.vectors.remove(&id) {
            self.memory_bytes = self.memory_bytes.saturating_sub(v.len() * std::mem::size_of::<f32>());
            true
        } else {
            false
        }
    }

    fn contains(&self, id: u64) -> bool {
        self.vectors.contains_key(&id)
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn for_each_id(&self, f: &mut dyn FnMut(u64)) {
        for id in self.vectors.keys().copied() {
            f(id);
        }
    }
}

/// Disk-backed vector store using a flat binary file and memory-mapped reads.
#[derive(Debug)]
pub struct MmapVectorStore {
    file_path: PathBuf,
    file: File,
    mmap: Option<Mmap>,
    dimension: Option<usize>,
    allow_overwrite: bool,
    offsets: HashMap<u64, (usize, usize)>,
    len: usize,
    stale_records: usize,
    stale_bytes: usize,
}

impl MmapVectorStore {
    /// Opens or creates a memory-mapped vector store.
    pub fn open(path: impl AsRef<Path>, dimension: Option<usize>, allow_overwrite: bool) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .map_err(|e| VectorStoreError::Io(e.to_string()))?;

        if file.metadata().map_err(|e| VectorStoreError::Io(e.to_string()))?.len() == 0 {
            let mut header = [0_u8; HEADER_SIZE];
            header[0..4].copy_from_slice(MMAP_MAGIC);
            file.write_all(&header)
                .map_err(|e| VectorStoreError::Io(e.to_string()))?;
            file.flush().map_err(|e| VectorStoreError::Io(e.to_string()))?;
        }

        let (offsets, len, stale_records, stale_bytes) = Self::load_index(&mut file)?;
        let mmap = Self::map_file(&file)?;

        Ok(Self {
            file_path,
            file,
            mmap,
            dimension,
            allow_overwrite,
            offsets,
            len,
            stale_records,
            stale_bytes,
        })
    }

    fn map_file(file: &File) -> Result<Option<Mmap>> {
        let size = file.metadata().map_err(|e| VectorStoreError::Io(e.to_string()))?.len();
        if size == 0 {
            return Ok(None);
        }
        // SAFETY: file descriptor remains valid for the lifetime of `MmapVectorStore`.
        let mmap = unsafe { MmapOptions::new().map(file).map_err(|e| VectorStoreError::Io(e.to_string()))? };
        Ok(Some(mmap))
    }

    fn load_index(file: &mut File) -> Result<(HashMap<u64, (usize, usize)>, usize, usize, usize)> {
        let file_len = file.metadata().map_err(|e| VectorStoreError::Io(e.to_string()))?.len() as usize;
        if file_len < HEADER_SIZE {
            return Err(VectorStoreError::CorruptData("file shorter than header".into()));
        }

        file.seek(SeekFrom::Start(0)).map_err(|e| VectorStoreError::Io(e.to_string()))?;
        let mut header = [0_u8; HEADER_SIZE];
        file.read_exact(&mut header)
            .map_err(|e| VectorStoreError::Io(e.to_string()))?;
        if &header[0..4] != MMAP_MAGIC {
            return Err(VectorStoreError::CorruptData("invalid file header magic".into()));
        }

        let mut cursor = HEADER_SIZE;
        let mut offsets = HashMap::new();
        let mut count: usize = 0;
        let mut stale_records = 0;
        let mut stale_bytes = 0;
        while cursor < file_len {
            if cursor + 12 > file_len {
                return Err(VectorStoreError::CorruptData("truncated record header".into()));
            }
            let mut rec = [0_u8; 12];
            file.read_exact(&mut rec)
                .map_err(|e| VectorStoreError::Io(e.to_string()))?;
            let id = u64::from_le_bytes(rec[0..8].try_into().unwrap());
            let dim = u32::from_le_bytes(rec[8..12].try_into().unwrap()) as usize;
            let bytes = dim
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| VectorStoreError::CorruptData("record size overflow".into()))?;
            let data_offset = cursor + 12;
            let end = data_offset + bytes;
            if end > file_len {
                return Err(VectorStoreError::CorruptData("truncated vector payload".into()));
            }

            if dim == 0 {
                if offsets.remove(&id).is_some() {
                    stale_records += 1;
                    stale_bytes += bytes + 12;
                    count = count.saturating_sub(1);
                }
            } else {
                if let Some((_, prev_dim)) = offsets.insert(id, (data_offset, dim)) {
                    stale_records += 1;
                    stale_bytes += prev_dim * std::mem::size_of::<f32>() + 12;
                } else {
                    count += 1;
                }
            }
            cursor = end;
            file.seek(SeekFrom::Start(cursor as u64))
                .map_err(|e| VectorStoreError::Io(e.to_string()))?;
        }

        Ok((offsets, count, stale_records, stale_bytes))
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.is_empty() {
            return Err(VectorStoreError::EmptyVector);
        }
        if let Some(expected) = self.dimension {
            if vector.len() != expected {
                return Err(VectorStoreError::DimensionMismatch {
                    expected,
                    actual: vector.len(),
                });
            }
        }
        Ok(())
    }

    fn remap(&mut self) -> Result<()> {
        self.file.flush().map_err(|e| VectorStoreError::Io(e.to_string()))?;
        self.mmap = Self::map_file(&self.file)?;
        Ok(())
    }

    fn maybe_compact(&mut self) -> Result<()> {
        let active_bytes = self.offsets.values().map(|(_, dim)| dim * std::mem::size_of::<f32>() + 12).sum::<usize>();
        let stale_ratio_exceeded = self.stale_records * COMPACTION_STALE_RATIO_DEN > self.len.max(1) * COMPACTION_STALE_RATIO_NUM;
        if !(stale_ratio_exceeded || self.stale_bytes >= COMPACTION_MIN_STALE_BYTES || self.stale_bytes > active_bytes / 2) {
            return Ok(());
        }
        let mut entries: Vec<(u64, Vec<f32>)> = Vec::with_capacity(self.offsets.len());
        self.for_each_id(&mut |id| {
            if let Some(v) = self.get(id) {
                entries.push((id, v.to_vec()));
            }
        });
        entries.sort_by_key(|(id, _)| *id);

        self.file.set_len(0).map_err(|e| VectorStoreError::Io(e.to_string()))?;
        self.file.seek(SeekFrom::Start(0)).map_err(|e| VectorStoreError::Io(e.to_string()))?;
        let mut header = [0_u8; HEADER_SIZE];
        header[0..4].copy_from_slice(MMAP_MAGIC);
        self.file.write_all(&header).map_err(|e| VectorStoreError::Io(e.to_string()))?;

        self.offsets.clear();
        for (id, vector) in entries {
            self.file.write_all(&id.to_le_bytes()).map_err(|e| VectorStoreError::Io(e.to_string()))?;
            self.file.write_all(&(vector.len() as u32).to_le_bytes()).map_err(|e| VectorStoreError::Io(e.to_string()))?;
            let data_offset = self.file.stream_position().map_err(|e| VectorStoreError::Io(e.to_string()))? as usize;
            for value in &vector {
                self.file.write_all(&value.to_le_bytes()).map_err(|e| VectorStoreError::Io(e.to_string()))?;
            }
            self.offsets.insert(id, (data_offset, vector.len()));
        }
        self.file.flush().map_err(|e| VectorStoreError::Io(e.to_string()))?;
        self.len = self.offsets.len();
        self.stale_records = 0;
        self.stale_bytes = 0;
        self.remap()?;
        Ok(())
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Estimated in-process memory used by offset index metadata.
    pub fn estimated_memory_bytes(&self) -> usize {
        self.offsets.len() * std::mem::size_of::<(u64, (usize, usize))>()
    }
}

impl VectorStore for MmapVectorStore {
    fn get(&self, id: u64) -> Option<&[f32]> {
        let (offset, dim) = *self.offsets.get(&id)?;
        let mmap = self.mmap.as_ref()?;
        let byte_len = dim * std::mem::size_of::<f32>();
        let bytes = mmap.get(offset..offset + byte_len)?;

        // SAFETY: vectors are always serialized as contiguous little-endian f32 sequences
        // aligned on 4-byte boundaries and remain immutable after mapping.
        let ptr = bytes.as_ptr() as *const f32;
        Some(unsafe { std::slice::from_raw_parts(ptr, dim) })
    }

    fn insert(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        self.validate_vector(vector)?;
        if !self.allow_overwrite && self.offsets.contains_key(&id) {
            return Err(VectorStoreError::DuplicateId(id));
        }

        self.file.seek(SeekFrom::End(0)).map_err(|e| VectorStoreError::Io(e.to_string()))?;
        self.file
            .write_all(&id.to_le_bytes())
            .map_err(|e| VectorStoreError::Io(e.to_string()))?;
        self.file
            .write_all(&(vector.len() as u32).to_le_bytes())
            .map_err(|e| VectorStoreError::Io(e.to_string()))?;

        let data_offset = self.file.stream_position().map_err(|e| VectorStoreError::Io(e.to_string()))? as usize;
        for value in vector {
            self.file
                .write_all(&value.to_le_bytes())
                .map_err(|e| VectorStoreError::Io(e.to_string()))?;
        }
        self.file.flush().map_err(|e| VectorStoreError::Io(e.to_string()))?;
        if let Some((_, prev_dim)) = self.offsets.insert(id, (data_offset, vector.len())) {
            self.stale_records += 1;
            self.stale_bytes += prev_dim * std::mem::size_of::<f32>() + 12;
        }
        self.len = self.offsets.len();
        self.remap()?;
        self.maybe_compact()?;
        Ok(())
    }

    fn delete(&mut self, id: u64) -> bool {
        if !self.offsets.contains_key(&id) {
            return false;
        }
        if self.file.seek(SeekFrom::End(0)).is_err() {
            return false;
        }
        if self.file.write_all(&id.to_le_bytes()).is_err() {
            return false;
        }
        if self.file.write_all(&0u32.to_le_bytes()).is_err() {
            return false;
        }
        if self.file.flush().is_err() {
            return false;
        }
        if let Some((_, prev_dim)) = self.offsets.remove(&id) {
            self.stale_bytes += prev_dim * std::mem::size_of::<f32>() + 12;
        }
        self.len = self.offsets.len();
        self.stale_records += 1;
        let _ = self.remap();
        let _ = self.maybe_compact();
        true
    }

    fn contains(&self, id: u64) -> bool {
        self.offsets.contains_key(&id)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn for_each_id(&self, f: &mut dyn FnMut(u64)) {
        for id in self.offsets.keys().copied() {
            f(id);
        }
    }
}



/// Vector store that keeps recent vectors in-memory and flushes least-recent items to disk.
#[derive(Debug)]
pub struct BudgetedVectorStore {
    memory: HashMap<u64, Vec<f32>>,
    recency: HashMap<u64, u64>,
    clock: u64,
    memory_bytes: usize,
    config: VectorStoreConfig,
    disk: MmapVectorStore,
}

impl BudgetedVectorStore {
    /// Opens a budgeted store with mmap persistence fallback.
    pub fn open(path: impl AsRef<Path>, dimension: Option<usize>, allow_overwrite: bool, config: VectorStoreConfig) -> Result<Self> {
        Ok(Self {
            memory: HashMap::new(),
            recency: HashMap::new(),
            clock: 0,
            memory_bytes: 0,
            config,
            disk: MmapVectorStore::open(path, dimension, allow_overwrite)?,
        })
    }

    fn evict_if_needed(&mut self) {
        let max = self.config.max_memory_bytes as usize;
        while self.memory_bytes > max {
            let Some((&victim_id, _)) = self.recency.iter().min_by_key(|(_, seq)| *seq) else { break; };
            if let Some(v) = self.memory.remove(&victim_id) {
                self.memory_bytes = self.memory_bytes.saturating_sub(v.len() * std::mem::size_of::<f32>());
            }
            self.recency.remove(&victim_id);
        }
    }

    /// Current in-memory bytes before disk fallback is used.
    pub fn resident_memory_bytes(&self) -> usize {
        self.memory_bytes
    }
}

impl VectorStore for BudgetedVectorStore {
    fn get(&self, id: u64) -> Option<&[f32]> {
        self.memory
            .get(&id)
            .map(Vec::as_slice)
            .or_else(|| self.disk.get(id))
    }

    fn insert(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        self.disk.insert(id, vector)?;
        let old_bytes = self.memory.get(&id).map(|v| v.len() * std::mem::size_of::<f32>()).unwrap_or(0);
        self.memory.insert(id, vector.to_vec());
        let new_bytes = vector.len() * std::mem::size_of::<f32>();
        self.memory_bytes = self.memory_bytes + new_bytes - old_bytes;
        self.clock = self.clock.saturating_add(1);
        self.recency.insert(id, self.clock);
        self.evict_if_needed();
        Ok(())
    }

    fn delete(&mut self, id: u64) -> bool {
        if let Some(v) = self.memory.remove(&id) {
            self.memory_bytes = self.memory_bytes.saturating_sub(v.len() * std::mem::size_of::<f32>());
        }
        self.recency.remove(&id);
        self.disk.delete(id)
    }

    fn contains(&self, id: u64) -> bool {
        self.memory.contains_key(&id) || self.disk.contains(id)
    }

    fn len(&self) -> usize {
        self.disk.len()
    }

    fn for_each_id(&self, f: &mut dyn FnMut(u64)) {
        self.disk.for_each_id(f)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    struct MockVectorStore {
        vectors: HashMap<u64, Vec<f32>>,
    }

    impl MockVectorStore {
        fn new() -> Self {
            Self {
                vectors: HashMap::new(),
            }
        }
    }

    impl VectorStore for MockVectorStore {
        fn get(&self, id: u64) -> Option<&[f32]> {
            self.vectors.get(&id).map(Vec::as_slice)
        }

        fn insert(&mut self, id: u64, vector: &[f32]) -> Result<()> {
            if vector.is_empty() {
                return Err(VectorStoreError::EmptyVector);
            }
            self.vectors.insert(id, vector.to_vec());
            Ok(())
        }

        fn delete(&mut self, id: u64) -> bool {
            self.vectors.remove(&id).is_some()
        }

        fn contains(&self, id: u64) -> bool {
            self.vectors.contains_key(&id)
        }

        fn len(&self) -> usize {
            self.vectors.len()
        }

        fn for_each_id(&self, f: &mut dyn FnMut(u64)) {
            for id in self.vectors.keys().copied() {
                f(id);
            }
        }
    }

    #[test]
    fn trait_backed_mock_compiles_and_supports_insert_get() {
        let mut store = MockVectorStore::new();
        store.insert(10, &[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(store.get(10), Some(&[1.0, 2.0, 3.0][..]));
    }

    #[test]
    fn trait_contract_insert_get_contains_len() {
        let mut store = MockVectorStore::new();
        assert_eq!(store.len(), 0);
        assert!(!store.contains(7));

        store.insert(7, &[0.1, 0.2]).unwrap();
        assert_eq!(store.len(), 1);
        assert!(store.contains(7));
        assert_eq!(store.get(7), Some(&[0.1, 0.2][..]));
        assert_eq!(store.get(99), None);
    }

    #[test]
    fn in_memory_insert_get_exact_roundtrip() {
        let mut store = InMemoryVectorStore::new(Some(3), false);
        store.insert(1, &[0.1, 0.2, 0.3]).unwrap();
        assert_eq!(store.get(1), Some(&[0.1, 0.2, 0.3][..]));
    }

    #[test]
    fn in_memory_duplicate_rejected_when_overwrite_disabled() {
        let mut store = InMemoryVectorStore::new(Some(2), false);
        store.insert(1, &[1.0, 2.0]).unwrap();
        let err = store.insert(1, &[9.0, 9.0]).unwrap_err();
        assert_eq!(err, VectorStoreError::DuplicateId(1));
        assert_eq!(store.get(1), Some(&[1.0, 2.0][..]));
    }

    #[test]
    fn in_memory_empty_vector_rejected() {
        let mut store = InMemoryVectorStore::new(None, true);
        assert_eq!(store.insert(1, &[]).unwrap_err(), VectorStoreError::EmptyVector);
    }

    #[test]
    fn in_memory_dimension_mismatch_rejected() {
        let mut store = InMemoryVectorStore::new(Some(4), true);
        assert_eq!(
            store.insert(1, &[0.0, 1.0]).unwrap_err(),
            VectorStoreError::DimensionMismatch {
                expected: 4,
                actual: 2,
            }
        );
    }

    #[test]
    fn in_memory_multiple_inserts_and_len_correctness() {
        let mut store = InMemoryVectorStore::new(Some(2), true);
        for (id, vector) in [(1, [0.0, 1.0]), (2, [1.0, 2.0]), (3, [2.0, 3.0])] {
            store.insert(id, &vector).unwrap();
        }
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn in_memory_missing_id_returns_none() {
        let mut store = InMemoryVectorStore::new(Some(2), true);
        store.insert(1, &[1.0, 2.0]).unwrap();
        assert_eq!(store.get(2), None);
    }

    #[test]
    fn config_defaults_when_env_missing() {
        let _guard = env_lock();
        std::env::remove_var("BARQ_MAX_MEMORY_MB");
        let cfg = VectorStoreConfig::default();
        assert_eq!(cfg.max_memory_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn config_parses_custom_env_var() {
        let _guard = env_lock();
        std::env::set_var("BARQ_MAX_MEMORY_MB", "64");
        let cfg = VectorStoreConfig::default();
        assert_eq!(cfg.max_memory_bytes, 64 * 1024 * 1024);
        std::env::remove_var("BARQ_MAX_MEMORY_MB");
    }

    #[test]
    fn config_invalid_env_var_falls_back_safely() {
        let _guard = env_lock();
        std::env::set_var("BARQ_MAX_MEMORY_MB", "not-a-number");
        let cfg = VectorStoreConfig::default();
        assert_eq!(cfg.max_memory_bytes, 256 * 1024 * 1024);
        std::env::remove_var("BARQ_MAX_MEMORY_MB");
    }

    #[test]
    fn in_memory_memory_estimate_increases_with_inserts() {
        let mut store = InMemoryVectorStore::new(Some(2), true);
        let before = store.estimated_memory_bytes();
        store.insert(1, &[1.0, 2.0]).unwrap();
        store.insert(2, &[3.0, 4.0]).unwrap();
        assert!(store.estimated_memory_bytes() > before);
    }

    #[test]
    fn in_memory_memory_estimate_stable_without_flush() {
        let mut store = InMemoryVectorStore::new(Some(2), true);
        store.insert(1, &[1.0, 2.0]).unwrap();
        let after_insert = store.estimated_memory_bytes();
        let after_noop = store.estimated_memory_bytes();
        assert_eq!(after_insert, after_noop);
    }


    #[test]
    fn budgeted_store_evicts_once_budget_exceeded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budgeted.bin");
        let mut store = BudgetedVectorStore::open(
            &path,
            Some(2),
            false,
            VectorStoreConfig { max_memory_bytes: 8 },
        )
        .unwrap();

        store.insert(1, &[1.0, 2.0]).unwrap();
        store.insert(2, &[3.0, 4.0]).unwrap();
        assert!(store.resident_memory_bytes() <= 8);
    }

    #[test]
    fn budgeted_store_evicted_vectors_retrievable_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budgeted.bin");
        let mut store = BudgetedVectorStore::open(
            &path,
            Some(2),
            false,
            VectorStoreConfig { max_memory_bytes: 8 },
        )
        .unwrap();

        store.insert(1, &[1.0, 2.0]).unwrap();
        store.insert(2, &[3.0, 4.0]).unwrap();
        assert_eq!(store.get(1), Some(&[1.0, 2.0][..]));
    }

    #[test]
    fn budgeted_store_non_evicted_vectors_still_available() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budgeted.bin");
        let mut store = BudgetedVectorStore::open(
            &path,
            Some(2),
            false,
            VectorStoreConfig { max_memory_bytes: 16 },
        )
        .unwrap();

        store.insert(1, &[1.0, 2.0]).unwrap();
        store.insert(2, &[3.0, 4.0]).unwrap();
        assert_eq!(store.get(2), Some(&[3.0, 4.0][..]));
    }

    #[test]
    fn budgeted_store_repeated_threshold_crossings_preserve_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budgeted.bin");
        let mut store = BudgetedVectorStore::open(
            &path,
            Some(2),
            false,
            VectorStoreConfig { max_memory_bytes: 8 },
        )
        .unwrap();

        for id in 0..10 {
            store.insert(id, &[id as f32, id as f32 + 1.0]).unwrap();
        }
        assert_eq!(store.len(), 10);
        assert_eq!(store.get(0), Some(&[0.0, 1.0][..]));
        assert_eq!(store.get(9), Some(&[9.0, 10.0][..]));
    }

    #[test]
    fn budgeted_store_recency_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budgeted.bin");
        let mut store = BudgetedVectorStore::open(
            &path,
            Some(2),
            false,
            VectorStoreConfig { max_memory_bytes: 8 },
        )
        .unwrap();

        store.insert(1, &[1.0, 1.0]).unwrap();
        store.insert(2, &[2.0, 2.0]).unwrap();
        // Oldest insert gets evicted first.
        assert!(!store.memory.contains_key(&1));
        assert!(store.memory.contains_key(&2));
    }

    #[test]
    fn budgeted_store_memory_stops_growing_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budgeted.bin");
        let mut store = BudgetedVectorStore::open(
            &path,
            Some(2),
            false,
            VectorStoreConfig { max_memory_bytes: 8 },
        )
        .unwrap();

        for id in 0..20 {
            store.insert(id, &[1.0, 2.0]).unwrap();
        }
        assert!(store.resident_memory_bytes() <= 8);
    }


    #[test]
    fn for_each_id_streams_ids_without_materialized_api() {
        let mut store = InMemoryVectorStore::new(Some(2), true);
        store.insert(1, &[1.0, 2.0]).unwrap();
        store.insert(2, &[3.0, 4.0]).unwrap();

        let mut seen = Vec::new();
        store.for_each_id(&mut |id| seen.push(id));
        seen.sort_unstable();
        assert_eq!(seen, vec![1, 2]);
    }


    #[test]
    fn mmap_overwrite_and_delete_trigger_reclaim_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compact.bin");
        let mut store = MmapVectorStore::open(&path, Some(2), true).unwrap();

        for _ in 0..10 {
            store.insert(1, &[1.0, 2.0]).unwrap();
        }
        let before_delete = std::fs::metadata(&path).unwrap().len();
        assert!(store.delete(1));
        assert!(!store.contains(1));

        store.insert(2, &[3.0, 4.0]).unwrap();
        let after_compact = std::fs::metadata(&path).unwrap().len();
        assert!(after_compact <= before_delete);
        assert_eq!(store.get(2), Some(&[3.0, 4.0][..]));
    }

    #[test]
    fn budgeted_store_delete_removes_from_memory_and_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budgeted_delete.bin");
        let mut store = BudgetedVectorStore::open(
            &path,
            Some(2),
            false,
            VectorStoreConfig { max_memory_bytes: 1024 },
        )
        .unwrap();

        store.insert(1, &[1.0, 2.0]).unwrap();
        assert!(store.delete(1));
        assert_eq!(store.get(1), None);
        assert!(!store.contains(1));
    }


    proptest! {
        #[test]
        fn in_memory_roundtrip_property(v in proptest::collection::vec(0.0f32..1000.0, 1..16), id in 1u64..10_000) {
            let mut store = InMemoryVectorStore::new(None, true);
            store.insert(id, &v).unwrap();
            let got = store.get(id).unwrap();
            prop_assert_eq!(got, &v[..]);
        }
    }

}
