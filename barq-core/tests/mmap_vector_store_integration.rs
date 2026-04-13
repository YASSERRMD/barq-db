use barq_core::storage::vector_store::{MmapVectorStore, VectorStore, VectorStoreError};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn mmap_create_insert_reopen_persists_vectors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vectors.bin");

    {
        let mut store = MmapVectorStore::open(&path, Some(3), false).unwrap();
        store.insert(10, &[1.0, 2.0, 3.0]).unwrap();
        store.insert(20, &[4.0, 5.0, 6.0]).unwrap();
        assert_eq!(store.len(), 2);
    }

    let reopened = MmapVectorStore::open(&path, Some(3), false).unwrap();
    assert_eq!(reopened.get(10), Some(&[1.0, 2.0, 3.0][..]));
    assert_eq!(reopened.get(20), Some(&[4.0, 5.0, 6.0][..]));
}

#[test]
fn mmap_random_access_multiple_ids_correct() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vectors.bin");
    let mut store = MmapVectorStore::open(&path, Some(2), false).unwrap();

    for (id, vector) in [(1, [1.0, 1.5]), (44, [2.0, 2.5]), (999, [3.0, 3.5])] {
        store.insert(id, &vector).unwrap();
    }

    assert_eq!(store.get(44), Some(&[2.0, 2.5][..]));
    assert_eq!(store.get(1), Some(&[1.0, 1.5][..]));
    assert_eq!(store.get(999), Some(&[3.0, 3.5][..]));
}

#[test]
fn mmap_corrupt_header_is_handled_safely() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vectors.bin");

    let mut f = OpenOptions::new().create(true).write(true).open(&path).unwrap();
    f.write_all(b"NOPE").unwrap();
    f.write_all(&[0_u8; 12]).unwrap();

    let err = MmapVectorStore::open(&path, Some(2), false).unwrap_err();
    assert!(matches!(err, VectorStoreError::CorruptData(msg) if msg.contains("magic")));
}

#[test]
fn mmap_truncated_record_is_handled_safely() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vectors.bin");

    {
        let mut store = MmapVectorStore::open(&path, Some(3), false).unwrap();
        store.insert(1, &[1.0, 2.0, 3.0]).unwrap();
    }

    let mut f = OpenOptions::new().write(true).open(&path).unwrap();
    let len = f.metadata().unwrap().len();
    f.set_len(len - 4).unwrap();

    let err = MmapVectorStore::open(&path, Some(3), false).unwrap_err();
    assert!(matches!(err, VectorStoreError::CorruptData(msg) if msg.contains("truncated")));
}

#[test]
fn mmap_invalid_offset_index_is_handled_safely() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vectors.bin");

    {
        let mut store = MmapVectorStore::open(&path, Some(2), false).unwrap();
        store.insert(3, &[7.0, 8.0]).unwrap();
    }

    // Corrupt vector length in first record to force invalid offset math.
    let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(16 + 8)).unwrap();
    f.write_all(&(u32::MAX).to_le_bytes()).unwrap();

    let err = MmapVectorStore::open(&path, Some(2), false).unwrap_err();
    assert!(matches!(err, VectorStoreError::CorruptData(msg) if msg.contains("truncated") || msg.contains("overflow")));
}

#[test]
fn mmap_restart_reads_previous_vectors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vectors.bin");

    {
        let mut store = MmapVectorStore::open(&path, Some(4), false).unwrap();
        store.insert(100, &[0.0, 1.0, 2.0, 3.0]).unwrap();
    }

    let reopened = MmapVectorStore::open(&path, Some(4), false).unwrap();
    assert_eq!(reopened.get(100), Some(&[0.0, 1.0, 2.0, 3.0][..]));
}
