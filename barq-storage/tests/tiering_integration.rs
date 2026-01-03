use barq_storage::{
    TieringManager, TieringPolicy, LocalObjectStore, StorageTier, ObjectStore,
    Storage,
};
use std::sync::Arc;
use barq_core::{Document, DocumentId, PayloadValue, CollectionSchema, FieldSchema, FieldType, DistanceMetric};

#[test]
fn test_tiering_integration_flush_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let hot_store_root = dir.path().to_path_buf(); // Same as Storage root
    
    // 1. Setup Storage with TieringManager
    let mut storage = Storage::open(dir.path()).unwrap();
    
    let hot_store = Arc::new(LocalObjectStore::new(hot_store_root.clone()).unwrap());
    let warm_dir = tempfile::tempdir().unwrap();
    let warm_store = Arc::new(LocalObjectStore::new(warm_dir.path()).unwrap());
    
    let tm = Arc::new(TieringManager::with_tiers(
        hot_store,
        Some(warm_store.clone()),
        None,
        TieringPolicy::default()
    ));
    
    // Set state path for persistence
    tm.set_state_path(dir.path().join("tiering_state.json"));
    storage.set_tiering_manager(tm.clone());

    // 2. Create Collection and Insert Data
    let schema = CollectionSchema {
        name: "tiered_coll".to_string(),
        fields: vec![FieldSchema {
            name: "vector".to_string(),
            field_type: FieldType::Vector {
                dimension: 2,
                metric: DistanceMetric::L2,
                index: None,
            },
            required: true,
        }],
        bm25_config: None,
        tenant_id: Default::default(),
    };
    storage.create_collection(schema.clone()).unwrap();

    storage.insert(
        "tiered_coll",
        Document {
            id: DocumentId::U64(1),
            vector: vec![0.5, 0.5],
            payload: Some(PayloadValue::String("hot".to_string())),
        },
        false
    ).unwrap();

    // 3. Flush WAL -> Segment
    // This should register the segment with TieringManager
    storage.flush_wal_to_segment(&Default::default(), "tiered_coll").unwrap();

    // Verify segment is registered (Hot)
    let stats = tm.get_stats();
    assert_eq!(stats.hot_objects, 1, "Segment should be registered as Hot");

    // 4. Move Segment to Warm Tier (Simulate Tiering)
    // We need to find the key. It's tenants/default/collections/tiered_coll/segments/segment_....jsonl
    let keys = tm.list_keys_with_prefix("");
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    
    // Move to Warm
    tm.move_to_tier(key, StorageTier::Warm).unwrap();
    
    let stats = tm.get_stats();
    assert_eq!(stats.hot_objects, 0);
    assert_eq!(stats.warm_objects, 1);
    
    // Verify file is GONE from Hot store (local fs)
    let local_path = hot_store_root.join(key);
    assert!(!local_path.exists(), "File should be deleted from Hot tier");

    // 5. Restart Storage (Replay)
    // This should detect missing local file, query TieringManager, download from Warm, and restore.
    drop(storage);
    
    // Re-open storage
    let storage2 = Storage::open_with_options(
        dir.path(),
        barq_storage::StorageOptions {
             tiering_manager: Some(tm.clone()),
        }
    ).unwrap();
}
