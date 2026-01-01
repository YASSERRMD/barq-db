//! Storage Tiering Module
//!
//! Provides automatic data movement between hot, warm, and cold storage tiers.

use super::traits::{ObjectStore, ObjectStoreError};
use super::RetryingObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Storage tier levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageTier {
    /// Frequently accessed data - local SSD or high-performance storage
    Hot,
    /// Less frequently accessed data - standard cloud storage
    Warm,
    /// Rarely accessed data - cold/archive storage (S3 Glacier, GCS Archive, Azure Cool)
    Cold,
}

impl StorageTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
        }
    }
}

/// Configuration for a storage tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    /// Maximum age before data moves to the next tier (in seconds)
    pub max_age_secs: u64,
    /// Maximum size in bytes before triggering eviction
    pub max_size_bytes: Option<u64>,
    /// Whether this tier is enabled
    pub enabled: bool,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            max_age_secs: 86400 * 7, // 7 days
            max_size_bytes: None,
            enabled: true,
        }
    }
}

/// Policy for automatic data tiering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// Hot tier config (e.g., move to warm after 1 day)
    pub hot: TierConfig,
    /// Warm tier config (e.g., move to cold after 30 days)
    pub warm: TierConfig,
    /// Cold tier config (e.g., delete after 365 days, or None to keep forever)
    pub cold: TierConfig,
    /// Check interval in seconds
    pub check_interval_secs: u64,
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            hot: TierConfig {
                max_age_secs: 86400,      // 1 day
                max_size_bytes: Some(10 * 1024 * 1024 * 1024), // 10 GB
                enabled: true,
            },
            warm: TierConfig {
                max_age_secs: 86400 * 30, // 30 days
                max_size_bytes: None,
                enabled: true,
            },
            cold: TierConfig {
                max_age_secs: 86400 * 365, // 365 days (for deletion)
                max_size_bytes: None,
                enabled: true,
            },
            check_interval_secs: 3600, // Check every hour
        }
    }
}

/// Metadata about a tiered object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredObjectInfo {
    pub key: String,
    pub tier: StorageTier,
    pub size_bytes: u64,
    pub created_at: i64,
    pub last_accessed: i64,
    pub access_count: u64,
}

/// Manages storage tiering across multiple backends.
pub struct TieringManager {
    hot_store: Arc<dyn ObjectStore>,
    warm_store: Option<Arc<dyn ObjectStore>>,
    cold_store: Option<Arc<dyn ObjectStore>>,
    policy: TieringPolicy,
    metadata: Arc<RwLock<HashMap<String, TieredObjectInfo>>>,
}

impl std::fmt::Debug for TieringManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieringManager")
         .field("hot_store", &"ObjectStore")
         .field("warm_store", &self.warm_store.as_ref().map(|_| "ObjectStore"))
         .field("cold_store", &self.cold_store.as_ref().map(|_| "ObjectStore"))
         .field("policy", &self.policy)
         .finish()
    }
}

impl TieringManager {
    /// Create a new TieringManager with only hot storage.
    pub fn new(hot_store: Arc<dyn ObjectStore>) -> Self {
        // Enforce retry logic
        let hot_store: Arc<dyn ObjectStore> = Arc::new(RetryingObjectStore::new(hot_store));

        Self {
            hot_store,
            warm_store: None,
            cold_store: None,
            policy: TieringPolicy::default(),
            metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create with all three tiers.
    pub fn with_tiers(
        hot_store: Arc<dyn ObjectStore>,
        warm_store: Option<Arc<dyn ObjectStore>>,
        cold_store: Option<Arc<dyn ObjectStore>>,
        policy: TieringPolicy,
    ) -> Self {
        // Enforce retry logic for all stores
        let hot_store: Arc<dyn ObjectStore> = Arc::new(RetryingObjectStore::new(hot_store));
        
        let warm_store = warm_store.map(|s| {
            let s: Arc<dyn ObjectStore> = Arc::new(RetryingObjectStore::new(s));
            s
        });

        let cold_store = cold_store.map(|s| {
            let s: Arc<dyn ObjectStore> = Arc::new(RetryingObjectStore::new(s));
            s
        });

        Self {
            hot_store,
            warm_store,
            cold_store,
            policy,
            metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the appropriate store for a tier.
    fn get_store(&self, tier: StorageTier) -> Option<&Arc<dyn ObjectStore>> {
        match tier {
            StorageTier::Hot => Some(&self.hot_store),
            StorageTier::Warm => self.warm_store.as_ref(),
            StorageTier::Cold => self.cold_store.as_ref(),
        }
    }

    /// Upload data to hot tier.
    pub fn upload(&self, local_path: &Path, key: &str) -> Result<(), ObjectStoreError> {
        let remote_path = PathBuf::from(key);
        self.hot_store.upload_file(local_path, &remote_path)?;
        
        let metadata = std::fs::metadata(local_path)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        
        let info = TieredObjectInfo {
            key: key.to_string(),
            tier: StorageTier::Hot,
            size_bytes: metadata.len(),
            created_at: now,
            last_accessed: now,
            access_count: 0,
        };
        
        self.metadata.write().unwrap().insert(key.to_string(), info);
        Ok(())
    }

    /// Download data from the appropriate tier.
    pub fn download(&self, key: &str, local_path: &Path) -> Result<StorageTier, ObjectStoreError> {
        let remote_path = PathBuf::from(key);
        
        // Find which tier has the data
        let tier = {
            let meta = self.metadata.read().unwrap();
            meta.get(key).map(|i| i.tier).unwrap_or(StorageTier::Hot)
        };
        
        let store = self.get_store(tier).ok_or_else(|| {
            ObjectStoreError::NotFound(format!("No store configured for tier {:?}", tier))
        })?;
        
        store.download_file(&remote_path, local_path)?;
        
        // Update access metadata
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        if let Some(info) = self.metadata.write().unwrap().get_mut(key) {
            info.last_accessed = now;
            info.access_count += 1;
        }
        
        Ok(tier)
    }

    /// Move an object to a different tier.
    pub fn move_to_tier(&self, key: &str, target_tier: StorageTier) -> Result<(), ObjectStoreError> {
        let current_tier = {
            let meta = self.metadata.read().unwrap();
            meta.get(key).map(|i| i.tier).unwrap_or(StorageTier::Hot)
        };
        
        if current_tier == target_tier {
            return Ok(());
        }
        
        let source_store = self.get_store(current_tier).ok_or_else(|| {
            ObjectStoreError::NotFound(format!("No store for tier {:?}", current_tier))
        })?;
        
        let target_store = self.get_store(target_tier).ok_or_else(|| {
            ObjectStoreError::Configuration(format!("Target tier {:?} not configured", target_tier))
        })?;
        
        let remote_path = PathBuf::from(key);
        
        // Download from source
        let temp_dir = std::env::temp_dir().join("barq_tiering");
        std::fs::create_dir_all(&temp_dir)?;
        let temp_file = temp_dir.join(key.replace('/', "_"));
        source_store.download_file(&remote_path, &temp_file)?;
        
        // Upload to target
        target_store.upload_file(&temp_file, &remote_path)?;
        
        // Delete from source
        source_store.delete(&remote_path)?;
        
        // Cleanup temp
        let _ = std::fs::remove_file(&temp_file);
        
        // Update metadata
        if let Some(info) = self.metadata.write().unwrap().get_mut(key) {
            info.tier = target_tier;
        }
        
        Ok(())
    }

    /// Run tiering policy and move data as needed.
    pub fn enforce_policy(&self) -> Result<TieringStats, ObjectStoreError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let mut stats = TieringStats::default();
        
        let keys_to_process: Vec<(String, TieredObjectInfo)> = {
            let meta = self.metadata.read().unwrap();
            meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        
        for (key, info) in keys_to_process {
            let age_secs = (now - info.created_at) as u64;
            
            match info.tier {
                StorageTier::Hot => {
                    if self.policy.hot.enabled && age_secs > self.policy.hot.max_age_secs {
                        if self.warm_store.is_some() {
                            self.move_to_tier(&key, StorageTier::Warm)?;
                            stats.moved_to_warm += 1;
                            stats.bytes_moved += info.size_bytes;
                        }
                    }
                }
                StorageTier::Warm => {
                    if self.policy.warm.enabled && age_secs > self.policy.warm.max_age_secs {
                        if self.cold_store.is_some() {
                            self.move_to_tier(&key, StorageTier::Cold)?;
                            stats.moved_to_cold += 1;
                            stats.bytes_moved += info.size_bytes;
                        }
                    }
                }
                StorageTier::Cold => {
                    // Optionally delete very old data
                    if self.policy.cold.enabled && age_secs > self.policy.cold.max_age_secs {
                        if let Some(store) = self.get_store(StorageTier::Cold) {
                            store.delete(Path::new(&key))?;
                            self.metadata.write().unwrap().remove(&key);
                            stats.deleted += 1;
                            stats.bytes_deleted += info.size_bytes;
                        }
                    }
                }
            }
        }
        
        Ok(stats)
    }

    /// Get statistics about current tier usage.
    pub fn get_stats(&self) -> TierStats {
        let meta = self.metadata.read().unwrap();
        let mut stats = TierStats::default();
        
        for info in meta.values() {
            match info.tier {
                StorageTier::Hot => {
                    stats.hot_objects += 1;
                    stats.hot_bytes += info.size_bytes;
                }
                StorageTier::Warm => {
                    stats.warm_objects += 1;
                    stats.warm_bytes += info.size_bytes;
                }
                StorageTier::Cold => {
                    stats.cold_objects += 1;
                    stats.cold_bytes += info.size_bytes;
                }
            }
        }
        stats
    }

    /// Delete an object from all tiers.
    pub fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let tier = {
            let meta = self.metadata.read().unwrap();
            meta.get(key).map(|i| i.tier)
        };
        
        if let Some(tier) = tier {
            if let Some(store) = self.get_store(tier) {
                store.delete(Path::new(key))?;
            }
        }
        
        self.metadata.write().unwrap().remove(key);
        Ok(())
    }
}

/// Statistics from a tiering enforcement run.
#[derive(Debug, Default)]
pub struct TieringStats {
    pub moved_to_warm: u64,
    pub moved_to_cold: u64,
    pub deleted: u64,
    pub bytes_moved: u64,
    pub bytes_deleted: u64,
}

/// Current tier usage statistics.
#[derive(Debug, Default)]
pub struct TierStats {
    pub hot_objects: u64,
    pub hot_bytes: u64,
    pub warm_objects: u64,
    pub warm_bytes: u64,
    pub cold_objects: u64,
    pub cold_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::LocalObjectStore;
    use tempfile::TempDir;

    #[test]
    fn test_upload_and_download() {
        let temp = TempDir::new().unwrap();
        let hot_store = Arc::new(LocalObjectStore::new(temp.path().join("hot")).unwrap());
        let manager = TieringManager::new(hot_store);

        // Create test file
        let src = temp.path().join("test.txt");
        std::fs::write(&src, b"test data").unwrap();

        manager.upload(&src, "my/file.txt").unwrap();

        let dst = temp.path().join("downloaded.txt");
        let tier = manager.download("my/file.txt", &dst).unwrap();
        
        assert_eq!(tier, StorageTier::Hot);
        assert_eq!(std::fs::read_to_string(&dst).unwrap(), "test data");
    }

    #[test]
    fn test_move_between_tiers() {
        let temp = TempDir::new().unwrap();
        let hot = Arc::new(LocalObjectStore::new(temp.path().join("hot")).unwrap());
        let warm = Arc::new(LocalObjectStore::new(temp.path().join("warm")).unwrap());
        
        let manager = TieringManager::with_tiers(
            hot,
            Some(warm),
            None,
            TieringPolicy::default(),
        );

        let src = temp.path().join("test.txt");
        std::fs::write(&src, b"tier test").unwrap();

        manager.upload(&src, "data.txt").unwrap();
        manager.move_to_tier("data.txt", StorageTier::Warm).unwrap();

        let stats = manager.get_stats();
        assert_eq!(stats.warm_objects, 1);
        assert_eq!(stats.hot_objects, 0);
    }
}
