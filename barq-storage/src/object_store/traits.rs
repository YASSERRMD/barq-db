//! Object Store Trait Definitions
//!
//! Defines the common interface for all object storage backends.

use std::path::Path;
use thiserror::Error;

/// Errors that can occur during object store operations.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Object not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Provider error: {0}")]
    Provider(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

/// Metadata about a stored object.
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Size in bytes
    pub size: u64,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified: i64,
    /// Content type/MIME type
    pub content_type: Option<String>,
    /// ETag or version identifier
    pub etag: Option<String>,
    /// Custom metadata
    pub custom_metadata: std::collections::HashMap<String, String>,
}

/// Abstraction over snapshot/segment upload targets.
///
/// This trait provides a unified interface for interacting with various
/// object storage backends including local filesystem, S3, GCS, and Azure Blob.
pub trait ObjectStore: Send + Sync {
    /// Upload the contents of a local directory into a destination prefix.
    /// Existing data at the destination may be replaced.
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), ObjectStoreError>;

    /// Download the contents of a remote prefix into a local directory.
    /// Existing data in the destination may be replaced.
    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), ObjectStoreError>;

    /// Upload a single file to the object store.
    fn upload_file(&self, local_path: &Path, remote_key: &Path) -> Result<(), ObjectStoreError>;

    /// Download a single file from the object store.
    fn download_file(&self, remote_key: &Path, local_path: &Path) -> Result<(), ObjectStoreError>;

    /// Delete an object or prefix from the object store.
    fn delete(&self, remote_key: &Path) -> Result<(), ObjectStoreError>;

    /// Check if an object exists at the given path.
    fn exists(&self, remote_key: &Path) -> Result<bool, ObjectStoreError>;

    /// Get metadata for an object.
    fn get_metadata(&self, remote_key: &Path) -> Result<ObjectMetadata, ObjectStoreError>;

    /// List objects under a prefix.
    fn list(&self, prefix: &Path) -> Result<Vec<String>, ObjectStoreError>;

    /// Copy an object from one location to another within the same store.
    fn copy(&self, src: &Path, dst: &Path) -> Result<(), ObjectStoreError>;

    /// Move an object from one location to another within the same store.
    fn move_object(&self, src: &Path, dst: &Path) -> Result<(), ObjectStoreError> {
        self.copy(src, dst)?;
        self.delete(src)?;
        Ok(())
    }

    /// Get the name/type of this object store for logging.
    fn store_type(&self) -> &'static str;
}
