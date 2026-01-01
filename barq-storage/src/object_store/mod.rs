//! Object Storage Module
//!
//! This module provides abstraction for cloud object storage providers
//! including S3, GCS, Azure Blob Storage, and local filesystem.

mod local;
mod traits;
mod retry;

#[cfg(feature = "s3")]
mod s3;

#[cfg(feature = "gcs")]
mod gcs;

#[cfg(feature = "azure")]
mod azure;

mod tiering;

// Re-exports
pub use local::LocalObjectStore;
pub use traits::{ObjectMetadata, ObjectStore, ObjectStoreError};
pub use tiering::{StorageTier, TieringPolicy, TieringManager, TierConfig};
pub use retry::{RetryConfig, RetryingObjectStore, with_retry, is_retryable};

#[cfg(feature = "s3")]
pub use s3::S3ObjectStore;

#[cfg(feature = "gcs")]
pub use gcs::GcsObjectStore;

#[cfg(feature = "azure")]
pub use azure::AzureBlobStore;

