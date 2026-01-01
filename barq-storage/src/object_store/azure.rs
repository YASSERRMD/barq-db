//! Azure Blob Storage Object Store Implementation

use super::traits::{ObjectMetadata, ObjectStore, ObjectStoreError};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use futures::StreamExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;
use walkdir::WalkDir;

/// Azure Blob Storage object store.
#[derive(Clone)]
pub struct AzureBlobStore {
    container_client: ContainerClient,
    prefix: Option<PathBuf>,
    runtime: std::sync::Arc<Runtime>,
}

impl AzureBlobStore {
    pub fn new(account: impl Into<String>, access_key: impl Into<String>, container: impl Into<String>) -> Result<Self, ObjectStoreError> {
        let runtime = Runtime::new().map_err(|e| ObjectStoreError::Configuration(e.to_string()))?;
        let account = account.into();
        let credentials = StorageCredentials::access_key(&account, access_key.into());
        let container_client = BlobServiceClient::new(&account, credentials).container_client(container.into());
        Ok(Self { container_client, prefix: None, runtime: std::sync::Arc::new(runtime) })
    }

    pub fn from_env(container: impl Into<String>) -> Result<Self, ObjectStoreError> {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT").map_err(|_| ObjectStoreError::Configuration("AZURE_STORAGE_ACCOUNT not set".into()))?;
        let key = std::env::var("AZURE_STORAGE_KEY").map_err(|_| ObjectStoreError::Configuration("AZURE_STORAGE_KEY not set".into()))?;
        Self::new(account, key, container)
    }

    fn key_from_path(&self, path: &Path) -> String {
        let mut key = PathBuf::new();
        if let Some(base) = &self.prefix { key.push(base); }
        key.push(path);
        key.iter().map(|c| c.to_string_lossy()).collect::<Vec<_>>().join("/")
    }

    fn blob_client(&self, key: &str) -> BlobClient { self.container_client.blob_client(key) }
    fn run_async<F, T>(&self, fut: F) -> Result<T, ObjectStoreError> where F: std::future::Future<Output = Result<T, ObjectStoreError>> { self.runtime.block_on(fut) }
}

impl ObjectStore for AzureBlobStore {
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), ObjectStoreError> {
        for entry in WalkDir::new(local_dir).into_iter().filter_map(|e| e.ok()).filter(|e| e.file_type().is_file()) {
            let relative = entry.path().strip_prefix(local_dir).map_err(|e| ObjectStoreError::InvalidPath(e.to_string()))?;
            self.upload_file(entry.path(), &remote_prefix.join(relative))?;
        }
        Ok(())
    }

    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), ObjectStoreError> {
        let prefix = self.key_from_path(remote_prefix);
        std::fs::create_dir_all(local_dir)?;
        self.run_async(async {
            let mut stream = self.container_client.list_blobs().prefix(prefix.clone()).into_stream();
            while let Some(Ok(page)) = stream.next().await {
                for blob in page.blobs.blobs() {
                    let rel = blob.name.trim_start_matches(&prefix).trim_start_matches('/');
                    if !rel.is_empty() {
                        let dest = local_dir.join(rel);
                        if let Some(p) = dest.parent() { std::fs::create_dir_all(p)?; }
                        let data: Vec<u8> = self.blob_client(&blob.name).get_content().await.map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
                        std::fs::write(&dest, data)?;
                    }
                }
            }
            Ok(())
        })
    }

    fn upload_file(&self, local_path: &Path, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        let data = std::fs::read(local_path)?;
        self.run_async(async { self.blob_client(&key).put_block_blob(data).await.map_err(|e| ObjectStoreError::Provider(e.to_string()))?; Ok(()) })
    }

    fn download_file(&self, remote_key: &Path, local_path: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            if let Some(p) = local_path.parent() { std::fs::create_dir_all(p)?; }
            let data: Vec<u8> = self.blob_client(&key).get_content().await.map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            std::fs::write(local_path, data)?;
            Ok(())
        })
    }

    fn delete(&self, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async { self.blob_client(&key).delete().await.map_err(|e| ObjectStoreError::Provider(e.to_string()))?; Ok(()) })
    }

    fn exists(&self, remote_key: &Path) -> Result<bool, ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async { self.blob_client(&key).exists().await.map_err(|e| ObjectStoreError::Provider(e.to_string())) })
    }

    fn get_metadata(&self, remote_key: &Path) -> Result<ObjectMetadata, ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            let props = self.blob_client(&key).get_properties().await.map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            Ok(ObjectMetadata { 
                size: props.blob.properties.content_length, 
                last_modified: props.blob.properties.last_modified.unix_timestamp(), 
                content_type: Some(props.blob.properties.content_type.to_string()),
                etag: Some(props.blob.properties.etag.to_string()), 
                custom_metadata: HashMap::new() 
            })
        })
    }

    fn list(&self, prefix: &Path) -> Result<Vec<String>, ObjectStoreError> {
        let key_prefix = self.key_from_path(prefix);
        self.run_async(async {
            let mut results = Vec::new();
            let mut stream = self.container_client.list_blobs().prefix(key_prefix).into_stream();
            while let Some(Ok(page)) = stream.next().await { for blob in page.blobs.blobs() { results.push(blob.name.clone()); } }
            Ok(results)
        })
    }

    fn copy(&self, src: &Path, dst: &Path) -> Result<(), ObjectStoreError> {
        let src_key = self.key_from_path(src);
        let dst_key = self.key_from_path(dst);
        self.run_async(async {
            let url = self.blob_client(&src_key).url().map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            self.blob_client(&dst_key).copy(url).await.map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            Ok(())
        })
    }

    fn store_type(&self) -> &'static str { "azure" }
}
