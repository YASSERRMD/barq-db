//! Google Cloud Storage Object Store Implementation

use super::traits::{ObjectMetadata, ObjectStore, ObjectStoreError};
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::objects::copy::CopyObjectRequest;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;
use walkdir::WalkDir;

/// Google Cloud Storage object store implementation.
#[derive(Clone)]
pub struct GcsObjectStore {
    client: Client,
    bucket: String,
    prefix: Option<PathBuf>,
    runtime: std::sync::Arc<Runtime>,
}

impl GcsObjectStore {
    pub fn new(bucket: impl Into<String>) -> Result<Self, ObjectStoreError> {
        Self::with_prefix(bucket, None::<PathBuf>)
    }

    pub fn with_prefix(
        bucket: impl Into<String>,
        prefix: Option<impl AsRef<Path>>,
    ) -> Result<Self, ObjectStoreError> {
        let runtime = Runtime::new()
            .map_err(|e| ObjectStoreError::Configuration(e.to_string()))?;
        let bucket = bucket.into();
        let prefix = prefix.map(|p| p.as_ref().to_path_buf());

        let client = runtime.block_on(async {
            let config = ClientConfig::default()
                .with_auth()
                .await
                .map_err(|e| ObjectStoreError::Configuration(e.to_string()))?;
            Ok::<Client, ObjectStoreError>(Client::new(config))
        })?;

        Ok(Self {
            client,
            bucket,
            prefix,
            runtime: std::sync::Arc::new(runtime),
        })
    }

    pub fn anonymous(bucket: impl Into<String>) -> Result<Self, ObjectStoreError> {
        let runtime = Runtime::new()
            .map_err(|e| ObjectStoreError::Configuration(e.to_string()))?;
        let bucket = bucket.into();

        let client = Client::new(ClientConfig::default());

        Ok(Self {
            client,
            bucket,
            prefix: None,
            runtime: std::sync::Arc::new(runtime),
        })
    }

    fn full_key(&self, prefix: &Path, relative: &Path) -> String {
        let mut key = PathBuf::new();
        if let Some(base) = &self.prefix {
            key.push(base);
        }
        key.push(prefix);
        key.push(relative);
        key.iter()
            .map(|c| c.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    }

    fn key_from_path(&self, path: &Path) -> String {
        let mut key = PathBuf::new();
        if let Some(base) = &self.prefix {
            key.push(base);
        }
        key.push(path);
        key.iter()
            .map(|c| c.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    }

    fn run_async<F, T>(&self, fut: F) -> Result<T, ObjectStoreError>
    where
        F: std::future::Future<Output = Result<T, ObjectStoreError>>,
    {
        self.runtime.block_on(fut)
    }

    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), ObjectStoreError> {
        let upload_type = UploadType::Simple(Media::new(key.to_string()));
        self.client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    ..Default::default()
                },
                data,
                &upload_type,
            )
            .await
            .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
        Ok(())
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        let data = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: key.to_string(),
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
        Ok(data)
    }
}

impl ObjectStore for GcsObjectStore {
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), ObjectStoreError> {
        self.run_async(async {
            for entry in WalkDir::new(local_dir) {
                let entry = entry.map_err(|e| ObjectStoreError::Io(e.into()))?;
                if entry.file_type().is_dir() {
                    continue;
                }
                let relative = entry
                    .path()
                    .strip_prefix(local_dir)
                    .map_err(|e| ObjectStoreError::InvalidPath(e.to_string()))?;
                let key = self.full_key(remote_prefix, relative);
                let data = std::fs::read(entry.path())?;
                self.upload_bytes(&key, data).await?;
            }
            Ok(())
        })
    }

    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), ObjectStoreError> {
        self.run_async(async {
            if local_dir.exists() {
                std::fs::remove_dir_all(local_dir)?;
            }
            std::fs::create_dir_all(local_dir)?;

            let prefix = self.key_from_path(remote_prefix);
            let mut page_token: Option<String> = None;

            loop {
                let resp = self
                    .client
                    .list_objects(&ListObjectsRequest {
                        bucket: self.bucket.clone(),
                        prefix: Some(prefix.clone()),
                        page_token: page_token.clone(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;

                if let Some(items) = resp.items {
                    for object in items {
                        let key = object.name;
                        let rel = key.trim_start_matches(&prefix);
                        let rel_path = Path::new(rel.trim_start_matches('/'));
                        if rel_path.as_os_str().is_empty() {
                            continue;
                        }
                        let dest_path = local_dir.join(rel_path);
                        if let Some(parent) = dest_path.parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        let data = self.download_bytes(&key).await?;
                        std::fs::write(&dest_path, data)?;
                    }
                }

                if let Some(token) = resp.next_page_token {
                    page_token = Some(token);
                } else {
                    break;
                }
            }
            Ok(())
        })
    }

    fn upload_file(&self, local_path: &Path, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            let data = std::fs::read(local_path)?;
            self.upload_bytes(&key, data).await
        })
    }

    fn download_file(&self, remote_key: &Path, local_path: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let data = self.download_bytes(&key).await?;
            std::fs::write(local_path, data)?;
            Ok(())
        })
    }

    fn delete(&self, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            self.client
                .delete_object(&DeleteObjectRequest {
                    bucket: self.bucket.clone(),
                    object: key,
                    ..Default::default()
                })
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            Ok(())
        })
    }

    fn exists(&self, remote_key: &Path) -> Result<bool, ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            match self
                .client
                .get_object(&GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: key,
                    ..Default::default()
                })
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("404") || err_str.contains("Not Found") {
                        Ok(false)
                    } else {
                        Err(ObjectStoreError::Provider(err_str))
                    }
                }
            }
        })
    }

    fn get_metadata(&self, remote_key: &Path) -> Result<ObjectMetadata, ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            let obj = self
                .client
                .get_object(&GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: key,
                    ..Default::default()
                })
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;

            let last_modified = obj
                .updated
                .map(|t| t.unix_timestamp())
                .unwrap_or(0);

            Ok(ObjectMetadata {
                size: obj.size as u64,
                last_modified,
                content_type: obj.content_type,
                etag: Some(obj.etag),
                custom_metadata: obj.metadata.unwrap_or_default(),
            })
        })
    }

    fn list(&self, prefix: &Path) -> Result<Vec<String>, ObjectStoreError> {
        let key_prefix = self.key_from_path(prefix);
        self.run_async(async {
            let mut results = Vec::new();
            let mut page_token: Option<String> = None;

            loop {
                let resp = self
                    .client
                    .list_objects(&ListObjectsRequest {
                        bucket: self.bucket.clone(),
                        prefix: Some(key_prefix.clone()),
                        page_token: page_token.clone(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;

                if let Some(items) = resp.items {
                    for object in items {
                        results.push(object.name);
                    }
                }

                if let Some(token) = resp.next_page_token {
                    page_token = Some(token);
                } else {
                    break;
                }
            }
            Ok(results)
        })
    }

    fn copy(&self, src: &Path, dst: &Path) -> Result<(), ObjectStoreError> {
        let src_key = self.key_from_path(src);
        let dst_key = self.key_from_path(dst);

        self.run_async(async {
            self.client
                .copy_object(&CopyObjectRequest {
                    source_bucket: self.bucket.clone(),
                    source_object: src_key,
                    destination_bucket: self.bucket.clone(),
                    destination_object: dst_key,
                    ..Default::default()
                })
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            Ok(())
        })
    }

    fn store_type(&self) -> &'static str {
        "gcs"
    }
}
