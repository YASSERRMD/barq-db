//! AWS S3 Object Store Implementation

use super::traits::{ObjectMetadata, ObjectStore, ObjectStoreError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;
use walkdir::WalkDir;

/// AWS S3 object store implementation.
#[derive(Clone)]
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
    prefix: Option<PathBuf>,
    runtime: std::sync::Arc<Runtime>,
}

impl S3ObjectStore {
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
            let config = aws_config::load_from_env().await;
            Client::new(&config)
        });

        Ok(Self {
            client,
            bucket,
            prefix,
            runtime: std::sync::Arc::new(runtime),
        })
    }

    pub fn with_endpoint(
        bucket: impl Into<String>,
        endpoint_url: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, ObjectStoreError> {
        let runtime = Runtime::new()
            .map_err(|e| ObjectStoreError::Configuration(e.to_string()))?;
        let bucket = bucket.into();
        let endpoint = endpoint_url.into();
        let region_str = region.into();

        let client = runtime.block_on(async {
            let config = aws_config::from_env()
                .endpoint_url(&endpoint)
                .region(aws_sdk_s3::config::Region::new(region_str))
                .load()
                .await;
            Client::new(&config)
        });

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

    async fn upload_dir_async(
        &self,
        local_dir: &Path,
        remote_prefix: &Path,
    ) -> Result<(), ObjectStoreError> {
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
            let body = ByteStream::from_path(entry.path())
                .await
                .map_err(|e| ObjectStoreError::Io(std::io::Error::other(e.to_string())))?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
        }
        Ok(())
    }

    async fn download_dir_async(
        &self,
        remote_prefix: &Path,
        local_dir: &Path,
    ) -> Result<(), ObjectStoreError> {
        if local_dir.exists() {
            std::fs::remove_dir_all(local_dir)?;
        }
        std::fs::create_dir_all(local_dir)?;

        let prefix = self.key_from_path(remote_prefix);
        let mut continuation_token = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;

            for object in resp.contents() {
                if let Some(key) = object.key() {
                    let rel = key.trim_start_matches(&prefix);
                    let rel_path = Path::new(rel.trim_start_matches('/'));
                    if rel_path.as_os_str().is_empty() {
                        continue;
                    }
                    let dest_path = local_dir.join(rel_path);
                    if let Some(parent) = dest_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let body = self
                        .client
                        .get_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await
                        .map_err(|e| ObjectStoreError::Provider(e.to_string()))?
                        .body
                        .collect()
                        .await
                        .map_err(|e| ObjectStoreError::Network(e.to_string()))?;
                    std::fs::write(&dest_path, body.into_bytes())?;
                }
            }
            
            if let Some(token) = resp.next_continuation_token() {
                continuation_token = Some(token.to_string());
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl ObjectStore for S3ObjectStore {
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), ObjectStoreError> {
        self.run_async(self.upload_dir_async(local_dir, remote_prefix))
    }

    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), ObjectStoreError> {
        self.run_async(self.download_dir_async(remote_prefix, local_dir))
    }

    fn upload_file(&self, local_path: &Path, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            let body = ByteStream::from_path(local_path)
                .await
                .map_err(|e| ObjectStoreError::Io(std::io::Error::other(e.to_string())))?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            Ok(())
        })
    }

    fn download_file(&self, remote_key: &Path, local_path: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let resp = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            let body = resp
                .body
                .collect()
                .await
                .map_err(|e| ObjectStoreError::Network(e.to_string()))?;
            std::fs::write(local_path, body.into_bytes())?;
            Ok(())
        })
    }

    fn delete(&self, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
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
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    let service_err = e.into_service_error();
                    if service_err.is_not_found() {
                        Ok(false)
                    } else {
                        Err(ObjectStoreError::Provider(service_err.to_string()))
                    }
                }
            }
        })
    }

    fn get_metadata(&self, remote_key: &Path) -> Result<ObjectMetadata, ObjectStoreError> {
        let key = self.key_from_path(remote_key);
        self.run_async(async {
            let resp = self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;

            let last_modified = resp
                .last_modified()
                .map(|t| t.secs())
                .unwrap_or(0);

            Ok(ObjectMetadata {
                size: resp.content_length().unwrap_or(0) as u64,
                last_modified,
                content_type: resp.content_type().map(|s| s.to_string()),
                etag: resp.e_tag().map(|s| s.to_string()),
                custom_metadata: resp
                    .metadata()
                    .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default(),
            })
        })
    }

    fn list(&self, prefix: &Path) -> Result<Vec<String>, ObjectStoreError> {
        let key_prefix = self.key_from_path(prefix);
        self.run_async(async {
            let mut results = Vec::new();
            let mut continuation_token = None;

            loop {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&key_prefix);
                if let Some(token) = continuation_token {
                    req = req.continuation_token(token);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;

                for object in resp.contents() {
                    if let Some(key) = object.key() {
                        results.push(key.to_string());
                    }
                }

                if let Some(token) = resp.next_continuation_token() {
                    continuation_token = Some(token.to_string());
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
        let copy_source = format!("{}/{}", self.bucket, src_key);

        self.run_async(async {
            self.client
                .copy_object()
                .bucket(&self.bucket)
                .copy_source(copy_source)
                .key(dst_key)
                .send()
                .await
                .map_err(|e| ObjectStoreError::Provider(e.to_string()))?;
            Ok(())
        })
    }

    fn store_type(&self) -> &'static str {
        "s3"
    }
}
