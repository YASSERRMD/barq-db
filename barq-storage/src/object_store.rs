use crate::StorageError;
use std::fs;
use std::path::{Path, PathBuf};

/// Abstraction over snapshot/segment upload targets.
pub trait ObjectStore: Send + Sync {
    /// Upload the contents of a local directory into a destination prefix.
    /// Existing data at the destination may be replaced.
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), StorageError>;

    /// Download the contents of a remote prefix into a local directory.
    /// Existing data in the destination may be replaced.
    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), StorageError>;
}

/// Local filesystem-based object store. Useful for testing backup flows.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn destination(&self, prefix: &Path) -> PathBuf {
        self.root.join(prefix)
    }
}

impl ObjectStore for LocalObjectStore {
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), StorageError> {
        let dest = self.destination(remote_prefix);
        if dest.exists() {
            fs::remove_dir_all(&dest)?;
        }
        crate::Storage::copy_dir_recursively(local_dir, &dest)?;
        Ok(())
    }

    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), StorageError> {
        let src = self.destination(remote_prefix);
        if !src.exists() {
            return Err(StorageError::SnapshotNotFound(
                src.to_string_lossy().to_string(),
            ));
        }
        if local_dir.exists() {
            fs::remove_dir_all(local_dir)?;
        }
        crate::Storage::copy_dir_recursively(&src, local_dir)?;
        Ok(())
    }
}

#[cfg(feature = "s3")]
mod s3_store {
    use super::ObjectStore;
    use crate::StorageError;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::{Client, Error as S3Error};
    use std::path::{Path, PathBuf};
    use tokio::runtime::Runtime;
    use walkdir::WalkDir;

    #[derive(Clone)]
    pub struct S3ObjectStore {
        client: Client,
        bucket: String,
        prefix: Option<PathBuf>,
    }

    impl S3ObjectStore {
        pub fn new(bucket: impl Into<String>) -> Result<Self, StorageError> {
            Self::with_prefix(bucket, None::<PathBuf>)
        }

        pub fn with_prefix(
            bucket: impl Into<String>,
            prefix: Option<impl AsRef<Path>>,
        ) -> Result<Self, StorageError> {
            let runtime = Runtime::new().map_err(|e| StorageError::ObjectStore(e.to_string()))?;
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

        fn run_async<F>(&self, fut: F) -> Result<F::Output, StorageError>
        where
            F: std::future::Future,
        {
            let runtime = Runtime::new().map_err(|e| StorageError::ObjectStore(e.to_string()))?;
            Ok(runtime.block_on(fut))
        }

        async fn upload_dir_async(
            &self,
            local_dir: &Path,
            remote_prefix: &Path,
        ) -> Result<(), StorageError> {
            for entry in WalkDir::new(local_dir) {
                let entry = entry.map_err(|e| StorageError::ObjectStore(e.to_string()))?;
                if entry.file_type().is_dir() {
                    continue;
                }
                let relative = entry
                    .path()
                    .strip_prefix(local_dir)
                    .map_err(|e| StorageError::ObjectStore(e.to_string()))?;
                let key = self.full_key(remote_prefix, relative);
                let body = ByteStream::from_path(entry.path())
                    .await
                    .map_err(|e| StorageError::ObjectStore(e.to_string()))?;
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(body)
                    .send()
                    .await
                    .map_err(map_s3_error)?;
            }
            Ok(())
        }

        async fn download_dir_async(
            &self,
            remote_prefix: &Path,
            local_dir: &Path,
        ) -> Result<(), StorageError> {
            if local_dir.exists() {
                std::fs::remove_dir_all(local_dir)?;
            }
            std::fs::create_dir_all(local_dir)?;

            let prefix = self.full_key(remote_prefix, Path::new(""));
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
                let resp = req.send().await.map_err(map_s3_error)?;
                if let Some(contents) = resp.contents() {
                    for object in contents {
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
                                .map_err(map_s3_error)?
                                .body
                                .collect()
                                .await
                                .map_err(|e| StorageError::ObjectStore(e.to_string()))?;
                            std::fs::write(&dest_path, body.into_bytes())?;
                        }
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

    fn map_s3_error(err: S3Error) -> StorageError {
        StorageError::ObjectStore(err.to_string())
    }

    impl ObjectStore for S3ObjectStore {
        fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), StorageError> {
            self.run_async(self.upload_dir_async(local_dir, remote_prefix))
        }

        fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), StorageError> {
            self.run_async(self.download_dir_async(remote_prefix, local_dir))
        }
    }
}

#[cfg(feature = "s3")]
pub use s3_store::S3ObjectStore;
