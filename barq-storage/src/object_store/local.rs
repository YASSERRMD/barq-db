//! Local Filesystem Object Store
//!
//! Implements the ObjectStore trait for the local filesystem.
//! Useful for development, testing, and single-node deployments.

use super::traits::{ObjectMetadata, ObjectStore, ObjectStoreError};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Local filesystem-based object store.
///
/// This implementation uses the local filesystem as the storage backend,
/// making it ideal for testing, development, and single-node deployments.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Create a new LocalObjectStore with the given root directory.
    ///
    /// # Arguments
    /// * `root` - The root directory for all object storage operations
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ObjectStoreError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Get the full path for a given prefix/key.
    fn full_path(&self, prefix: &Path) -> PathBuf {
        self.root.join(prefix)
    }

    /// Recursively copy a directory.
    fn copy_dir_recursively(src: &Path, dst: &Path) -> Result<(), ObjectStoreError> {
        fs::create_dir_all(dst)?;
        for entry in WalkDir::new(src) {
            let entry = entry.map_err(|e| ObjectStoreError::Io(e.into()))?;
            let relative = entry
                .path()
                .strip_prefix(src)
                .map_err(|e| ObjectStoreError::InvalidPath(e.to_string()))?;
            let dest_path = dst.join(relative);

            if entry.file_type().is_dir() {
                fs::create_dir_all(&dest_path)?;
            } else {
                if let Some(parent) = dest_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(entry.path(), &dest_path)?;
            }
        }
        Ok(())
    }
}

impl ObjectStore for LocalObjectStore {
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), ObjectStoreError> {
        let dest = self.full_path(remote_prefix);
        if dest.exists() {
            fs::remove_dir_all(&dest)?;
        }
        Self::copy_dir_recursively(local_dir, &dest)?;
        Ok(())
    }

    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), ObjectStoreError> {
        let src = self.full_path(remote_prefix);
        if !src.exists() {
            return Err(ObjectStoreError::NotFound(
                src.to_string_lossy().to_string(),
            ));
        }
        if local_dir.exists() {
            fs::remove_dir_all(local_dir)?;
        }
        Self::copy_dir_recursively(&src, local_dir)?;
        Ok(())
    }

    fn upload_file(&self, local_path: &Path, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let dest = self.full_path(remote_key);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(local_path, &dest)?;
        Ok(())
    }

    fn download_file(&self, remote_key: &Path, local_path: &Path) -> Result<(), ObjectStoreError> {
        let src = self.full_path(remote_key);
        if !src.exists() {
            return Err(ObjectStoreError::NotFound(
                src.to_string_lossy().to_string(),
            ));
        }
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&src, local_path)?;
        Ok(())
    }

    fn delete(&self, remote_key: &Path) -> Result<(), ObjectStoreError> {
        let path = self.full_path(remote_key);
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn exists(&self, remote_key: &Path) -> Result<bool, ObjectStoreError> {
        Ok(self.full_path(remote_key).exists())
    }

    fn get_metadata(&self, remote_key: &Path) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.full_path(remote_key);
        let metadata = fs::metadata(&path)?;
        let last_modified = metadata
            .modified()
            .map(|t| {
                t.duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        Ok(ObjectMetadata {
            size: metadata.len(),
            last_modified,
            content_type: None,
            etag: None,
            custom_metadata: HashMap::new(),
        })
    }

    fn list(&self, prefix: &Path) -> Result<Vec<String>, ObjectStoreError> {
        let path = self.full_path(prefix);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for entry in WalkDir::new(&path).min_depth(1) {
            let entry = entry.map_err(|e| ObjectStoreError::Io(e.into()))?;
            if entry.file_type().is_file() {
                let relative = entry
                    .path()
                    .strip_prefix(&path)
                    .map_err(|e| ObjectStoreError::InvalidPath(e.to_string()))?;
                results.push(relative.to_string_lossy().to_string());
            }
        }
        Ok(results)
    }

    fn copy(&self, src: &Path, dst: &Path) -> Result<(), ObjectStoreError> {
        let src_path = self.full_path(src);
        let dst_path = self.full_path(dst);

        if src_path.is_dir() {
            Self::copy_dir_recursively(&src_path, &dst_path)?;
        } else {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&src_path, &dst_path)?;
        }
        Ok(())
    }

    fn store_type(&self) -> &'static str {
        "local"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_upload_download_file() {
        let temp = TempDir::new().unwrap();
        let store = LocalObjectStore::new(temp.path().join("store")).unwrap();

        // Create a test file
        let src_dir = temp.path().join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let test_file = src_dir.join("test.txt");
        fs::write(&test_file, b"hello world").unwrap();

        // Upload
        store
            .upload_file(&test_file, Path::new("backup/test.txt"))
            .unwrap();

        // Verify exists
        assert!(store.exists(Path::new("backup/test.txt")).unwrap());

        // Download
        let dst_file = temp.path().join("dst/test.txt");
        store
            .download_file(Path::new("backup/test.txt"), &dst_file)
            .unwrap();

        assert_eq!(fs::read_to_string(&dst_file).unwrap(), "hello world");
    }

    #[test]
    fn test_upload_download_dir() {
        let temp = TempDir::new().unwrap();
        let store = LocalObjectStore::new(temp.path().join("store")).unwrap();

        // Create test directory structure
        let src_dir = temp.path().join("src");
        fs::create_dir_all(src_dir.join("subdir")).unwrap();
        fs::write(src_dir.join("file1.txt"), b"content1").unwrap();
        fs::write(src_dir.join("subdir/file2.txt"), b"content2").unwrap();

        // Upload directory
        store
            .upload_dir(&src_dir, Path::new("backup"))
            .unwrap();

        // Download to new location
        let dst_dir = temp.path().join("dst");
        store
            .download_dir(Path::new("backup"), &dst_dir)
            .unwrap();

        assert_eq!(
            fs::read_to_string(dst_dir.join("file1.txt")).unwrap(),
            "content1"
        );
        assert_eq!(
            fs::read_to_string(dst_dir.join("subdir/file2.txt")).unwrap(),
            "content2"
        );
    }

    #[test]
    fn test_list_objects() {
        let temp = TempDir::new().unwrap();
        let store = LocalObjectStore::new(temp.path().join("store")).unwrap();

        // Create some files
        let src = temp.path().join("src");
        fs::create_dir_all(src.join("a/b")).unwrap();
        fs::write(src.join("file1.txt"), b"1").unwrap();
        fs::write(src.join("a/file2.txt"), b"2").unwrap();
        fs::write(src.join("a/b/file3.txt"), b"3").unwrap();

        store.upload_dir(&src, Path::new("data")).unwrap();

        let files = store.list(Path::new("data")).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_delete() {
        let temp = TempDir::new().unwrap();
        let store = LocalObjectStore::new(temp.path().join("store")).unwrap();

        let src = temp.path().join("src/test.txt");
        fs::create_dir_all(src.parent().unwrap()).unwrap();
        fs::write(&src, b"data").unwrap();

        store.upload_file(&src, Path::new("test.txt")).unwrap();
        assert!(store.exists(Path::new("test.txt")).unwrap());

        store.delete(Path::new("test.txt")).unwrap();
        assert!(!store.exists(Path::new("test.txt")).unwrap());
    }
}
