use crate::StorageError;
use std::fs;
use std::path::{Path, PathBuf};

/// Abstraction over snapshot/segment upload targets.
pub trait ObjectStore: Send + Sync {
    /// Upload the contents of a local directory into a destination prefix.
    /// Existing data at the destination may be replaced.
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), StorageError>;
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
}
