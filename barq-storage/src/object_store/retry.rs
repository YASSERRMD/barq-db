//! Retry Module
//!
//! Provides retry logic with exponential backoff for object store operations.

use super::traits::ObjectStoreError;
use std::thread;
use std::time::Duration;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay before first retry (in milliseconds)
    pub initial_delay_ms: u64,
    /// Maximum delay between retries (in milliseconds)
    pub max_delay_ms: u64,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Add jitter to prevent thundering herd
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 10_000,
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Create a new retry config with custom max retries.
    pub fn with_max_retries(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Default::default()
        }
    }

    /// Create a config for aggressive retries (more attempts, longer delays).
    pub fn aggressive() -> Self {
        Self {
            max_retries: 5,
            initial_delay_ms: 200,
            max_delay_ms: 30_000,
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }

    /// Create a config for quick retries (fewer attempts, shorter delays).
    pub fn quick() -> Self {
        Self {
            max_retries: 2,
            initial_delay_ms: 50,
            max_delay_ms: 1_000,
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }

    /// Disable retries.
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Calculate delay for a given attempt number.
    fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_delay = self.initial_delay_ms as f64
            * self.backoff_multiplier.powi(attempt as i32);
        let capped_delay = base_delay.min(self.max_delay_ms as f64);
        
        let final_delay = if self.jitter {
            // Add up to 25% jitter
            let jitter_factor = 1.0 + (rand_simple() * 0.25);
            capped_delay * jitter_factor
        } else {
            capped_delay
        };

        Duration::from_millis(final_delay as u64)
    }
}

/// Simple random number generator (0.0 to 1.0) without external dependencies.
fn rand_simple() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}

/// Determines if an error is retryable.
pub fn is_retryable(error: &ObjectStoreError) -> bool {
    match error {
        // Network errors are typically transient
        ObjectStoreError::Network(_) => true,
        // Provider errors may be rate limits or temporary issues
        ObjectStoreError::Provider(msg) => {
            let msg_lower = msg.to_lowercase();
            msg_lower.contains("timeout")
                || msg_lower.contains("rate limit")
                || msg_lower.contains("throttl")
                || msg_lower.contains("temporarily")
                || msg_lower.contains("try again")
                || msg_lower.contains("service unavailable")
                || msg_lower.contains("503")
                || msg_lower.contains("429")
                || msg_lower.contains("500")
                || msg_lower.contains("502")
                || msg_lower.contains("504")
        }
        // IO errors may be transient
        ObjectStoreError::Io(e) => {
            matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
            )
        }
        // Not found and permission denied are not retryable
        ObjectStoreError::NotFound(_) => false,
        ObjectStoreError::PermissionDenied(_) => false,
        ObjectStoreError::Configuration(_) => false,
        ObjectStoreError::Serialization(_) => false,
        ObjectStoreError::InvalidPath(_) => false,
    }
}

/// Execute a function with retry logic.
pub fn with_retry<F, T>(config: &RetryConfig, operation: F) -> Result<T, ObjectStoreError>
where
    F: Fn() -> Result<T, ObjectStoreError>,
{
    let mut last_error: Option<ObjectStoreError> = None;

    for attempt in 0..=config.max_retries {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                // Check if we should retry
                if attempt < config.max_retries && is_retryable(&e) {
                    let delay = config.calculate_delay(attempt);
                    thread::sleep(delay);
                    last_error = Some(e);
                } else {
                    return Err(e);
                }
            }
        }
    }

    // This should only be reached if all retries failed
    Err(last_error.unwrap_or_else(|| {
        ObjectStoreError::Network("All retry attempts exhausted".to_string())
    }))
}

/// A wrapper that adds retry capability to any ObjectStore.
pub struct RetryingObjectStore<S> {
    inner: S,
    config: RetryConfig,
}

impl<S> RetryingObjectStore<S> {
    /// Create a new RetryingObjectStore with default retry config.
    pub fn new(store: S) -> Self {
        Self {
            inner: store,
            config: RetryConfig::default(),
        }
    }

    /// Create with custom retry config.
    pub fn with_config(store: S, config: RetryConfig) -> Self {
        Self {
            inner: store,
            config,
        }
    }

    /// Get a reference to the inner store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get the retry config.
    pub fn config(&self) -> &RetryConfig {
        &self.config
    }
}

use super::traits::{ObjectMetadata, ObjectStore};
use std::path::Path;

impl<S: ObjectStore> ObjectStore for RetryingObjectStore<S> {
    fn upload_dir(&self, local_dir: &Path, remote_prefix: &Path) -> Result<(), ObjectStoreError> {
        with_retry(&self.config, || self.inner.upload_dir(local_dir, remote_prefix))
    }

    fn download_dir(&self, remote_prefix: &Path, local_dir: &Path) -> Result<(), ObjectStoreError> {
        with_retry(&self.config, || self.inner.download_dir(remote_prefix, local_dir))
    }

    fn upload_file(&self, local_path: &Path, remote_key: &Path) -> Result<(), ObjectStoreError> {
        with_retry(&self.config, || self.inner.upload_file(local_path, remote_key))
    }

    fn download_file(&self, remote_key: &Path, local_path: &Path) -> Result<(), ObjectStoreError> {
        with_retry(&self.config, || self.inner.download_file(remote_key, local_path))
    }

    fn delete(&self, remote_key: &Path) -> Result<(), ObjectStoreError> {
        with_retry(&self.config, || self.inner.delete(remote_key))
    }

    fn exists(&self, remote_key: &Path) -> Result<bool, ObjectStoreError> {
        with_retry(&self.config, || self.inner.exists(remote_key))
    }

    fn get_metadata(&self, remote_key: &Path) -> Result<ObjectMetadata, ObjectStoreError> {
        with_retry(&self.config, || self.inner.get_metadata(remote_key))
    }

    fn list(&self, prefix: &Path) -> Result<Vec<String>, ObjectStoreError> {
        with_retry(&self.config, || self.inner.list(prefix))
    }

    fn copy(&self, src: &Path, dst: &Path) -> Result<(), ObjectStoreError> {
        with_retry(&self.config, || self.inner.copy(src, dst))
    }

    fn store_type(&self) -> &'static str {
        self.inner.store_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_retry_succeeds_after_failures() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        
        let config = RetryConfig::with_max_retries(3);
        
        let result = with_retry(&config, || {
            let current = attempts_clone.fetch_add(1, Ordering::SeqCst);
            if current < 2 {
                Err(ObjectStoreError::Network("temporary failure".to_string()))
            } else {
                Ok("success")
            }
        });
        
        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_non_retryable_error_fails_immediately() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        
        let config = RetryConfig::with_max_retries(3);
        
        let result: Result<(), _> = with_retry(&config, || {
            attempts_clone.fetch_add(1, Ordering::SeqCst);
            Err(ObjectStoreError::NotFound("file not found".to_string()))
        });
        
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_retry_config_delay_calculation() {
        let config = RetryConfig {
            initial_delay_ms: 100,
            backoff_multiplier: 2.0,
            max_delay_ms: 1000,
            jitter: false,
            max_retries: 5,
        };
        
        assert_eq!(config.calculate_delay(0).as_millis(), 100);
        assert_eq!(config.calculate_delay(1).as_millis(), 200);
        assert_eq!(config.calculate_delay(2).as_millis(), 400);
        assert_eq!(config.calculate_delay(3).as_millis(), 800);
        // Should be capped at max_delay_ms
        assert_eq!(config.calculate_delay(4).as_millis(), 1000);
    }

    #[test]
    fn test_is_retryable() {
        assert!(is_retryable(&ObjectStoreError::Network("timeout".to_string())));
        assert!(is_retryable(&ObjectStoreError::Provider("rate limit exceeded".to_string())));
        assert!(is_retryable(&ObjectStoreError::Provider("503 Service Unavailable".to_string())));
        
        assert!(!is_retryable(&ObjectStoreError::NotFound("file not found".to_string())));
        assert!(!is_retryable(&ObjectStoreError::PermissionDenied("access denied".to_string())));
        assert!(!is_retryable(&ObjectStoreError::Configuration("bad config".to_string())));
    }
}
