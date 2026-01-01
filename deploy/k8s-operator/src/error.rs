use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Kubernetes Client Error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Serialization Error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Missing Object Key: {0}")]
    MissingObjectKey(&'static str),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
