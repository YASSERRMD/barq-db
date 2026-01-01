use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(group = "barq.io", version = "v1alpha1", kind = "BarqDB", namespaced)]
#[kube(status = "BarqDBStatus")]
#[kube(shortname = "bdb")]
// #[kube(printcolumn(name = "Replicas", jsonpath = ".spec.replicas"))]
// #[kube(printcolumn(name = "Ready", jsonpath = ".status.readyReplicas"))]
// #[kube(printcolumn(name = "Phase", jsonpath = ".status.phase"))]
// #[kube(printcolumn(name = "Age", jsonpath = ".metadata.creationTimestamp"))]
pub struct BarqDBSpec {
    #[serde(default = "default_replicas")]
    pub replicas: i32,
    #[serde(default = "default_image")]
    pub image: String,
    #[serde(default)]
    pub resources: Resources,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub config: BarqConfig,
    #[serde(default)]
    pub tiering: TieringConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct Resources {
    #[serde(default)]
    pub requests: ResourceList,
    #[serde(default)]
    pub limits: ResourceList,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ResourceList {
    #[serde(default = "default_cpu")]
    pub cpu: String,
    #[serde(default = "default_memory")]
    pub memory: String,
}

impl Default for ResourceList {
    fn default() -> Self {
        Self {
            cpu: default_cpu(),
            memory: default_memory(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct StorageConfig {
    #[serde(default = "default_storage_size")]
    pub size: String,
    #[serde(rename = "storageClassName")]
    pub storage_class_name: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            size: default_storage_size(),
            storage_class_name: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct BarqConfig {
    #[serde(default = "default_log_level", rename = "logLevel")]
    pub log_level: String,
    #[serde(default = "default_index_type", rename = "indexType")]
    pub index_type: String,
    #[serde(default = "default_mode")]
    pub mode: String,
}

impl Default for BarqConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            index_type: default_index_type(),
            mode: default_mode(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct TieringConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(rename = "warmStorage")]
    pub warm_storage: Option<TierStorageConfig>,
    #[serde(rename = "coldStorage")]
    pub cold_storage: Option<TierStorageConfig>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TierStorageConfig {
    pub provider: String,
    pub bucket: String,
    #[serde(rename = "secretRef")]
    pub secret_ref: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct BarqDBStatus {
    pub phase: Option<String>,
    pub replicas: i32,
    #[serde(rename = "readyReplicas")]
    pub ready_replicas: i32,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    pub endpoints: Option<Endpoints>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct Condition {
    pub type_: String,
    pub status: String,
    #[serde(rename = "lastTransitionTime")]
    pub last_transition_time: String,
    pub reason: String,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct Endpoints {
    pub http: String,
    pub grpc: String,
}

fn default_replicas() -> i32 { 1 }
fn default_image() -> String { "yasserrmd/barq-db:latest".to_string() }
fn default_cpu() -> String { "500m".to_string() }
fn default_memory() -> String { "1Gi".to_string() }
fn default_storage_size() -> String { "10Gi".to_string() }
fn default_log_level() -> String { "info".to_string() }
fn default_index_type() -> String { "HNSW".to_string() }
fn default_mode() -> String { "standalone".to_string() }
