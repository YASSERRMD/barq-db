use std::collections::HashMap;
use std::fs;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Identifier for a node within the cluster.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Identifier for a shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u32);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: NodeId,
    pub address: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReadPreference {
    Primary,
    Followers,
    Any,
}

impl Default for ReadPreference {
    fn default() -> Self {
        Self::Primary
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub nodes: Vec<NodeConfig>,
    pub shard_count: u32,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    #[serde(default)]
    pub read_preference: ReadPreference,
}

fn default_replication_factor() -> u32 {
    1
}

impl ClusterConfig {
    pub fn single_node() -> Self {
        Self {
            node_id: NodeId::new("local"),
            nodes: vec![NodeConfig {
                id: NodeId::new("local"),
                address: "localhost".into(),
            }],
            shard_count: 1,
            replication_factor: 1,
            read_preference: ReadPreference::Primary,
        }
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ClusterError> {
        let content = fs::read_to_string(path)?;
        serde_json::from_str(&content).map_err(ClusterError::from)
    }

    pub fn from_env_or_default() -> Result<Self, ClusterError> {
        match std::env::var("BARQ_CLUSTER_CONFIG") {
            Ok(path) => Self::from_path(path),
            Err(_) => Ok(Self::single_node()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardPlacement {
    pub shard: ShardId,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
}

#[derive(Clone, Debug)]
pub struct ClusterRouter {
    pub node_id: NodeId,
    pub placements: HashMap<ShardId, ShardPlacement>,
    pub read_preference: ReadPreference,
}

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("config error: {0}")]
    Config(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("cluster has no nodes configured")]
    EmptyCluster,

    #[error("replication factor must be at least 1")]
    InvalidReplication,

    #[error("shard {shard:?} is not hosted on node {node:?}; target node: {target:?}")]
    NotLocal {
        shard: ShardId,
        node: NodeId,
        target: NodeId,
    },
}

impl ClusterRouter {
    pub fn from_config(config: ClusterConfig) -> Result<Self, ClusterError> {
        if config.nodes.is_empty() {
            return Err(ClusterError::EmptyCluster);
        }
        if config.replication_factor == 0 {
            return Err(ClusterError::InvalidReplication);
        }

        let shard_count = config.shard_count.max(1);
        let node_count = config.nodes.len() as u32;
        let replication = config.replication_factor.min(node_count);
        let mut placements = HashMap::new();
        for shard_index in 0..shard_count {
            let primary_index = shard_index % node_count;
            let mut replicas = Vec::new();
            for offset in 1..replication {
                let idx = (shard_index + offset) % node_count;
                replicas.push(config.nodes[idx as usize].id.clone());
            }
            let placement = ShardPlacement {
                shard: ShardId(shard_index),
                primary: config.nodes[primary_index as usize].id.clone(),
                replicas,
            };
            placements.insert(ShardId(shard_index), placement);
        }

        Ok(Self {
            node_id: config.node_id,
            placements,
            read_preference: config.read_preference,
        })
    }

    pub fn shard_for_key(&self, key: &str) -> ShardId {
        let mut hasher = BuildHasherDefault::<ahash::AHasher>::default().build_hasher();
        hasher.write(key.as_bytes());
        ShardId((hasher.finish() % self.placements.len() as u64) as u32)
    }

    pub fn route(&self, key: &str, read_preference: Option<ReadPreference>) -> ShardRouting {
        let shard = self.shard_for_key(key);
        let placement = self
            .placements
            .get(&shard)
            .expect("shard placement should exist");
        let preference = read_preference.unwrap_or_else(|| self.read_preference.clone());
        let target = match preference {
            ReadPreference::Primary => placement.primary.clone(),
            ReadPreference::Followers => placement
                .replicas
                .first()
                .cloned()
                .unwrap_or_else(|| placement.primary.clone()),
            ReadPreference::Any => placement
                .replicas
                .first()
                .cloned()
                .unwrap_or_else(|| placement.primary.clone()),
        };
        let role = if target == placement.primary {
            ReplicaRole::Primary
        } else {
            ReplicaRole::Follower
        };
        ShardRouting {
            shard: placement.shard,
            primary: placement.primary.clone(),
            replicas: placement.replicas.clone(),
            target,
            role,
        }
    }

    pub fn ensure_primary(&self, key: &str) -> Result<(), ClusterError> {
        let routing = self.route(key, Some(ReadPreference::Primary));
        if routing.target == self.node_id {
            Ok(())
        } else {
            Err(ClusterError::NotLocal {
                shard: routing.shard,
                node: self.node_id.clone(),
                target: routing.target,
            })
        }
    }

    pub fn ensure_local(
        &self,
        key: &str,
        read_preference: Option<ReadPreference>,
    ) -> Result<(), ClusterError> {
        let routing = self.route(key, read_preference);
        if routing.primary == self.node_id || routing.replicas.contains(&self.node_id) {
            Ok(())
        } else {
            Err(ClusterError::NotLocal {
                shard: routing.shard,
                node: self.node_id.clone(),
                target: routing.target,
            })
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShardRouting {
    pub shard: ShardId,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
    pub target: NodeId,
    pub role: ReplicaRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaRole {
    Primary,
    Follower,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ClusterConfig {
        ClusterConfig {
            node_id: NodeId::new("node-0"),
            nodes: vec![
                NodeConfig {
                    id: NodeId::new("node-0"),
                    address: "n0".into(),
                },
                NodeConfig {
                    id: NodeId::new("node-1"),
                    address: "n1".into(),
                },
                NodeConfig {
                    id: NodeId::new("node-2"),
                    address: "n2".into(),
                },
            ],
            shard_count: 4,
            replication_factor: 2,
            read_preference: ReadPreference::Primary,
        }
    }

    #[test]
    fn builds_placements_round_robin() {
        let router = ClusterRouter::from_config(test_config()).unwrap();
        assert_eq!(router.placements.len(), 4);
        let shard0 = router.placements.get(&ShardId(0)).unwrap();
        assert_eq!(shard0.primary.0, "node-0");
        assert_eq!(shard0.replicas[0].0, "node-1");

        let shard1 = router.placements.get(&ShardId(1)).unwrap();
        assert_eq!(shard1.primary.0, "node-1");
        assert_eq!(shard1.replicas[0].0, "node-2");
    }

    #[test]
    fn routes_consistently_by_hash() {
        let router = ClusterRouter::from_config(test_config()).unwrap();
        let shard_a = router.shard_for_key("tenant-a");
        let shard_b = router.shard_for_key("tenant-a");
        assert_eq!(shard_a, shard_b);
    }

    #[test]
    fn rejects_remote_primary() {
        let router = ClusterRouter::from_config(test_config()).unwrap();
        let key = "key-on-other";
        let routing = router.route(key, None);
        if routing.primary != router.node_id {
            assert!(router.ensure_primary(key).is_err());
        } else {
            assert!(router.ensure_primary(key).is_ok());
        }
    }
}
