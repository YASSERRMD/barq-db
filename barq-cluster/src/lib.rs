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

    /// Persist the configuration to a file, allowing static membership via config files.
    pub fn to_path(&self, path: impl AsRef<Path>) -> Result<(), ClusterError> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
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

/// Representation of a shard belonging to a logical collection/tenant.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shard {
    pub id: ShardId,
    pub collection: String,
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

    #[error("shard {0:?} is not known in the cluster")]
    UnknownShard(ShardId),

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

    /// Determine a shard using a tenant/document composite key, ensuring multi-tenant
    /// collections always shard consistently for the same tenant.
    pub fn shard_for_tenant_document(&self, tenant: &str, document_id: &str) -> ShardId {
        let composite = format!("{}:{}", tenant, document_id);
        self.shard_for_key(&composite)
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

    /// Return the placement for a shard id, validating existence.
    pub fn placement(&self, shard: ShardId) -> Result<ShardPlacement, ClusterError> {
        self.placements
            .get(&shard)
            .cloned()
            .ok_or(ClusterError::UnknownShard(shard))
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

/// In-memory replication log entry used for log-shipping style replication.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationEntry {
    pub shard: ShardId,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

/// Replication log state for a single shard on a node.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationLog {
    entries: Vec<ReplicationEntry>,
    committed: u64,
}

impl ReplicationLog {
    pub fn append(&mut self, entry: ReplicationEntry) {
        self.entries.push(entry);
    }

    pub fn commit_up_to(&mut self, index: u64) {
        self.committed = self.committed.max(index);
    }

    pub fn committed_index(&self) -> u64 {
        self.committed
    }

    pub fn entries(&self) -> &[ReplicationEntry] {
        &self.entries
    }
}

/// High level replication helper that ships log entries from primaries to followers.
#[derive(Clone, Debug, Default)]
pub struct ReplicationManager {
    logs: HashMap<NodeId, HashMap<ShardId, ReplicationLog>>,
}

impl ReplicationManager {
    pub fn new(nodes: &[NodeId], shard_count: u32) -> Self {
        let mut logs = HashMap::new();
        for node in nodes {
            let mut shard_logs = HashMap::new();
            for shard in 0..shard_count {
                shard_logs.insert(ShardId(shard), ReplicationLog::default());
            }
            logs.insert(node.clone(), shard_logs);
        }
        Self { logs }
    }

    /// Ship a payload to the primary and all replicas for the shard placement.
    pub fn replicate(
        &mut self,
        placement: &ShardPlacement,
        payload: Vec<u8>,
        term: u64,
    ) -> ReplicationResult {
        let mut acked = Vec::new();
        let mut index = 0;
        let mut ship =
            |node: &NodeId,
             role: ReplicaRole,
             logs: &mut HashMap<NodeId, HashMap<ShardId, ReplicationLog>>| {
                if let Some(shard_logs) = logs.get_mut(node) {
                    if let Some(log) = shard_logs.get_mut(&placement.shard) {
                        index = (log.entries.len() as u64) + 1;
                        log.append(ReplicationEntry {
                            shard: placement.shard,
                            index,
                            term,
                            payload: payload.clone(),
                        });
                        log.commit_up_to(index);
                        acked.push((node.clone(), role));
                    }
                }
            };

        ship(&placement.primary, ReplicaRole::Primary, &mut self.logs);
        for follower in &placement.replicas {
            ship(follower, ReplicaRole::Follower, &mut self.logs);
        }

        ReplicationResult { index, acked }
    }

    pub fn log_for(&self, node: &NodeId, shard: ShardId) -> Option<&ReplicationLog> {
        self.logs.get(node).and_then(|shards| shards.get(&shard))
    }
}

/// Result describing how replication was applied across nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationResult {
    pub index: u64,
    pub acked: Vec<(NodeId, ReplicaRole)>,
}

/// Administrative helper for re-sharding and membership changes.
#[derive(Clone, Debug)]
pub struct ClusterAdmin {
    pub config: ClusterConfig,
}

impl ClusterAdmin {
    pub fn new(config: ClusterConfig) -> Self {
        Self { config }
    }

    pub fn add_node(&mut self, node: NodeConfig) {
        self.config.nodes.push(node);
    }

    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.config.nodes.retain(|n| &n.id != node_id);
    }

    /// Move a shard to a new primary and replicas, returning an updated placement map.
    pub fn move_shard(
        &self,
        shard: ShardId,
        new_primary: NodeId,
        replicas: Vec<NodeId>,
    ) -> Result<HashMap<ShardId, ShardPlacement>, ClusterError> {
        let mut router = ClusterRouter::from_config(self.config.clone())?;
        let mut placements = router.placements.clone();
        placements.insert(
            shard,
            ShardPlacement {
                shard,
                primary: new_primary,
                replicas,
            },
        );
        router.placements = placements.clone();
        Ok(placements)
    }

    /// Recompute placements after membership changes, returning a fresh router.
    pub fn rebalance(&self) -> Result<ClusterRouter, ClusterError> {
        ClusterRouter::from_config(self.config.clone())
    }
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
    fn shards_by_tenant_and_document() {
        let router = ClusterRouter::from_config(test_config()).unwrap();
        let shard_a = router.shard_for_tenant_document("tenant-a", "doc-1");
        let shard_b = router.shard_for_tenant_document("tenant-a", "doc-1");
        assert_eq!(shard_a, shard_b);

        let shard_c = router.shard_for_tenant_document("tenant-b", "doc-1");
        assert_ne!(shard_a, shard_c);
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

    #[test]
    fn replicates_entries_to_followers() {
        let config = test_config();
        let router = ClusterRouter::from_config(config.clone()).unwrap();
        let placement = router.placement(ShardId(0)).unwrap();
        let mut manager = ReplicationManager::new(
            &config
                .nodes
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            config.shard_count,
        );

        let result = manager.replicate(&placement, b"payload".to_vec(), 1);
        assert_eq!(result.index, 1);
        assert_eq!(result.acked.len(), 2);

        let primary_log = manager
            .log_for(&placement.primary, placement.shard)
            .unwrap();
        assert_eq!(primary_log.committed_index(), 1);
        assert_eq!(primary_log.entries().len(), 1);

        let follower = placement.replicas.first().unwrap();
        let follower_log = manager.log_for(follower, placement.shard).unwrap();
        assert_eq!(follower_log.entries().len(), 1);
        assert_eq!(follower_log.entries()[0].payload, b"payload".to_vec());
    }

    #[test]
    fn config_round_trip_to_disk() {
        let cfg = test_config();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.json");
        cfg.to_path(&path).unwrap();
        let loaded = ClusterConfig::from_path(&path).unwrap();
        assert_eq!(loaded.nodes.len(), cfg.nodes.len());
        assert_eq!(loaded.shard_count, cfg.shard_count);
    }

    #[test]
    fn admin_rebalances_after_membership_change() {
        let mut admin = ClusterAdmin::new(test_config());
        admin.add_node(NodeConfig {
            id: NodeId::new("node-3"),
            address: "n3".into(),
        });
        let router = admin.rebalance().unwrap();
        assert_eq!(router.placements.len(), 4);
        // With four nodes, at least one shard should place node-3 as a primary or replica.
        let has_new_node = router.placements.values().any(|placement| {
            placement.primary.0 == "node-3" || placement.replicas.iter().any(|n| n.0 == "node-3")
        });
        assert!(has_new_node);
    }
}
