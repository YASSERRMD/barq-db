use std::collections::HashMap;
use std::fs;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

mod raft;

pub use raft::*;

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

/// Honest capability mode for the current cluster deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterMode {
    /// All reads and writes are handled on a single node.
    SingleNode,
    /// Requests are routed across static primaries/replicas without consensus.
    RoutedReplication,
    /// Writes are committed through a real consensus protocol.
    ConsensusBacked,
}

/// Durability level implied by a successful write acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteDurability {
    /// The write is durable only on the local node.
    NodeLocal,
    /// The write is acknowledged after the shard primary applies it locally.
    PrimaryOnly,
    /// The write is acknowledged after a consensus quorum commits it.
    ConsensusQuorum,
}

/// Runtime status that describes the current cluster capability honestly.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    pub mode: ClusterMode,
    pub write_durability: WriteDurability,
    pub shard_count: u32,
    pub node_count: usize,
}

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
    /// Explicit shard placements, used for resharding or manual overrides. When empty, a
    /// round-robin scheme is derived from the configured nodes and replication factor.
    #[serde(default)]
    pub placements: HashMap<ShardId, ShardPlacement>,
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
            placements: HashMap::new(),
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
    node_addresses: HashMap<NodeId, String>,
    consensus: Option<ConsensusRuntime>,
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

    #[error("node {0:?} is not part of the configured cluster")]
    UnknownNode(NodeId),

    #[error("consensus error for shard {shard:?}: {source}")]
    Consensus {
        shard: ShardId,
        #[source]
        source: RaftError,
    },

    #[error("consensus quorum was not reached for shard {shard:?}; acknowledged {acked} of {quorum}")]
    QuorumUnavailable {
        shard: ShardId,
        acked: usize,
        quorum: usize,
    },

    #[error("shard {shard:?} is not hosted on node {node:?}; target node: {target:?}")]
    NotLocal {
        shard: ShardId,
        node: NodeId,
        target: NodeId,
        target_address: Option<String>,
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

        let mut placements = config.placements.clone();
        let node_addresses: HashMap<_, _> = config
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.address.clone()))
            .collect();
        if placements.is_empty() {
            let shard_count = config.shard_count.max(1);
            let node_count = config.nodes.len() as u32;
            let replication = config.replication_factor.min(node_count);
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
        }

        // Validate that every placement references known nodes.
        let node_ids: Vec<_> = config.nodes.iter().map(|n| n.id.clone()).collect();
        let known_nodes: HashMap<_, _> = node_ids.iter().map(|id| (id, ())).collect();
        for placement in placements.values() {
            if !known_nodes.contains_key(&placement.primary) {
                return Err(ClusterError::UnknownNode(placement.primary.clone()));
            }
            for replica in &placement.replicas {
                if !known_nodes.contains_key(replica) {
                    return Err(ClusterError::UnknownNode(replica.clone()));
                }
            }
        }

        let consensus = if config.nodes.len() > 1 && config.replication_factor > 1 {
            Some(ConsensusRuntime::from_placements(&placements)?)
        } else {
            None
        };

        Ok(Self {
            node_id: config.node_id,
            placements,
            read_preference: config.read_preference,
            node_addresses,
            consensus,
        })
    }

    pub fn shard_for_key(&self, key: &str) -> ShardId {
        let mut hasher = BuildHasherDefault::<ahash::AHasher>::default().build_hasher();
        hasher.write(key.as_bytes());
        ShardId((hasher.finish() % self.placements.len() as u64) as u32)
    }

    /// Return the honest capability mode for the configured cluster.
    pub fn mode(&self) -> ClusterMode {
        if self.node_addresses.len() <= 1 {
            ClusterMode::SingleNode
        } else if self.consensus.is_some() {
            ClusterMode::ConsensusBacked
        } else {
            ClusterMode::RoutedReplication
        }
    }

    /// Return the durability implied by a successful write acknowledgement.
    pub fn write_durability(&self) -> WriteDurability {
        match self.mode() {
            ClusterMode::SingleNode => WriteDurability::NodeLocal,
            ClusterMode::RoutedReplication => WriteDurability::PrimaryOnly,
            ClusterMode::ConsensusBacked => WriteDurability::ConsensusQuorum,
        }
    }

    /// Return runtime status describing the currently supported cluster mode.
    pub fn status(&self) -> ClusterStatus {
        ClusterStatus {
            node_id: self.node_id.clone(),
            mode: self.mode(),
            write_durability: self.write_durability(),
            shard_count: self.placements.len() as u32,
            node_count: self.node_addresses.len(),
        }
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
        let target_address = self.node_addresses.get(&target).cloned();
        ShardRouting {
            shard: placement.shard,
            primary: placement.primary.clone(),
            replicas: placement.replicas.clone(),
            target,
            role,
            target_address,
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
                target_address: routing.target_address,
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
                target_address: routing.target_address,
            })
        }
    }

    /// Commit a write through the active runtime write path.
    pub fn commit_write(&self, key: &str, payload: Vec<u8>) -> Result<(), ClusterError> {
        let routing = self.route(key, Some(ReadPreference::Primary));
        if routing.target != self.node_id {
            return Err(ClusterError::NotLocal {
                shard: routing.shard,
                node: self.node_id.clone(),
                target: routing.target,
                target_address: routing.target_address,
            });
        }

        if let Some(consensus) = &self.consensus {
            let outcome = consensus
                .append(routing.shard, &routing.primary, payload)
                .map_err(|source| ClusterError::Consensus {
                    shard: routing.shard,
                    source,
                })?;
            if !outcome.committed {
                return Err(ClusterError::QuorumUnavailable {
                    shard: routing.shard,
                    acked: outcome.acked,
                    quorum: outcome.quorum,
                });
            }
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn isolate_consensus_node(&self, shard: ShardId, node: &NodeId) -> Result<(), ClusterError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(ClusterError::UnknownShard(shard))?;
        consensus
            .isolate(shard, node)
            .map_err(|source| ClusterError::Consensus { shard, source })
    }

    #[cfg(test)]
    pub fn heal_consensus_node(&self, shard: ShardId, node: &NodeId) -> Result<(), ClusterError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(ClusterError::UnknownShard(shard))?;
        consensus
            .heal(shard, node)
            .map_err(|source| ClusterError::Consensus { shard, source })
    }

    #[cfg(test)]
    pub fn consensus_state(
        &self,
        shard: ShardId,
        node: &NodeId,
    ) -> Result<RaftNodeState, ClusterError> {
        let consensus = self
            .consensus
            .as_ref()
            .ok_or(ClusterError::UnknownShard(shard))?;
        consensus
            .state(shard, node)
            .map_err(|source| ClusterError::Consensus { shard, source })
    }
}

#[derive(Clone, Debug)]
struct ConsensusRuntime {
    shards: Arc<Mutex<HashMap<ShardId, RaftCluster>>>,
}

impl ConsensusRuntime {
    fn from_placements(
        placements: &HashMap<ShardId, ShardPlacement>,
    ) -> Result<Self, ClusterError> {
        let mut shards = HashMap::new();
        for placement in placements.values() {
            let mut nodes = Vec::with_capacity(1 + placement.replicas.len());
            nodes.push(placement.primary.clone());
            nodes.extend(placement.replicas.iter().cloned());

            let mut cluster = RaftCluster::new(nodes);
            cluster
                .elect_leader(&placement.primary)
                .map_err(|source| ClusterError::Consensus {
                    shard: placement.shard,
                    source,
                })?;
            shards.insert(placement.shard, cluster);
        }
        Ok(Self {
            shards: Arc::new(Mutex::new(shards)),
        })
    }

    fn append(
        &self,
        shard: ShardId,
        leader: &NodeId,
        payload: Vec<u8>,
    ) -> Result<AppendOutcome, RaftError> {
        let mut shards = self.shards.lock().expect("consensus runtime lock poisoned");
        let cluster = shards
            .get_mut(&shard)
            .ok_or_else(|| RaftError::UnknownNode(leader.clone()))?;
        cluster.append_entry(leader, payload)
    }

    #[cfg(test)]
    fn isolate(&self, shard: ShardId, node: &NodeId) -> Result<(), RaftError> {
        let mut shards = self.shards.lock().expect("consensus runtime lock poisoned");
        let cluster = shards
            .get_mut(&shard)
            .ok_or_else(|| RaftError::UnknownNode(node.clone()))?;
        cluster.isolate(node)
    }

    #[cfg(test)]
    fn heal(&self, shard: ShardId, node: &NodeId) -> Result<(), RaftError> {
        let mut shards = self.shards.lock().expect("consensus runtime lock poisoned");
        let cluster = shards
            .get_mut(&shard)
            .ok_or_else(|| RaftError::UnknownNode(node.clone()))?;
        cluster.heal(node)
    }

    #[cfg(test)]
    fn state(&self, shard: ShardId, node: &NodeId) -> Result<RaftNodeState, RaftError> {
        let shards = self.shards.lock().expect("consensus runtime lock poisoned");
        let cluster = shards
            .get(&shard)
            .ok_or_else(|| RaftError::UnknownNode(node.clone()))?;
        cluster.state(node).cloned()
    }
}

#[derive(Clone, Debug)]
pub struct ShardRouting {
    pub shard: ShardId,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
    pub target: NodeId,
    pub role: ReplicaRole,
    pub target_address: Option<String>,
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
        &mut self,
        shard: ShardId,
        new_primary: NodeId,
        replicas: Vec<NodeId>,
    ) -> Result<HashMap<ShardId, ShardPlacement>, ClusterError> {
        let mut base_router = ClusterRouter::from_config(self.config.clone())?;
        let mut placements = base_router.placements.clone();
        placements.insert(
            shard,
            ShardPlacement {
                shard,
                primary: new_primary,
                replicas,
            },
        );
        base_router.placements = placements.clone();
        self.config.placements = placements.clone();
        Ok(placements)
    }

    /// Recompute placements after membership changes, returning a fresh router.
    pub fn rebalance(&mut self) -> Result<ClusterRouter, ClusterError> {
        let mut new_config = self.config.clone();
        new_config.placements.clear();
        let router = ClusterRouter::from_config(new_config.clone())?;
        self.config.placements = router.placements.clone();
        self.config.shard_count = router.placements.len() as u32;
        Ok(router)
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
            placements: HashMap::new(),
        }
    }

    fn routed_test_config() -> ClusterConfig {
        let mut config = test_config();
        config.replication_factor = 1;
        config
    }

    fn local_primary_key(router: &ClusterRouter) -> (String, ShardId) {
        (0..10_000)
            .map(|i| format!("key-{i}"))
            .find_map(|candidate| {
                let routing = router.route(&candidate, Some(ReadPreference::Primary));
                if routing.primary == router.node_id {
                    Some((candidate, routing.shard))
                } else {
                    None
                }
            })
            .expect("expected a key for a local primary shard")
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

        let different = (0..10).find_map(|i| {
            let shard = router.shard_for_tenant_document(&format!("tenant-{i}"), "doc-1");
            if shard != shard_a {
                Some(shard)
            } else {
                None
            }
        });
        assert!(
            different.is_some(),
            "expected at least one tenant to map to a different shard"
        );
    }

    #[test]
    fn rejects_remote_primary() {
        let router = ClusterRouter::from_config(routed_test_config()).unwrap();
        let key = "key-on-other";
        let routing = router.route(key, None);
        if routing.primary != router.node_id {
            assert!(router.ensure_primary(key).is_err());
        } else {
            assert!(router.ensure_primary(key).is_ok());
        }
    }

    #[test]
    fn exposes_target_address_when_not_local() {
        let config = routed_test_config();
        let router = ClusterRouter::from_config(config.clone()).unwrap();

        let remote_shard = router
            .placements
            .values()
            .find(|p| p.primary != router.node_id)
            .expect("expected at least one remote shard");

        let key = (0..10_000)
            .map(|i| format!("key-{i}"))
            .find(|candidate| router.shard_for_key(candidate) == remote_shard.shard)
            .expect("should find a key that hashes to the shard");

        let result = router.ensure_primary(&key);
        let err = result.expect_err("routing should be remote");
        match err {
            ClusterError::NotLocal {
                target,
                target_address,
                ..
            } => {
                assert_eq!(target, remote_shard.primary);
                let expected_address = config
                    .nodes
                    .iter()
                    .find(|n| n.id == target)
                    .map(|n| n.address.clone())
                    .expect("node has address");
                assert_eq!(target_address, Some(expected_address));
            }
            _ => panic!("unexpected error: {err:?}"),
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
    fn honors_read_preference_and_manual_placements() {
        let mut config = test_config();
        config.replication_factor = 3;
        config.shard_count = 1;
        config.placements.insert(
            ShardId(0),
            ShardPlacement {
                shard: ShardId(0),
                primary: NodeId::new("node-1"),
                replicas: vec![NodeId::new("node-2"), NodeId::new("node-0")],
            },
        );

        let router = ClusterRouter::from_config(config).unwrap();
        let routing_any = router.route("key", Some(ReadPreference::Any));
        assert_eq!(routing_any.target, NodeId::new("node-2"));
        assert_eq!(routing_any.role, ReplicaRole::Follower);

        let routing_followers = router.route("key", Some(ReadPreference::Followers));
        assert_eq!(routing_followers.target, NodeId::new("node-2"));
        assert_eq!(routing_followers.role, ReplicaRole::Follower);
    }

    #[test]
    fn concurrent_replication_preserves_ordering() {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let config = test_config();
        let router = ClusterRouter::from_config(config.clone()).unwrap();
        let placement = router.placement(ShardId(1)).unwrap();
        let manager = Arc::new(Mutex::new(ReplicationManager::new(
            &config
                .nodes
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            config.shard_count,
        )));

        let threads: Vec<_> = (0..5)
            .map(|i| {
                let manager = manager.clone();
                let placement = placement.clone();
                thread::spawn(move || {
                    for j in 0..10 {
                        let payload = format!("payload-{i}-{j}").into_bytes();
                        let mut guard = manager.lock().unwrap();
                        guard.replicate(&placement, payload, 1);
                    }
                })
            })
            .collect();

        for handle in threads {
            handle.join().unwrap();
        }

        let guard = manager.lock().unwrap();
        let log = guard
            .log_for(&placement.primary, placement.shard)
            .expect("log for primary");
        assert_eq!(log.entries().len(), 50);
        assert_eq!(log.committed_index(), 50);
        let mut indices: Vec<_> = log.entries().iter().map(|e| e.index).collect();
        indices.sort();
        assert_eq!(indices, (1..=50).collect::<Vec<_>>());
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

    #[test]
    fn admin_can_move_shard_and_persist_config() {
        let mut admin = ClusterAdmin::new(test_config());
        let updated = admin
            .move_shard(
                ShardId(2),
                NodeId::new("node-1"),
                vec![NodeId::new("node-0")],
            )
            .unwrap();

        assert_eq!(
            updated.get(&ShardId(2)).unwrap().primary,
            NodeId::new("node-1")
        );
        assert_eq!(admin.config.placements.len(), updated.len());
        assert!(admin.config.placements.contains_key(&ShardId(2)));
    }

    #[test]
    fn cluster_mode_serializes_and_deserializes() {
        let encoded = serde_json::to_string(&ClusterMode::RoutedReplication).unwrap();
        assert_eq!(encoded, "\"routed_replication\"");

        let decoded: ClusterMode = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, ClusterMode::RoutedReplication);
    }

    #[test]
    fn cluster_status_reports_honest_mode() {
        let router = ClusterRouter::from_config(test_config()).unwrap();
        let status = router.status();
        assert_eq!(status.mode, ClusterMode::ConsensusBacked);
        assert_eq!(status.write_durability, WriteDurability::ConsensusQuorum);
        assert_eq!(status.node_count, 3);
        assert_eq!(status.shard_count, 4);
    }

    #[test]
    fn single_replica_multi_node_stays_routed_replication() {
        let router = ClusterRouter::from_config(routed_test_config()).unwrap();
        let status = router.status();
        assert_eq!(status.mode, ClusterMode::RoutedReplication);
        assert_eq!(status.write_durability, WriteDurability::PrimaryOnly);
    }

    #[test]
    fn consensus_commit_routes_runtime_writes_through_quorum() {
        let router = ClusterRouter::from_config(test_config()).unwrap();
        let (key, shard) = local_primary_key(&router);

        router.commit_write(&key, b"set:key=1".to_vec()).unwrap();

        let leader = router.consensus_state(shard, &router.node_id).unwrap();
        assert_eq!(leader.commit_index(), 1);
        assert_eq!(leader.entries().len(), 1);
        let follower = router
            .placements
            .get(&shard)
            .unwrap()
            .replicas
            .first()
            .unwrap()
            .clone();
        let follower_state = router.consensus_state(shard, &follower).unwrap();
        assert_eq!(follower_state.commit_index(), 1);
        assert_eq!(follower_state.entries().len(), 1);
    }

    #[test]
    fn consensus_commit_rejects_when_quorum_is_lost() {
        let router = ClusterRouter::from_config(test_config()).unwrap();
        let (key, shard) = local_primary_key(&router);
        let follower = router
            .placements
            .get(&shard)
            .unwrap()
            .replicas
            .first()
            .unwrap()
            .clone();
        router.isolate_consensus_node(shard, &follower).unwrap();

        let err = router
            .commit_write(&key, b"set:key=2".to_vec())
            .expect_err("quorum loss should reject the write");
        assert!(matches!(
            err,
            ClusterError::QuorumUnavailable {
                shard: failed_shard,
                acked: 1,
                quorum: 2
            } if failed_shard == shard
        ));
    }

    #[test]
    fn replication_log_append_advances_indices_in_order() {
        let config = test_config();
        let router = ClusterRouter::from_config(config.clone()).unwrap();
        let placement = router.placement(ShardId(0)).unwrap();
        let mut manager = ReplicationManager::new(
            &config
                .nodes
                .iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>(),
            config.shard_count,
        );

        let first = manager.replicate(&placement, b"first".to_vec(), 7);
        let second = manager.replicate(&placement, b"second".to_vec(), 7);

        assert_eq!(first.index, 1);
        assert_eq!(second.index, 2);
        assert_eq!(second.acked.len(), 2);

        let primary_log = manager
            .log_for(&placement.primary, placement.shard)
            .expect("primary log");
        assert_eq!(primary_log.committed_index(), 2);
        assert_eq!(primary_log.entries().len(), 2);
        assert_eq!(primary_log.entries()[0].payload, b"first".to_vec());
        assert_eq!(primary_log.entries()[1].payload, b"second".to_vec());

        let follower = placement.replicas.first().expect("follower");
        let follower_log = manager
            .log_for(follower, placement.shard)
            .expect("follower log");
        assert_eq!(follower_log.committed_index(), 2);
        assert_eq!(follower_log.entries().len(), 2);
        assert_eq!(follower_log.entries()[0].index, 1);
        assert_eq!(follower_log.entries()[1].index, 2);
    }
}
