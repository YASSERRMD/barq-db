use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::NodeId;

/// Role for a node participating in the in-memory Raft simulation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// A single entry replicated through the Raft log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

/// State for a simulated Raft node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RaftNodeState {
    current_term: u64,
    voted_for: Option<NodeId>,
    role: RaftRole,
    log: Vec<RaftLogEntry>,
    commit_index: u64,
}

impl Default for RaftNodeState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            role: RaftRole::Follower,
            log: Vec::new(),
            commit_index: 0,
        }
    }
}

impl RaftNodeState {
    /// The node's current election term.
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// The node's current role.
    pub fn role(&self) -> RaftRole {
        self.role
    }

    /// The highest committed log index on this node.
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// The node's log entries in index order.
    pub fn entries(&self) -> &[RaftLogEntry] {
        &self.log
    }
}

/// Error returned by the deterministic Raft simulation.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RaftError {
    #[error("unknown node {0:?}")]
    UnknownNode(NodeId),

    #[error("node {0:?} is not the current leader")]
    NotLeader(NodeId),

    #[error(
        "node {node:?} is a stale leader for term {term}; observed current term is {current_term}"
    )]
    StaleLeader {
        node: NodeId,
        term: u64,
        current_term: u64,
    },
}

/// Result of running a deterministic election round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectionOutcome {
    pub candidate: NodeId,
    pub term: u64,
    pub votes: usize,
    pub quorum: usize,
    pub elected: bool,
}

/// Result of appending an entry through the simulated Raft cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendOutcome {
    pub index: u64,
    pub term: u64,
    pub acked: usize,
    pub quorum: usize,
    pub committed: bool,
}

/// Deterministic in-memory Raft cluster used for consensus simulation and tests.
#[derive(Clone, Debug)]
pub struct RaftCluster {
    nodes: HashMap<NodeId, RaftNodeState>,
    isolated: HashSet<NodeId>,
    quorum: usize,
}

impl RaftCluster {
    /// Create a new cluster with the provided node ids.
    pub fn new(nodes: Vec<NodeId>) -> Self {
        assert!(!nodes.is_empty(), "raft cluster requires at least one node");
        let quorum = (nodes.len() / 2) + 1;
        let nodes = nodes
            .into_iter()
            .map(|id| (id, RaftNodeState::default()))
            .collect();
        Self {
            nodes,
            isolated: HashSet::new(),
            quorum,
        }
    }

    /// Return the configured quorum size.
    pub fn quorum(&self) -> usize {
        self.quorum
    }

    /// Access the current state for a node.
    pub fn state(&self, node: &NodeId) -> Result<&RaftNodeState, RaftError> {
        self.nodes
            .get(node)
            .ok_or_else(|| RaftError::UnknownNode(node.clone()))
    }

    /// Isolate a node from the rest of the cluster.
    pub fn isolate(&mut self, node: &NodeId) -> Result<(), RaftError> {
        self.ensure_node(node)?;
        self.isolated.insert(node.clone());
        Ok(())
    }

    /// Restore connectivity for a previously isolated node.
    pub fn heal(&mut self, node: &NodeId) -> Result<(), RaftError> {
        self.ensure_node(node)?;
        self.isolated.remove(node);
        Ok(())
    }

    /// Start an election from the given candidate and return the result.
    pub fn elect_leader(&mut self, candidate: &NodeId) -> Result<ElectionOutcome, RaftError> {
        self.ensure_node(candidate)?;

        {
            let state = self.node_mut(candidate)?;
            state.current_term += 1;
            state.role = RaftRole::Candidate;
            state.voted_for = Some(candidate.clone());
        }

        let term = self.state(candidate)?.current_term;
        let candidate_last = self.last_log_position(candidate)?;
        let mut votes = 1;
        let voters = self.other_nodes(candidate);

        for voter_id in voters {
            if !self.can_communicate(candidate, &voter_id) {
                continue;
            }

            let voter_term = self.state(&voter_id)?.current_term;
            if voter_term > term {
                let candidate_state = self.node_mut(candidate)?;
                candidate_state.current_term = voter_term;
                candidate_state.role = RaftRole::Follower;
                candidate_state.voted_for = None;
                return Ok(ElectionOutcome {
                    candidate: candidate.clone(),
                    term: voter_term,
                    votes,
                    quorum: self.quorum,
                    elected: false,
                });
            }

            let voter_last = self.last_log_position(&voter_id)?;
            let up_to_date = candidate_last.1 > voter_last.1
                || (candidate_last.1 == voter_last.1 && candidate_last.0 >= voter_last.0);
            let grant = {
                let voter = self.state(&voter_id)?;
                up_to_date
                    && (voter.current_term < term
                        || voter.voted_for.is_none()
                        || voter.voted_for.as_ref() == Some(candidate))
            };

            if grant {
                let voter = self.node_mut(&voter_id)?;
                voter.current_term = term;
                voter.role = RaftRole::Follower;
                voter.voted_for = Some(candidate.clone());
                votes += 1;
            }
        }

        let elected = votes >= self.quorum;
        if elected {
            let candidate_state = self.node_mut(candidate)?;
            candidate_state.role = RaftRole::Leader;
            candidate_state.voted_for = None;
        }

        Ok(ElectionOutcome {
            candidate: candidate.clone(),
            term,
            votes,
            quorum: self.quorum,
            elected,
        })
    }

    /// Append an entry through the current leader, replicating to reachable followers.
    pub fn append_entry(
        &mut self,
        leader: &NodeId,
        payload: Vec<u8>,
    ) -> Result<AppendOutcome, RaftError> {
        self.ensure_node(leader)?;
        if self.state(leader)?.role != RaftRole::Leader {
            return Err(RaftError::NotLeader(leader.clone()));
        }

        let term = self.state(leader)?.current_term;
        let index = self.state(leader)?.entries().len() as u64 + 1;
        {
            let state = self.node_mut(leader)?;
            state.log.push(RaftLogEntry {
                index,
                term,
                payload,
            });
        }

        let followers = self.other_nodes(leader);
        let mut acked = 1;
        for follower_id in followers {
            if !self.can_communicate(leader, &follower_id) {
                continue;
            }

            let follower_term = self.state(&follower_id)?.current_term;
            if follower_term > term {
                let state = self.node_mut(leader)?;
                state.current_term = follower_term;
                state.role = RaftRole::Follower;
                state.voted_for = None;
                return Err(RaftError::StaleLeader {
                    node: leader.clone(),
                    term,
                    current_term: follower_term,
                });
            }

            {
                let follower = self.node_mut(&follower_id)?;
                if follower.current_term < term {
                    follower.current_term = term;
                    follower.role = RaftRole::Follower;
                    follower.voted_for = None;
                }
            }

            self.replicate_to_follower(leader, &follower_id)?;
            acked += 1;
        }

        let committed = acked >= self.quorum;
        if committed {
            self.node_mut(leader)?.commit_index = index;
            let reachable = self.other_nodes(leader);
            for follower_id in reachable {
                if self.can_communicate(leader, &follower_id) {
                    let follower = self.node_mut(&follower_id)?;
                    follower.commit_index = index.min(follower.log.len() as u64);
                }
            }
        }

        Ok(AppendOutcome {
            index,
            term,
            acked,
            quorum: self.quorum,
            committed,
        })
    }

    /// Synchronize a leader's committed log to every reachable follower.
    pub fn synchronize(&mut self, leader: &NodeId) -> Result<(), RaftError> {
        self.ensure_node(leader)?;
        if self.state(leader)?.role != RaftRole::Leader {
            return Err(RaftError::NotLeader(leader.clone()));
        }

        let term = self.state(leader)?.current_term;
        for follower_id in self.other_nodes(leader) {
            if !self.can_communicate(leader, &follower_id) {
                continue;
            }

            let follower_term = self.state(&follower_id)?.current_term;
            if follower_term > term {
                let state = self.node_mut(leader)?;
                state.current_term = follower_term;
                state.role = RaftRole::Follower;
                state.voted_for = None;
                return Err(RaftError::StaleLeader {
                    node: leader.clone(),
                    term,
                    current_term: follower_term,
                });
            }

            self.replicate_to_follower(leader, &follower_id)?;
        }

        Ok(())
    }

    fn replicate_to_follower(
        &mut self,
        leader: &NodeId,
        follower: &NodeId,
    ) -> Result<(), RaftError> {
        let (leader_log, leader_term, leader_commit) = {
            let leader_state = self.state(leader)?;
            (
                leader_state.log.clone(),
                leader_state.current_term,
                leader_state.commit_index,
            )
        };

        let follower_state = self.node_mut(follower)?;
        follower_state.current_term = follower_state.current_term.max(leader_term);
        follower_state.role = RaftRole::Follower;
        follower_state.voted_for = None;

        let mut mismatch = follower_state.log.len();
        for index in 0..follower_state.log.len().min(leader_log.len()) {
            if follower_state.log[index].term != leader_log[index].term
                || follower_state.log[index].payload != leader_log[index].payload
            {
                mismatch = index;
                break;
            }
        }

        if mismatch < follower_state.log.len() {
            follower_state.log.truncate(mismatch);
        }

        if follower_state.log.len() < leader_log.len() {
            follower_state
                .log
                .extend(leader_log[follower_state.log.len()..].iter().cloned());
        }

        follower_state.commit_index = leader_commit.min(follower_state.log.len() as u64);
        Ok(())
    }

    fn other_nodes(&self, current: &NodeId) -> Vec<NodeId> {
        self.nodes
            .keys()
            .filter(|node| *node != current)
            .cloned()
            .collect()
    }

    fn can_communicate(&self, left: &NodeId, right: &NodeId) -> bool {
        self.nodes.contains_key(left)
            && self.nodes.contains_key(right)
            && !self.isolated.contains(left)
            && !self.isolated.contains(right)
    }

    fn last_log_position(&self, node: &NodeId) -> Result<(u64, u64), RaftError> {
        let state = self.state(node)?;
        Ok(state
            .log
            .last()
            .map(|entry| (entry.term, entry.index))
            .unwrap_or((0, 0)))
    }

    fn ensure_node(&self, node: &NodeId) -> Result<(), RaftError> {
        if self.nodes.contains_key(node) {
            Ok(())
        } else {
            Err(RaftError::UnknownNode(node.clone()))
        }
    }

    fn node_mut(&mut self, node: &NodeId) -> Result<&mut RaftNodeState, RaftError> {
        self.nodes
            .get_mut(node)
            .ok_or_else(|| RaftError::UnknownNode(node.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_nodes() -> Vec<NodeId> {
        vec![
            NodeId::new("node-0"),
            NodeId::new("node-1"),
            NodeId::new("node-2"),
        ]
    }

    #[test]
    fn elects_leader_by_quorum_and_advances_term() {
        let nodes = test_nodes();
        let leader = nodes[0].clone();
        let mut cluster = RaftCluster::new(nodes.clone());

        let election = cluster.elect_leader(&leader).unwrap();

        assert!(election.elected);
        assert_eq!(election.term, 1);
        assert_eq!(election.votes, 3);
        assert_eq!(cluster.state(&leader).unwrap().role(), RaftRole::Leader);
        assert_eq!(cluster.state(&nodes[1]).unwrap().current_term(), 1);
        assert_eq!(cluster.state(&nodes[2]).unwrap().current_term(), 1);
    }

    #[test]
    fn quorum_commit_replication_applies_to_majority() {
        let nodes = test_nodes();
        let leader = nodes[0].clone();
        let mut cluster = RaftCluster::new(nodes.clone());
        assert!(cluster.elect_leader(&leader).unwrap().elected);

        let append = cluster.append_entry(&leader, b"set:key=1".to_vec()).unwrap();

        assert!(append.committed);
        assert_eq!(append.acked, 3);
        assert_eq!(cluster.state(&leader).unwrap().commit_index(), 1);
        assert_eq!(cluster.state(&nodes[1]).unwrap().commit_index(), 1);
        assert_eq!(cluster.state(&nodes[2]).unwrap().commit_index(), 1);
        assert_eq!(cluster.state(&nodes[1]).unwrap().entries().len(), 1);
    }

    #[test]
    fn quorum_loss_leaves_entries_uncommitted() {
        let nodes = test_nodes();
        let leader = nodes[0].clone();
        let mut cluster = RaftCluster::new(nodes.clone());
        assert!(cluster.elect_leader(&leader).unwrap().elected);

        cluster.isolate(&nodes[1]).unwrap();
        cluster.isolate(&nodes[2]).unwrap();
        let append = cluster.append_entry(&leader, b"set:key=2".to_vec()).unwrap();

        assert!(!append.committed);
        assert_eq!(append.acked, 1);
        assert_eq!(cluster.state(&leader).unwrap().commit_index(), 0);
        assert_eq!(cluster.state(&leader).unwrap().entries().len(), 1);
    }

    #[test]
    fn stale_leader_is_rejected_after_new_term() {
        let nodes = test_nodes();
        let first_leader = nodes[0].clone();
        let second_leader = nodes[1].clone();
        let mut cluster = RaftCluster::new(nodes.clone());

        assert!(cluster.elect_leader(&first_leader).unwrap().elected);
        cluster.isolate(&first_leader).unwrap();

        let second_election = cluster.elect_leader(&second_leader).unwrap();
        assert!(second_election.elected);
        assert_eq!(second_election.term, 2);

        cluster.heal(&first_leader).unwrap();
        let err = cluster
            .append_entry(&first_leader, b"stale-write".to_vec())
            .unwrap_err();

        assert_eq!(
            err,
            RaftError::StaleLeader {
                node: first_leader.clone(),
                term: 1,
                current_term: 2,
            }
        );
        assert_eq!(cluster.state(&first_leader).unwrap().role(), RaftRole::Follower);
        assert_eq!(cluster.state(&first_leader).unwrap().current_term(), 2);
    }

    #[test]
    fn follower_catches_up_after_partition_heal_without_duplicate_entries() {
        let nodes = test_nodes();
        let leader = nodes[0].clone();
        let lagging = nodes[2].clone();
        let mut cluster = RaftCluster::new(nodes.clone());

        assert!(cluster.elect_leader(&leader).unwrap().elected);
        cluster.isolate(&lagging).unwrap();

        let first = cluster.append_entry(&leader, b"first".to_vec()).unwrap();
        let second = cluster.append_entry(&leader, b"second".to_vec()).unwrap();
        assert!(first.committed);
        assert!(second.committed);
        assert_eq!(cluster.state(&lagging).unwrap().entries().len(), 0);

        cluster.heal(&lagging).unwrap();
        cluster.synchronize(&leader).unwrap();
        cluster.synchronize(&leader).unwrap();

        let lagging_state = cluster.state(&lagging).unwrap();
        assert_eq!(lagging_state.commit_index(), 2);
        assert_eq!(lagging_state.entries().len(), 2);
        assert_eq!(lagging_state.entries()[0].payload, b"first".to_vec());
        assert_eq!(lagging_state.entries()[1].payload, b"second".to_vec());
    }
}
