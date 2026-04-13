# Cluster Capability Audit

Date: 2026-04-13

This document audits the cluster-related code in Barq DB and compares the
current implementation to the requirements for real consensus-backed writes.

## Current Capabilities

The repository currently implements:

- Static cluster configuration via `ClusterConfig`
- Deterministic shard placement and request routing via `ClusterRouter`
- Primary/follower targeting for reads and primary enforcement for writes
- Administrative shard placement updates and rebalancing helpers
- An in-memory `ReplicationLog` and `ReplicationManager` for log-shipping style
  replication tests
- A deterministic in-memory `RaftCluster` engine with leader election, term
  transitions, quorum commit, stale-leader rejection, follower catch-up, and
  partition/heal simulation coverage

These capabilities provide concrete consensus behavior inside `barq-cluster`,
and replicated runtime write paths now use those quorum commits before
acknowledging a successful write.

## Missing Consensus Requirements

The repository does not currently implement:

- Durable on-disk persistence for Raft term, vote, and log state
- Membership changes with consensus safety
- Crash recovery for in-flight consensus state
- Snapshotting/log compaction for the Raft log
- Production network transport between real nodes

Without these properties, the system cannot honestly claim Raft or
Raft-equivalent production deployment semantics across the full database.

## Current Write Semantics

Replicated runtime API writes are routed to the shard primary and then committed
through the per-shard in-memory Raft engine before the server acknowledges
success. Multi-node deployments with `replication_factor = 1` still use routed
replication without quorum durability.

The current write durability should be reported explicitly as:

- `NodeLocal` for single-node deployments
- `PrimaryOnly` for single-replica multi-node deployments
- `ConsensusQuorum` for replicated multi-node deployments

## Honest Capability Statement

The current cluster model should be described as:

- `SingleNode` when only one node is configured
- `RoutedReplication` when multiple nodes are configured with
  `replication_factor = 1`
- `ConsensusBacked` when multiple nodes are configured with
  `replication_factor > 1`

## Recommended Documentation Language

Use wording such as:

- "sharding and routed replication"
- "static primary/replica placement"
- "per-shard quorum-committed writes"
- "log-shipping style replication helpers"

Avoid wording such as:

- "fully durable Raft consensus"
- "production-hardened distributed consensus"
- "quorum-committed distributed writes across real networked nodes"
