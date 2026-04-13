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

These capabilities support shard routing and basic replication simulation, but
they do not provide consensus semantics.

## Missing Consensus Requirements

The repository does not currently implement:

- Leader election
- Persistent term tracking
- Majority quorum commit
- A durable replicated log
- Log matching and conflict resolution
- Follower catch-up after lag or restart
- Membership changes with consensus safety
- Network partition handling
- Crash recovery for in-flight consensus state
- Stale leader rejection

Without these properties, the system cannot honestly claim Raft or
Raft-equivalent consensus.

## Current Write Semantics

Current writes are routed to the configured primary for a shard. The cluster
crate also exposes an in-memory replication helper, but API write acceptance is
not tied to a quorum commit protocol and does not wait for consensus across
nodes.

The present behavior is best described as static shard routing with optional
replication helpers, not consensus-backed distributed writes.

The current write durability should be reported explicitly as:

- `NodeLocal` for single-node deployments
- `PrimaryOnly` for routed multi-node deployments
- never `ConsensusQuorum` in the current implementation

## Honest Capability Statement

The current cluster model should be described as:

- `SingleNode` when only one node is configured
- `RoutedReplication` when multiple nodes are configured and requests are routed
  to primaries/replicas without consensus
- not `ConsensusBacked`

## Recommended Documentation Language

Use wording such as:

- "sharding and routed replication"
- "static primary/replica placement"
- "log-shipping style replication helpers"

Avoid wording such as:

- "Raft consensus"
- "consensus-backed replication"
- "quorum-committed distributed writes"
