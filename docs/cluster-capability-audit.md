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
but the API/runtime write path is still primarily routed replication unless a
consensus-backed integration is added on top.

## Missing Consensus Requirements

The repository does not currently implement:

- Durable on-disk persistence for Raft term, vote, and log state
- Runtime/API integration that routes client writes through the Raft engine
- Membership changes with consensus safety
- Crash recovery for in-flight consensus state
- Snapshotting/log compaction for the Raft log
- Production network transport between real nodes

Without these properties, the system cannot honestly claim Raft or
Raft-equivalent production deployment semantics across the full database.

## Current Write Semantics

Current API writes are still routed to the configured primary for a shard. The
cluster crate now also exposes a deterministic Raft engine, but API write
acceptance is not yet wired to that quorum commit path.

The present runtime behavior is best described as static shard routing with
replication helpers, plus an in-crate consensus engine available for simulation
and verification.

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
