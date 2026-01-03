# Phase 4: Kubernetes Operator & Deployment

## Overview
This phase aims to simplify the deployment and management of Barq DB on Kubernetes. We will develop a Rust-based Kubernetes Operator using `kube-rs` to automate lifecycle management, specifically focusing on the new storage tiering capabilities.

**Branch**: `phase-4-kubernetes-operator`
**Priority**: High
**Dependencies**: `barq-server`, `barq-storage`, `kube-rs` crate

---

## Task 4.1: Operator Skeleton & CRD

### Description
Create a new crate `barq-operator` and define the Custom Resource Definition (CRD) for `BarqCluster`.

### Implementation Details

#### New Crate
- Initialize `barq-operator` in the workspace.
- dependency: `kube`, `k8s-openapi`, `schemars`, `serde`, `tokio`, `tracing`.

#### CRD Definition (`BarqCluster`)
```rust
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(group = "barq.db", version = "v1alpha1", kind = "BarqCluster", namespaced)]
pub struct BarqClusterSpec {
    pub replicas: i32,
    pub image: String,
    pub storage: StorageSpec,
    pub tiering: Option<TieringSpec>,
    pub resources: Option<ResourceRequirements>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct TieringSpec {
    pub enabled: bool,
    pub hot_path: String, // ephemeral-storage or pvc
    pub warm: Option<WarmTierSpec>,
    pub cold: Option<ColdTierSpec>,
    // Env vars mapping
}
```

### Acceptance Criteria
- [ ] `barq-operator` crate compiles.
- [ ] CRD generates correctly.
- [ ] Basic controller loop implemented (watch changes).

---

## Task 4.2: StatefulSet Reconciliation

### Description
Implement the reconciliation logic to create/update the Kubernetes `StatefulSet` and `Service` based on the `BarqCluster` spec.

### Implementation Details
- Map `BarqClusterSpec` to `StatefulSet`.
- Env Var Injection:
    - `BARQ_TIERING_ENABLED`
    - `BARQ_S3_BUCKET` / `BARQ_GCS_BUCKET` (from secrets or spec)
    - `BARQ_ADDR`
- Volume Mounts:
    - PVC for `/data` (Hot/Warm tier).

### Acceptance Criteria
- [ ] Operator creates a StatefulSet when a BarqCluster is applied.
- [ ] Operator updates replicas when spec changes.
- [ ] Correct environment variables are injected for storage tiering.

---

## Task 4.3: Helm Chart

### Description
Package the operator and the CRD into a Helm chart for easy distribution.

### Implementation Details
- `deploy/charts/barq-operator`
- `deploy/charts/barq-db` (Standalone chart for those not using operator, optional, but Operator chart is priority).

### Acceptance Criteria
- [ ] `helm install barq-operator ./deploy/charts/barq-operator` works.
- [ ] Can deploy a BarqCluster using the CRD.

---

## Future Phase
- Auto-scaling based on CPU/Memory/QPS (HPA).
- Backup/Restore CRDs (triggering Snapshot API).
