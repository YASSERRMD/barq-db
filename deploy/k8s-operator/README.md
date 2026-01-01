# Barq DB Kubernetes Operator

A Kubernetes operator for managing Barq DB clusters.

## Overview

The Barq Operator automates the deployment and management of Barq DB clusters on Kubernetes. It provides:

- **Automated Deployment**: Deploy Barq DB clusters with a simple Custom Resource
- **Scaling**: Automatic horizontal scaling based on load
- **Storage Tiering**: Configure hot/warm/cold storage tiers with cloud backends
- **Self-Healing**: Automatic recovery from failures
- **Rolling Updates**: Zero-downtime upgrades

## Installation

### Prerequisites

- Kubernetes 1.19+
- kubectl configured to access your cluster
- Helm 3+ (optional, for Helm installation)

### Install the CRD

```bash
kubectl apply -f config/crd/bases/barq.io_barqdbs.yaml
```

### Install the Operator

```bash
kubectl apply -f config/rbac/role.yaml
kubectl apply -f config/manager/manager.yaml
```

## Usage

### Create a Barq DB Cluster

Create a `BarqDB` custom resource:

```yaml
apiVersion: barq.io/v1alpha1
kind: BarqDB
metadata:
  name: my-barq-cluster
  namespace: default
spec:
  replicas: 3
  image: yasserrmd/barq-db:latest
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2000m"
      memory: "4Gi"
  storage:
    size: "20Gi"
    storageClassName: "standard"
  config:
    logLevel: "info"
    indexType: "HNSW"
    mode: "cluster"
  tiering:
    enabled: true
    warmStorage:
      provider: "s3"
      bucket: "my-barq-warm-storage"
      secretRef: "aws-credentials"
```

Apply it:

```bash
kubectl apply -f my-barq-cluster.yaml
```

### Check Status

```bash
kubectl get barqdbs
kubectl describe barqdb my-barq-cluster
```

### Scale the Cluster

```bash
kubectl scale barqdb my-barq-cluster --replicas=5
```

### Delete the Cluster

```bash
kubectl delete barqdb my-barq-cluster
```

## Configuration

### Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `replicas` | integer | Yes | Number of Barq DB replicas (1-100) |
| `image` | string | No | Docker image (default: yasserrmd/barq-db:latest) |
| `resources` | object | No | CPU/memory requests and limits |
| `storage` | object | No | Persistent storage configuration |
| `config` | object | No | Barq DB configuration options |
| `tiering` | object | No | Storage tiering configuration |

### Storage Tiering

Configure object storage backends for warm and cold tiers:

```yaml
tiering:
  enabled: true
  warmStorage:
    provider: "s3"  # s3, gcs, or azure
    bucket: "warm-bucket"
    secretRef: "cloud-credentials"
  coldStorage:
    provider: "gcs"
    bucket: "cold-bucket"
    secretRef: "gcs-credentials"
```

## Development

### Building the Operator
 
```bash
cargo build --release -p barq-operator
```

### Building Docker Image

From the project root:

```bash
docker build -t yasserrmd/barq-operator:latest -f deploy/k8s-operator/Dockerfile .
```

### Running Locally

```bash
cargo run --bin barq-operator -- --kubeconfig ~/.kube/config
```

## License

MIT License - see [LICENSE](../../LICENSE) for details.
