# Kubernetes Operator

The Barq DB Operator simplifies the deployment and management of Barq clusters on Kubernetes. It handles complex tasks like stateful set management, storage provisioning, and tiering configuration.

## Installation

### Prerequisites
- Kubernetes 1.19+
- Helm 3+

### Deploying the Operator

1.  **Add the Helm Repo**
    ```bash
    helm repo add barq https://yasserrmd.github.io/barq-db
    helm repo update
    ```

2.  **Install the Operator Chart**
    ```bash
    helm install barq-operator barq/barq-operator -n barq-system --create-namespace
    ```

## Custom Resources

The Operator introduces the `BarqDB` custom resource definition (CRD).

### Basic Example

```yaml
apiVersion: barq.io/v1alpha1
kind: BarqDB
metadata:
  name: my-cluster
  namespace: default
spec:
  replicas: 3
  image: yasserrmd/barq-db:latest
  storage:
    size: 50Gi
```

## Storage Tiering Configuration

To enable object storage tiering (e.g., to S3), you need to configure the `tiering` section and provide credentials.

### 1. Create Credentials Secret

```bash
kubectl create secret generic aws-creds \
  --from-literal=AWS_ACCESS_KEY_ID=your-key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your-secret \
  --from-literal=AWS_REGION=us-east-1
```

### 2. Configure CRD

```yaml
spec:
  # ... other fields
  tiering:
    enabled: true
    warmStorage:
      provider: s3
      bucket: my-warm-data
      secretRef: aws-creds
    coldStorage:
      provider: s3
      bucket: my-cold-archive
      secretRef: aws-creds
```

The operator will automatically inject the necessary environment variables and mount the required volumes.
