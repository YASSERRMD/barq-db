# Barq DB Helm Chart

A Helm chart for deploying Barq DB - High-Performance Vector Database.

## Prerequisites

- Kubernetes 1.19+
- Helm 3+
- PV provisioner support in the cluster (if using persistence)

## Installation

### Add the Helm Repository

```bash
helm repo add barq https://yasserrmd.github.io/barq-db
helm repo update
```

### Install the Chart

```bash
helm install my-barq barq/barq-db
```

### Install with Custom Values

```bash
helm install my-barq barq/barq-db \
  --set replicaCount=3 \
  --set persistence.size=50Gi \
  --set barq.apiKey=my-secret-key
```

### Install from Local Directory

```bash
helm install my-barq ./deploy/helm/barq-db
```

## Configuration

See `values.yaml` for full configuration options.

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `yasserrmd/barq-db` |
| `image.tag` | Image tag | `latest` |
| `service.httpPort` | HTTP API port | `8080` |
| `service.grpcPort` | gRPC API port | `50051` |
| `persistence.enabled` | Enable persistence | `true` |
| `persistence.size` | Storage size | `10Gi` |
| `barq.apiKey` | API key for auth | `""` |
| `barq.mode` | Cluster mode | `standalone` |
| `tiering.enabled` | Enable storage tiering | `false` |

### Persistence

By default, persistence is enabled with a 10Gi volume. To disable:

```bash
helm install my-barq barq/barq-db --set persistence.enabled=false
```

### Storage Tiering

Enable S3 tiering:

```yaml
tiering:
  enabled: true
  warm:
    enabled: true
    provider: s3
    bucket: my-warm-bucket
    prefix: warm/

cloudCredentials:
  aws:
    accessKeyId: "AKIA..."
    secretAccessKey: "..."
    region: "us-east-1"
```

### Ingress

Enable ingress with TLS:

```yaml
ingress:
  enabled: true
  className: nginx
  hosts:
    - host: barq.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: barq-tls
      hosts:
        - barq.example.com
```

### Autoscaling

Enable HPA:

```yaml
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
```

## Upgrading

```bash
helm upgrade my-barq barq/barq-db
```

## Uninstalling

```bash
helm uninstall my-barq
```

**Note:** This will not delete the PVCs. To delete data:

```bash
kubectl delete pvc -l app.kubernetes.io/name=barq-db
```

## Examples

### Production Setup

```yaml
replicaCount: 3

resources:
  limits:
    cpu: 4000m
    memory: 8Gi
  requests:
    cpu: 1000m
    memory: 2Gi

persistence:
  enabled: true
  size: 100Gi
  storageClassName: fast-ssd

barq:
  existingSecret: barq-api-key
  logLevel: warn
  mode: cluster

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 20

affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app.kubernetes.io/name: barq-db
        topologyKey: kubernetes.io/hostname
```

## License

MIT License - see [LICENSE](../../LICENSE) for details.
