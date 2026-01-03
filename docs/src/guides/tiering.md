# Storage Tiering

Barq DB's storage tiering feature allows you to optimize storage costs by automatically moving older data segments to cheaper object storage services like Amazon S3, Google Cloud Storage (GCS), or Azure Blob Storage.

## How it Works

Barq divides data into "hot", "warm", and "cold" tiers:

1.  **Hot Tier**: Local SSD/NVMe. Used for recent writes, WAL, and frequently accessed indexes.
2.  **Warm Tier**: Object Storage (S3/GCS). Older segments are lazily uploaded here. They are downloaded to local disk when queried (hydrated).
3.  **Cold Tier**: Object Storage (Archive classes). Used for long-term retention. Access requires a restoration process.

## Configuration

Tiering is configured via environment variables or the `BarqDB` CRD in Kubernetes.

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `BARQ_TIERING_ENABLED` | Enable the tiering manager | `true` |
| `BARQ_WARM_TIER_PROVIDER` | Provider for warm tier | `s3`, `gcs`, `azure` |
| `BARQ_WARM_TIER_BUCKET` | Bucket name | `my-bucket` |
| `AWS_ACCESS_KEY_ID` | AWS Credentials | `...` |

### Policies

Currently, Barq supports a **Time-Based Policy**. Segments older than a configured threshold (e.g., 24 hours) are candidates for offloading to the warm tier.

## Hydration

When a query hits a segment that is not present locally but exists in the tiering metadata:
1.  The query engine requests the segment from the `TieringManager`.
2.  The manager downloads the file from the configured object store.
3.  The segment is cached locally for subsequent queries.
4.  If the local cache is full, LRU eviction removes unused segments.

## Disaster Recovery

The tiering state is persisted to `tiering_state.json`. In case of a total node failure, you can restore the dataset by:
1.  Pointing a new Barq instance to the same bucket.
2.  (Future) Running a "metadata reconstruction" tool to rebuild the local state from the object store.
