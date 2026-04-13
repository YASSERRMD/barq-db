# SDKs

Barq v2 SDKs are gRPC-first. The canonical contract is defined in `proto/barq.proto`.

## Current gRPC Client Types

| Language | Client |
|----------|--------|
| Python | `GrpcClient` |
| TypeScript / Node.js | `GrpcClient` |
| Go | `GrpcClient` |
| Rust | `BarqGrpcClient` |

The HTTP clients remain available for compatibility with the current HTTP surface, but new contract work should start from the gRPC definitions in `proto/barq.proto`.

Performance benchmark docs for Barq v2 live in [Performance Benchmarks](performance.md).

## Observability Methods

The current SDK observability surface is aligned to the canonical gRPC contract:

- Python: `get_metrics()`, `get_cluster_status()`, `get_segment_info()`
- TypeScript: `getMetrics()`, `getClusterStatus()`, `getSegmentInfo()`
- Go: `GetMetrics`, `GetClusterStatus`, `GetSegmentInfo`
- Rust: `get_metrics`, `get_cluster_status`, `get_segment_info`
