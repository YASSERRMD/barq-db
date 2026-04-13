# SDKs

Barq SDKs are gRPC-first. The canonical contract is defined in `proto/barq.proto`.

## Current gRPC Client Types

| Language | Client |
|----------|--------|
| Python | `GrpcClient` |
| TypeScript / Node.js | `GrpcClient` |
| Go | `GrpcClient` |
| Rust | `BarqGrpcClient` |

The HTTP clients remain available for compatibility with the current HTTP surface, but new contract work should start from the gRPC definitions in `proto/barq.proto`.
