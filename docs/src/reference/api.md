# API Specification

`proto/barq.proto` is the canonical Barq API definition.

## Canonical RPCs

| RPC | Purpose |
|-----|---------|
| `Status` | Server status / health |
| `CreateCollection` | Create a collection |
| `Insert` | Insert a document |
| `Search` | Search a collection |

## Compatibility RPCs

These RPCs are still present while older clients migrate:

| RPC | Notes |
|-----|-------|
| `Health` | Compatibility alias for `Status` |
| `InsertDocument` | Compatibility alias for `Insert` |
| `BatchSearch` | Legacy compatibility RPC |

## SDK Alignment

The gRPC clients in the Python, TypeScript, Go, and Rust SDKs are expected to track `proto/barq.proto` directly. HTTP endpoints remain available as a compatibility surface, not the source of truth for the contract.
