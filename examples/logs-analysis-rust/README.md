# Logs Analysis Demo (Rust)

This example demonstrates a high-throughput log ingestion and analysis pipeline using Barq DB and Rust.

## Prerequisites

- Rust 1.70+
- Barq DB running locally

## Usage

```bash
cargo run
```

## Scenario

1. Creates a `system_logs` collection.
2. Spawns multiple producers generating synthetic logs (INFO, WARN, ERROR).
3. Inserts logs into Barq DB.
4. Performs queries to find "ERROR" logs within a timeframe.
