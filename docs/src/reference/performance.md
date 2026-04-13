# Barq v2 Performance Benchmarks

Barq v2 includes a dedicated benchmark utility crate, `barq-bench`, for deterministic ingest and search benchmark runs.

## What It Covers

- Deterministic dataset generation with a fixed seed
- Ingest benchmark utilities with structured JSON/CSV output
- Search benchmark utilities with deterministic percentile and QPS reporting
- Reproducible shell scripts for 1M, 10M, and 50M benchmark scenarios

## Run The Benchmarks

Scripted runs:

```bash
./benchmarks/scripts/run_1m.sh
./benchmarks/scripts/run_10m.sh
./benchmarks/scripts/run_50m.sh
```

Direct CLI runs:

```bash
cargo run -p barq-bench -- --format json ingest --seed 11 --count 1000000 --dimension 128
cargo run -p barq-bench -- --format json search --queries 10000 --simulated-latency-micros 250
```

## Notes

- These are benchmark utilities, not correctness tests.
- Search benchmarks currently use deterministic simulated latency inputs rather than executing live index queries.
- CLI memory reporting currently emits placeholder baseline/peak values rather than OS-level RSS sampling.
- Benchmark scripts are intended to be reproducible starting points for comparative runs across environments.

## Source Files

- `benchmarks/README.md`
- `benchmarks/scripts/run_1m.sh`
- `benchmarks/scripts/run_10m.sh`
- `benchmarks/scripts/run_50m.sh`
