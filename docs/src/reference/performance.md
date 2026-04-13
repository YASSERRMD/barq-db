# Barq v2 Performance Benchmarks

Barq v2 includes a dedicated benchmark utility crate, `barq-bench`, for deterministic ingest and search benchmark runs.

## What It Covers

- Deterministic dataset generation with a fixed seed
- Ingest benchmark utilities backed by real collection inserts
- Search benchmark utilities backed by live in-process collection searches
- Reproducible shell scripts for 1M, 10M, and 50M benchmark scenarios
- RSS sampling before and after benchmark execution
- GitHub Actions benchmark smoke coverage via `.github/workflows/benchmarks.yml`

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
cargo run -p barq-bench -- --format json search --seed 11 --count 1000000 --dimension 128 --queries 10000
```

## Notes

- These are benchmark utilities, not correctness tests.
- Search benchmarks execute live search calls against deterministic in-process collections.
- Memory reporting samples RSS before and after benchmark execution.
- Benchmark scripts are intended to be reproducible starting points for comparative runs across environments.

## Source Files

- `benchmarks/README.md`
- `benchmarks/scripts/run_1m.sh`
- `benchmarks/scripts/run_10m.sh`
- `benchmarks/scripts/run_50m.sh`
