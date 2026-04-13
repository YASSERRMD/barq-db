# Barq-DB Benchmark Utilities

These scripts run **benchmark utilities**, not correctness tests.

## Reproducible benchmark runs

```bash
./benchmarks/scripts/run_1m.sh
./benchmarks/scripts/run_10m.sh
./benchmarks/scripts/run_50m.sh
```

For direct CLI usage:

```bash
cargo run -p barq-bench -- --format json ingest --seed 11 --count 1000000 --dimension 128
cargo run -p barq-bench -- --format json search --seed 11 --count 1000000 --dimension 128 --queries 10000
```
