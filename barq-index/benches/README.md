# Barq DB Benchmarks

This directory contains comprehensive benchmarks for measuring Barq DB's performance across vector search, text search, and storage operations.

## Running Benchmarks

### Quick Start

```bash
# Run all benchmarks
cargo bench -p barq-index

# Run a specific benchmark group
cargo bench -p barq-index -- search
cargo bench -p barq-index -- index_build
cargo bench -p barq-index -- distance

# Save results to JSON
cargo bench -p barq-index -- --save-baseline main
```

## Benchmark Categories

### 1. Index Construction (`index_build`)

Measures time to build various index types with different parameters:

| Benchmark | Description |
|-----------|-------------|
| `flat/{dim}` | Flat index build time at dimension |
| `hnsw_m16/{dim}` | HNSW with M=16 at dimension |
| `hnsw_m32/{dim}` | HNSW with M=32 at dimension |

Dimensions tested: 128, 384, 768

### 2. Search Performance (`search`)

Measures query latency for different index types and top-k values:

| Benchmark | Description |
|-----------|-------------|
| `flat/{k}` | Brute-force search, top-k results |
| `hnsw/{k}` | HNSW approximate search, top-k results |

Top-k values: 10, 50, 100

### 3. Distance Calculations (`distance`)

Measures raw distance computation performance:

| Benchmark | Description |
|-----------|-------------|
| `l2/{dim}` | L2 (Euclidean) distance |
| `cosine/{dim}` | Cosine distance |
| `dot/{dim}` | Dot product |

Dimensions tested: 128, 384, 768, 1536

### 4. Batch Insert (`batch_insert`)

Measures bulk insert throughput:

| Benchmark | Description |
|-----------|-------------|
| `hnsw/{size}` | Insert batch of documents into HNSW index |

Batch sizes: 100, 500, 1000

### 5. Scaling (`scaling`)

Measures how performance scales with dataset size:

| Benchmark | Description |
|-----------|-------------|
| `hnsw_build/{size}` | Build time vs dataset size |

Dataset sizes: 1,000, 5,000, 10,000

## Interpreting Results

Criterion outputs results to `target/criterion/`. Each benchmark includes:

- **Mean time**: Average execution time
- **Throughput**: Elements/second (for insert/search)
- **Comparison**: vs baseline if available

### Example Output

```
search/hnsw/10          time:   [2.1234 ms 2.1456 ms 2.1678 ms]
                        thrpt:  [46,130 elem/s 46,605 elem/s 47,092 elem/s]
                        change: [-2.3% -1.1% +0.2%] (p = 0.12 > 0.05)
```

## CI Integration

Benchmarks run nightly via GitHub Actions. Results are compared against the `main` baseline. Regressions > 10% trigger alerts.

To compare against a baseline:

```bash
# Set baseline
cargo bench -p barq-index -- --save-baseline before-change

# Make changes...

# Compare
cargo bench -p barq-index -- --baseline before-change
```

## Adding New Benchmarks

1. Add benchmark function to `barq-index/benches/ann_bench.rs`
2. Register in `criterion_group!` macro
3. Update this README

## Hardware Recommendations

For consistent results:
- Disable CPU frequency scaling
- Close background applications
- Use `nice -n -20` for benchmark processes
- Run multiple iterations (Criterion handles this)
