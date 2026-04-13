#!/usr/bin/env bash
set -euo pipefail

cargo run -p barq-bench --release -- --format json ingest --seed 11 --count 50000000 --dimension 128
cargo run -p barq-bench --release -- --format json search --queries 100000 --simulated-latency-micros 250
