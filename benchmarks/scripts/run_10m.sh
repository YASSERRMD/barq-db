#!/usr/bin/env bash
set -euo pipefail

cargo run -p barq-bench -- --format json ingest --seed 11 --count 10000000 --dimension 128
cargo run -p barq-bench -- --format json search --queries 50000 --simulated-latency-micros 250
