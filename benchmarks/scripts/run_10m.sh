#!/usr/bin/env bash
set -euo pipefail

cargo run -p barq-bench -- --format json ingest --seed 11 --count 10000000 --dimension 128
cargo run -p barq-bench -- --format json search --seed 11 --count 10000000 --dimension 128 --queries 50000
