#!/usr/bin/env bash
set -euo pipefail

cargo run -p barq-bench -- --format json ingest --seed 11 --count 1000000 --dimension 128
cargo run -p barq-bench -- --format json search --seed 11 --count 1000000 --dimension 128 --queries 10000
