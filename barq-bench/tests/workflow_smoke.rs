use std::fs;
use std::path::Path;

#[test]
fn benchmark_ci_workflow_runs_benchmark_smoke_commands() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join(".github/workflows/benchmarks.yml");
    let workflow = fs::read_to_string(&workflow_path)
        .expect("benchmark workflow should exist");

    assert!(workflow.contains("cargo test -p barq-bench"));
    assert!(workflow.contains("cargo run -p barq-bench -- --format json ingest"));
    assert!(workflow.contains(
        "cargo run -p barq-bench -- --format json search --seed 11 --count 128 --dimension 16 --queries 16"
    ));
}
