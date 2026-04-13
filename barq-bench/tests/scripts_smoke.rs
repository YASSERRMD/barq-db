use std::fs;

#[test]
fn benchmark_scripts_have_expected_commands() {
    for path in [
        "../benchmarks/scripts/run_1m.sh",
        "../benchmarks/scripts/run_10m.sh",
        "../benchmarks/scripts/run_50m.sh",
    ] {
        let script = fs::read_to_string(path).expect("script should exist");
        assert!(script.contains("cargo run -p barq-bench"));
        assert!(script.contains("--format json"));
        assert!(script.contains("--seed 11"));
        assert!(script.contains("--count"));
        assert!(script.contains("--dimension 128"));
    }
}
