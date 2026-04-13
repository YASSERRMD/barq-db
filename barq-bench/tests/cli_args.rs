use barq_bench::cli::{Cli, Command};
use clap::Parser;

#[test]
fn parses_ingest_args() {
    let parsed = Cli::parse_from([
        "barq-bench",
        "--format",
        "json",
        "ingest",
        "--seed",
        "7",
        "--count",
        "100",
        "--dimension",
        "64",
    ]);

    assert_eq!(
        parsed,
        Cli {
            format: "json".to_string(),
            command: Command::Ingest {
                seed: 7,
                count: 100,
                dimension: 64,
            },
        }
    );
}

#[test]
fn rejects_invalid_args_cleanly() {
    let err = Cli::try_parse_from([
        "barq-bench",
        "search",
        "--seed",
        "7",
        "--count",
        "100",
        "--dimension",
        "64",
        "--queries",
        "bad",
    ])
    .expect_err("invalid count should fail");
    let rendered = err.to_string();
    assert!(rendered.contains("invalid value"));
}
