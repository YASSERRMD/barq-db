use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn read_file(path: &str) -> String {
    fs::read_to_string(workspace_root().join(path)).unwrap_or_else(|error| {
        panic!("failed to read {path}: {error}");
    })
}

#[test]
fn canonical_proto_is_documented_across_docs_and_sdks() {
    let cases: &[(&str, &[&str])] = &[
        (
            "README.md",
            &[
                "Barq v2",
                "proto/barq.proto",
                "gRPC is the primary API surface",
                "Performance Benchmarks",
            ],
        ),
        (
            "docs/src/reference/performance.md",
            &[
                "Barq v2 Performance Benchmarks",
                "barq-bench",
                "./benchmarks/scripts/run_1m.sh",
                "cargo run -p barq-bench",
            ],
        ),
        (
            "docs/src/reference/api.md",
            &[
                "proto/barq.proto",
                "Status",
                "Insert",
                "GetMetrics",
                "GetClusterStatus",
                "GetSegmentInfo",
            ],
        ),
        (
            "docs/src/reference/sdks.md",
            &[
                "Barq v2",
                "proto/barq.proto",
                "GrpcClient",
                "get_metrics",
                "GetMetrics",
                "Performance Benchmarks",
            ],
        ),
        (
            "barq-sdk-python/README.md",
            &[
                "Barq v2",
                "proto/barq.proto",
                "GrpcClient",
                "get_metrics()",
                "get_segment_info()",
                "Performance Benchmarks",
            ],
        ),
        (
            "barq-sdk-go/README.md",
            &[
                "Barq v2",
                "proto/barq.proto",
                "GrpcClient",
                "GetMetrics",
                "GetSegmentInfo",
                "Performance Benchmarks",
            ],
        ),
        (
            "barq-sdk-rust/README.md",
            &[
                "Barq v2",
                "proto/barq.proto",
                "BarqGrpcClient",
                "get_metrics",
                "get_segment_info",
                "Performance Benchmarks",
            ],
        ),
        (
            "barq-sdk-ts/README.md",
            &[
                "Barq v2",
                "proto/barq.proto",
                "GrpcClient",
                "getMetrics()",
                "getSegmentInfo()",
                "Performance Benchmarks",
            ],
        ),
    ];

    for (path, required) in cases {
        let contents = read_file(path);
        for needle in *required {
            assert!(
                contents.contains(needle),
                "{path} should contain {needle:?}"
            );
        }
    }
}
