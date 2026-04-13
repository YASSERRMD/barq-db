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
            &["proto/barq.proto", "gRPC is the primary API surface"],
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
            &["proto/barq.proto", "GrpcClient", "get_metrics", "GetMetrics"],
        ),
        (
            "barq-sdk-python/README.md",
            &["proto/barq.proto", "GrpcClient", "get_metrics()", "get_segment_info()"],
        ),
        (
            "barq-sdk-go/README.md",
            &["proto/barq.proto", "GrpcClient", "GetMetrics", "GetSegmentInfo"],
        ),
        (
            "barq-sdk-rust/README.md",
            &[
                "proto/barq.proto",
                "BarqGrpcClient",
                "get_metrics",
                "get_segment_info",
            ],
        ),
        (
            "barq-sdk-ts/README.md",
            &["proto/barq.proto", "GrpcClient", "getMetrics()", "getSegmentInfo()"],
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
