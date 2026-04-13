use barq_bench::report::{build_memory_report, render_csv, BenchmarkReport};
use serde_json::Value;

#[test]
fn memory_report_fields_are_populated() {
    let memory = build_memory_report(1024, 4096, 8192);

    assert_eq!(memory.rss_before_bytes, 1024);
    assert_eq!(memory.rss_after_bytes, 4096);
    assert_eq!(memory.rss_delta_bytes, 3072);
    assert_eq!(memory.peak_rss_bytes, 8192);
}

#[test]
fn json_and_csv_schemas_are_valid() {
    let report = BenchmarkReport {
        benchmark: "search".to_string(),
        format_version: 1,
        memory: build_memory_report(200, 500, 700),
    };

    let json = serde_json::to_string(&report).expect("json serialization should work");
    let value: Value = serde_json::from_str(&json).expect("json should parse");
    assert!(value.get("benchmark").is_some());
    assert!(value.get("format_version").is_some());
    assert!(value
        .get("memory")
        .and_then(|m| m.get("rss_before_bytes"))
        .is_some());

    let csv = render_csv(&report);
    let mut lines = csv.lines();
    let header = lines.next().expect("csv should contain header");
    let row = lines.next().expect("csv should contain one row");
    assert_eq!(
        header,
        "benchmark,format_version,rss_before_bytes,rss_after_bytes,rss_delta_bytes,peak_rss_bytes"
    );
    assert_eq!(row, "search,1,200,500,300,700");
}
