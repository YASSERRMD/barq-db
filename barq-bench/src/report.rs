use serde::{Deserialize, Serialize};

/// Memory usage details captured around a benchmark run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryReport {
    pub rss_before_bytes: u64,
    pub rss_after_bytes: u64,
    pub rss_delta_bytes: i64,
    pub peak_rss_bytes: u64,
}

/// Full benchmark output schema emitted by the benchmark utility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub benchmark: String,
    pub format_version: u32,
    pub memory: MemoryReport,
}

pub fn build_memory_report(rss_before_bytes: u64, rss_after_bytes: u64, peak_rss_bytes: u64) -> MemoryReport {
    MemoryReport {
        rss_before_bytes,
        rss_after_bytes,
        rss_delta_bytes: rss_after_bytes as i64 - rss_before_bytes as i64,
        peak_rss_bytes,
    }
}

pub fn render_csv(report: &BenchmarkReport) -> String {
    let header = "benchmark,format_version,rss_before_bytes,rss_after_bytes,rss_delta_bytes,peak_rss_bytes";
    let row = format!(
        "{},{},{},{},{},{}",
        report.benchmark,
        report.format_version,
        report.memory.rss_before_bytes,
        report.memory.rss_after_bytes,
        report.memory.rss_delta_bytes,
        report.memory.peak_rss_bytes
    );
    format!("{header}\n{row}\n")
}
