use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Summary statistics for search benchmark latency and throughput.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchStats {
    pub samples: usize,
    pub p50_millis: f64,
    pub p95_millis: f64,
    pub p99_millis: f64,
    pub qps: f64,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchBenchmarkConfig {
    pub queries: usize,
    pub simulated_latency_micros: u64,
}

pub fn run_search_benchmark(config: &SearchBenchmarkConfig) -> SearchStats {
    let per_query = Duration::from_micros(config.simulated_latency_micros);
    let latencies = vec![per_query; config.queries];
    let total = Duration::from_micros(config.simulated_latency_micros * config.queries as u64);
    calculate_search_stats(&latencies, total)
}

pub fn percentile(sorted_samples: &[f64], percentile: f64) -> f64 {
    assert!(!sorted_samples.is_empty(), "samples cannot be empty");
    assert!((0.0..=100.0).contains(&percentile), "percentile out of range");

    let rank = (percentile / 100.0) * ((sorted_samples.len() - 1) as f64);
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;

    if lower == upper {
        sorted_samples[lower]
    } else {
        let fraction = rank - lower as f64;
        sorted_samples[lower] + (sorted_samples[upper] - sorted_samples[lower]) * fraction
    }
}

pub fn calculate_search_stats(latencies: &[Duration], total_duration: Duration) -> SearchStats {
    assert!(!latencies.is_empty(), "latencies cannot be empty");

    let mut millis: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    millis.sort_by(|a, b| a.total_cmp(b));
    let seconds = total_duration.as_secs_f64().max(f64::EPSILON);

    SearchStats {
        samples: millis.len(),
        p50_millis: percentile(&millis, 50.0),
        p95_millis: percentile(&millis, 95.0),
        p99_millis: percentile(&millis, 99.0),
        qps: millis.len() as f64 / seconds,
    }
}
