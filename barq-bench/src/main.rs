use barq_bench::cli::{Cli, Command};
use barq_bench::dataset::{generate_dataset, DatasetConfig};
use barq_bench::ingest::{run_ingestion_benchmark, IngestionBenchmarkConfig};
use barq_bench::report::{render_csv, BenchmarkReport};
use barq_bench::runtime::sample_memory_report;
use barq_bench::search::{run_search_benchmark, SearchBenchmarkConfig};
use clap::Parser;

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Ingest {
            seed,
            count,
            dimension,
        } => {
            let dataset = generate_dataset(&DatasetConfig {
                seed,
                count,
                dimension,
            })
            .expect("invalid dataset parameters");

            let (ingest, memory) = sample_memory_report(|| {
                run_ingestion_benchmark(
                    &IngestionBenchmarkConfig {
                        warmup_iterations: 1,
                        measured_iterations: 1,
                    },
                    &dataset,
                )
            })
            .expect("ingestion benchmark should succeed");

            let report = BenchmarkReport {
                benchmark: ingest.benchmark,
                format_version: 1,
                memory,
            };
            emit(&cli.format, &report);
        }
        Command::Search {
            seed,
            count,
            dimension,
            queries,
        } => {
            let dataset = generate_dataset(&DatasetConfig {
                seed,
                count,
                dimension,
            });
            let dataset = dataset.expect("invalid dataset parameters");
            let (stats, memory) = sample_memory_report(|| {
                run_search_benchmark(&SearchBenchmarkConfig { queries }, &dataset)
            })
            .expect("search benchmark should succeed");

            let report = BenchmarkReport {
                benchmark: format!(
                    "search:p50={:.3},p95={:.3},p99={:.3},qps={:.2}",
                    stats.p50_millis, stats.p95_millis, stats.p99_millis, stats.qps
                ),
                format_version: 1,
                memory,
            };
            emit(&cli.format, &report);
        }
    }
}

fn emit(format: &str, report: &BenchmarkReport) {
    match format {
        "json" => println!(
            "{}",
            serde_json::to_string_pretty(report).expect("json should serialize")
        ),
        "csv" => print!("{}", render_csv(report)),
        _ => unreachable!("clap value parser should prevent unsupported formats"),
    }
}
