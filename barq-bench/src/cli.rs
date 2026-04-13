use clap::{Parser, Subcommand};

#[derive(Debug, Clone, Parser, PartialEq, Eq)]
#[command(name = "barq-bench", about = "Benchmark utilities for Barq-DB")]
pub struct Cli {
    /// Output format for benchmark results.
    #[arg(long, default_value = "json", value_parser = ["json", "csv"])]
    pub format: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Run ingestion benchmarks.
    Ingest {
        #[arg(long)]
        seed: u64,
        #[arg(long)]
        count: usize,
        #[arg(long)]
        dimension: usize,
    },
    /// Run search benchmarks.
    Search {
        #[arg(long)]
        seed: u64,
        #[arg(long)]
        count: usize,
        #[arg(long)]
        dimension: usize,
        #[arg(long)]
        queries: usize,
    },
}
