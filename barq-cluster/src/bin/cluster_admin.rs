use std::path::PathBuf;

use barq_cluster::{ClusterAdmin, ClusterConfig, NodeConfig, NodeId, ShardId, ShardPlacement};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "barq-cluster-admin",
    about = "Admin CLI for managing Barq clusters"
)]
struct Cli {
    /// Path to the cluster config file (JSON)
    #[arg(long, global = true)]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print shard placements for the provided configuration
    Show {},
    /// Move a shard to a new primary and replicas
    Move {
        #[arg(long)]
        shard: u32,
        #[arg(long)]
        primary: String,
        #[arg(long)]
        replicas: Vec<String>,
    },
    /// Add a node to the cluster membership
    AddNode {
        #[arg(long)]
        id: String,
        #[arg(long)]
        address: String,
    },
    /// Remove a node from the cluster membership
    RemoveNode {
        #[arg(long)]
        id: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = ClusterConfig::from_path(&cli.config)?;
    let mut admin = ClusterAdmin::new(config);

    match cli.command {
        Commands::Show {} => {
            let router = barq_cluster::ClusterRouter::from_config(admin.config.clone())?;
            for placement in router.placements.values() {
                println!(
                    "shard {} => primary={}, replicas={:?}",
                    placement.shard.0, placement.primary.0, placement.replicas
                );
            }
        }
        Commands::Move {
            shard,
            primary,
            replicas,
        } => {
            let placements = admin.move_shard(
                ShardId(shard),
                NodeId::new(primary),
                replicas.into_iter().map(NodeId::new).collect(),
            )?;
            persist(&admin.config, placements, &cli.config)?;
        }
        Commands::AddNode { id, address } => {
            admin.add_node(NodeConfig {
                id: NodeId::new(id),
                address,
            });
            let router = admin.rebalance()?;
            persist(&admin.config, router.placements, &cli.config)?;
        }
        Commands::RemoveNode { id } => {
            admin.remove_node(&NodeId::new(id));
            let router = admin.rebalance()?;
            persist(&admin.config, router.placements, &cli.config)?;
        }
    }

    Ok(())
}

fn persist(
    config: &ClusterConfig,
    placements: std::collections::HashMap<ShardId, ShardPlacement>,
    path: &PathBuf,
) -> anyhow::Result<()> {
    println!("writing updated configuration to {}", path.display());
    let mut updated = config.clone();
    updated.shard_count = placements.len() as u32;
    updated.placements = placements;
    updated.to_path(path)?;
    Ok(())
}
