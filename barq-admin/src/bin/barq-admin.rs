use std::path::PathBuf;

use barq_cluster::{ClusterAdmin, ClusterConfig, NodeConfig, NodeId, ShardId, ShardPlacement};
use clap::{Parser, Subcommand};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(
    name = "barq-admin",
    about = "Admin CLI for managing Barq clusters"
)]
struct Cli {
    /// Path to the cluster config file (JSON)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Admin API endpoint URL
    #[arg(long, global = true, env = "BARQ_ADMIN_ENDPOINT", default_value = "http://localhost:8080")]
    endpoint: String,

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
    /// Trigger compaction for a collection
    Compact {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        collection: String,
    },
    /// Trigger index rebuild
    RebuildIndex {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        collection: String,
    },
    /// Drain a node for maintenance
    Drain {
        #[arg(long)]
        node: String,
    },
    /// Check cluster health
    Health {},
    /// View cluster topology from the running server
    Topology {},
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Handling static config commands
    if let Some(config_path) = &cli.config {
        if matches!(cli.command, Commands::Show {} | Commands::Move { .. } | Commands::AddNode { .. } | Commands::RemoveNode { .. }) {
             let config = ClusterConfig::from_path(config_path)?;
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
                    persist(&admin.config, placements, config_path)?;
                }
                Commands::AddNode { id, address } => {
                    admin.add_node(NodeConfig {
                        id: NodeId::new(id),
                        address,
                    });
                    let router = admin.rebalance()?;
                    persist(&admin.config, router.placements, config_path)?;
                }
                Commands::RemoveNode { id } => {
                    admin.remove_node(&NodeId::new(id));
                    let router = admin.rebalance()?;
                    persist(&admin.config, router.placements, config_path)?;
                }
                _ => {} // Handled below
             }
             return Ok(());
        }
    }

    // Handling runtime commands via HTTP
    let client = reqwest::Client::new();
    let base_url = cli.endpoint.trim_end_matches('/');

    match cli.command {
        Commands::Health {} => {
            let resp = client.get(format!("{}/health", base_url)).send().await?;
            println!("Status: {}", resp.status());
            println!("{}", resp.text().await?);
        }
        Commands::Topology {} => {
            let resp = client.get(format!("{}/admin/topology", base_url))
                .header("x-api-key", "admin-key") // Fallback/Test key, usually from env or arg
                .header("x-tenant-id", "default")
                .send().await?;
            println!("{}", resp.text().await?);
        }
        Commands::Compact { tenant, collection } => {
            let resp = client.post(format!("{}/admin/compact", base_url))
                .header("x-tenant-id", &tenant)
                .json(&json!({ "tenant": tenant, "collection": collection }))
                .send().await?;
            println!("Response: {}", resp.status());
            println!("{}", resp.text().await?);
        }
         Commands::RebuildIndex { tenant, collection } => {
             let resp = client.post(format!("{}/admin/index/rebuild", base_url))
                .header("x-tenant-id", &tenant)
                .json(&json!({ "tenant": tenant, "collection": collection }))
                .send().await?;
            println!("Response: {}", resp.status());
            println!("{}", resp.text().await?);
         }
         Commands::Drain { node } => {
             let resp = client.post(format!("{}/admin/node/drain", base_url))
                .json(&json!({ "node_id": node }))
                .send().await?;
             println!("Response: {}", resp.status());
             println!("{}", resp.text().await?);
         }
        _ => {
            if cli.config.is_none() {
                println!("Error: --config is required for this command");
            }
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
