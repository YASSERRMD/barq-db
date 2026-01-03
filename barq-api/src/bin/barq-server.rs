use barq_api::{ApiAuth, AppState, build_router_from_state, ClusterConfig, ClusterRouter};
use barq_api::grpc::GrpcService;
use barq_proto::barq::barq_server::BarqServer;
use barq_storage::Storage;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tonic::transport::Server as TonicServer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "barq-server", about = "Barq DB Server")]
struct Cli {
    /// HTTP listening address
    #[arg(long, env = "BARQ_ADDR", default_value = "0.0.0.0:8080")]
    addr: SocketAddr,

    /// Storage directory path
    #[arg(long, env = "BARQ_STORAGE_DIR", default_value = "./data")]
    storage_dir: PathBuf,

    /// Enable TLS
    #[arg(long, env = "BARQ_TLS_ENABLED")]
    tls: bool,

    /// TLS certificate path
    #[arg(long, env = "BARQ_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    /// TLS private key path
    #[arg(long, env = "BARQ_TLS_KEY")]
    tls_key: Option<PathBuf>,

    /// TLS client CA path (for mTLS)
    #[arg(long, env = "BARQ_TLS_CLIENT_CA")]
    tls_client_ca: Option<PathBuf>,

    /// Cluster configuration file path
    #[arg(long, env = "BARQ_CLUSTER_CONFIG")]
    cluster: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize observability
    init_observability();

    let cli = Cli::parse();
    info!("Starting Barq Server");
    info!("Storage directory: {:?}", cli.storage_dir);
    
    // Initialize TieringManager if enabled
    let tiering_manager = if std::env::var("BARQ_TIERING_ENABLED").unwrap_or_default() == "true" {
        use barq_storage::{
            TieringManager, TieringPolicy, LocalObjectStore, ObjectStore,
            S3ObjectStore, GcsObjectStore, AzureBlobStore
        };
        use std::sync::Arc;

        info!("Initializing storage tiering...");
        
        let hot_path = cli.storage_dir.clone();
        if !hot_path.exists() {
             std::fs::create_dir_all(&hot_path)?;
        }
        let hot_store = Arc::new(LocalObjectStore::new(hot_path)?);

        let create_cloud_store = |provider: String, bucket: String| -> Option<Arc<dyn ObjectStore>> {
            match provider.as_str() {
                "s3" => {
                    info!("Configuring S3 tier: {}", bucket);
                    S3ObjectStore::new(bucket).ok().map(|s| Arc::new(s) as Arc<dyn ObjectStore>)
                },
                "gcs" => {
                    info!("Configuring GCS tier: {}", bucket);
                    GcsObjectStore::new(bucket).ok().map(|s| Arc::new(s) as Arc<dyn ObjectStore>)
                },
                "azure" => {
                    info!("Configuring Azure tier: {}", bucket);
                     let account = std::env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default();
                     let key = std::env::var("AZURE_STORAGE_ACCESS_KEY").unwrap_or_default();
                     AzureBlobStore::new(account, key, bucket).ok().map(|s| Arc::new(s) as Arc<dyn ObjectStore>)
                },
                _ => {
                    warn!("Unknown storage provider: {}", provider);
                    None
                }
            }
        };

        let warm_store = if let (Ok(p), Ok(b)) = (std::env::var("BARQ_WARM_TIER_PROVIDER"), std::env::var("BARQ_WARM_TIER_BUCKET")) {
            create_cloud_store(p, b)
        } else {
            None
        };

        let cold_store = if let (Ok(p), Ok(b)) = (std::env::var("BARQ_COLD_TIER_PROVIDER"), std::env::var("BARQ_COLD_TIER_BUCKET")) {
            create_cloud_store(p, b)
        } else {
            None
        };

        let tm = TieringManager::with_tiers(
            hot_store,
            warm_store,
            cold_store,
            TieringPolicy::default(),
        );
        
        // Configure persistence for tiering state in the root of data directory
        tm.set_state_path(cli.storage_dir.join("tiering_state.json"));
        
        info!("Storage tiering initialized");
        Some(Arc::new(tm))
    } else {
        None
    };

    // Initialize storage
    let storage = Storage::open_with_options(
        &cli.storage_dir, 
        barq_storage::StorageOptions {
            tiering_manager,
        }
    )?;

    // Setup auth (default to no auth for now, can be enhanced)
    let auth = ApiAuth::new();

    // Setup cluster
    let cluster_config = if let Some(path) = cli.cluster {
        ClusterConfig::from_path(path).expect("failed to load cluster config")
    } else {
        ClusterConfig::from_env_or_default().expect("failed to load cluster config")
    };
    let cluster = ClusterRouter::from_config(cluster_config).expect("invalid cluster config");

    let state = AppState::new(storage, auth, cluster);
    let grpc_service = GrpcService::new(state.clone());

    // Axum Router
    let app = build_router_from_state(state.clone()).layer(TraceLayer::new_for_http());

    // Spawn gRPC server
    let grpc_addr = "0.0.0.0:50051".parse().unwrap();
    info!("gRPC server listening on {}", grpc_addr);
    tokio::spawn(async move {
        TonicServer::builder()
            .add_service(BarqServer::new(grpc_service))
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });

    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C signal handler");
        info!("shutdown signal received");
    };

    let listener = tokio::net::TcpListener::bind(cli.addr).await?;

    if let (Some(cert_path), Some(key_path)) = (cli.tls_cert, cli.tls_key) {
        use axum_server::tls_rustls::RustlsConfig;
        
        let config = RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .expect("failed to load TLS config");
            
        info!("Barq (TLS) listening on {}", cli.addr);
        
        // Convert tokio listener to std for axum_server
        let std_listener = listener.into_std().expect("failed to convert listener");
        std_listener.set_nonblocking(true).expect("failed to set nonblocking");

        axum_server::from_tcp_rustls(std_listener, config)
            .expect("TLS bind failed")
            .serve(app.into_make_service())
            .await
            .expect("server failed");
    } else {
        info!("Barq listening on {}", cli.addr);
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .expect("server failed");
    }

    info!("Server stopped successfully");
    Ok(())
}

fn init_observability() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_file(true)
        .json(); // Structured JSON logs

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,barq_api=debug,barq_storage=debug".into());

    let registry = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(env_filter);

    // Optional OpenTelemetry integration
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        use opentelemetry::KeyValue;
        use opentelemetry_sdk::{trace as sdktrace, Resource};
        

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic(),
            )
            .with_trace_config(
                sdktrace::config().with_resource(Resource::new(vec![
                    KeyValue::new("service.name", "barq-server"),
                ])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .expect("failed to install OpenTelemetry tracer");

        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        registry.with(otel_layer).init();
        info!("OpenTelemetry tracing initialized");
    } else {
        registry.init();
    }
}

// Helper to access start_server_with_auth since it is not pub in lib.rs?
// Check lib.rs visibility. start_server_with_auth is pub.

