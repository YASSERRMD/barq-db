use barq_api::{start_server_with_auth, start_tls_server, ApiAuth, TlsConfig};
use barq_storage::Storage;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize observability
    init_observability();

    let cli = Cli::parse();
    info!("Starting Barq Server");
    info!("Storage directory: {:?}", cli.storage_dir);
    
    // Initialize storage
    let storage = Storage::open(&cli.storage_dir)?;

    // Setup auth (default to no auth for now, can be enhanced)
    let auth = ApiAuth::new();

    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Shutdown signal received");
    };

    if cli.tls {
        let cert = cli.tls_cert.expect("TLS certificate path required if TLS enabled");
        let key = cli.tls_key.expect("TLS private key path required if TLS enabled");
        let mut tls_config = TlsConfig::new(cert, key);
        if let Some(ca) = cli.tls_client_ca {
            tls_config = tls_config.with_client_ca(ca);
        }
        
        info!("Listening on https://{}", cli.addr);
        start_tls_server(cli.addr, storage, auth, tls_config, shutdown)
            .await?
            .await??;
    } else {
        let listener = tokio::net::TcpListener::bind(cli.addr).await?;
        info!("Listening on http://{}", cli.addr);
        start_server_with_auth(listener, storage, auth, shutdown).await.await??;
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
        use opentelemetry::{global, KeyValue};
        use opentelemetry_sdk::{trace as sdktrace, Resource};
        use opentelemetry_otlp::WithExportConfig;

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

