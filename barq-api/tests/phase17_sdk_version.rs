use barq_api::grpc::GrpcService;
use barq_api::{ApiAuth, ApiRole, AppState, ClusterConfig, ClusterRouter};
use barq_core::{DistanceMetric, TenantId};
use barq_proto::barq::barq_server::BarqServer;
use barq_sdk_rust::BarqClient;
use barq_storage::Storage;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;
use tokio::process::Command;
use tonic::transport::Server;

fn sdk_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn start_test_grpc_server() -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let dir = tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let auth = ApiAuth::new().require_keys();
    auth.insert("sdk-key", TenantId::new("tenant-sdk"), ApiRole::TenantAdmin);
    let cluster = ClusterRouter::from_config(ClusterConfig::single_node()).unwrap();

    let state = AppState::new(storage, auth, cluster);
    let service = GrpcService::new(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(BarqServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    (local_addr, handle, tx)
}

async fn run_command<I, S>(
    label: &str,
    workdir: &Path,
    envs: &[(&str, &str)],
    program: &str,
    args: I,
) where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.current_dir(workdir);
    command.args(args);
    command.envs(envs.iter().copied());

    let output = command.output().await.unwrap_or_else(|error| {
        panic!("{label} failed to start: {error}");
    });

    if !output.status.success() {
        panic!(
            "{label} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

#[tokio::test]
async fn test_sdk_public_clients_preserve_v1_behavior() {
    let (addr, handle, tx) = start_test_grpc_server().await;
    let grpc_addr = addr.to_string();
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();

    {
        let _guard = sdk_env_lock().lock().unwrap();
        std::env::set_var("BARQ_GRPC_ADDR", &grpc_addr);
        std::env::set_var("API_VERSION", "v1");

        let client = BarqClient::new("http://127.0.0.1:8080", "sdk-key");
        client.health().await.expect("rust health");
        client
            .create_collection("sdk-rust-v1", 2, DistanceMetric::Cosine, None, None)
            .await
            .expect("rust create collection");

        let collection = client.collection("sdk-rust-v1");
        collection
            .insert(
                barq_core::DocumentId::U64(100),
                vec![1.0, 0.0],
                Some(serde_json::json!({"sdk": "rust", "mode": "v1"})),
            )
            .await
            .expect("rust insert");

        let results = collection
            .search(Some(vec![1.0, 0.0]), None, 1, None, None)
            .await
            .expect("rust search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["id"], serde_json::json!({"U64": 100}));

        std::env::remove_var("API_VERSION");
        std::env::remove_var("BARQ_GRPC_ADDR");
    }

    let common_env = [
        ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
        ("BARQ_BASE_URL", "http://127.0.0.1:8080"),
        ("BARQ_API_KEY", "sdk-key"),
        ("API_VERSION", "v1"),
    ];

    run_command(
        "python compat client v1",
        &workspace_root.join("barq-sdk-python"),
        &common_env,
        "python3",
        [
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_compat_client.py",
        ],
    )
    .await;

    run_command(
        "go compat client v1",
        &workspace_root.join("barq-sdk-go"),
        &common_env,
        "go",
        [
            "test",
            "./...",
            "-run",
            "TestCompatClientCreateInsertSearch",
            "-count=1",
        ],
    )
    .await;

    run_command(
        "typescript build",
        &workspace_root.join("barq-sdk-ts"),
        &[],
        "node",
        ["./node_modules/typescript/lib/tsc.js", "--pretty", "false"],
    )
    .await;

    run_command(
        "typescript compat client v1",
        &workspace_root.join("barq-sdk-ts"),
        &common_env,
        "node",
        ["--test", "test/compat_client.test.js"],
    )
    .await;

    tx.send(()).unwrap();
    handle.await.unwrap();
}
