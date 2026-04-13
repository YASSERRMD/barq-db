use barq_api::grpc::GrpcService;
use barq_api::{ApiAuth, AppState, ClusterConfig, ClusterRouter};
use barq_core::DistanceMetric;
use barq_sdk_rust::BarqGrpcClient;
use barq_proto::barq::barq_server::BarqServer;
use barq_storage::Storage;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use tokio::process::Command;
use tonic::transport::Server;

async fn start_test_grpc_server() -> (
    SocketAddr,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let dir = tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let auth = ApiAuth::new();
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
async fn test_sdk_grpc_smoke_clients() {
    let (addr, handle, tx) = start_test_grpc_server().await;
    let grpc_addr = addr.to_string();
    let grpc_endpoint = format!("http://{}", addr);
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();

    let mut rust_client = BarqGrpcClient::connect(grpc_endpoint)
        .await
        .expect("rust sdk connect");
    assert!(rust_client.status().await.expect("rust status"));
    rust_client
        .create_collection("sdk-rust-grpc", 2, DistanceMetric::Cosine)
        .await
        .expect("rust create collection");
    rust_client
        .insert(
            "sdk-rust-grpc",
            "rust-doc",
            vec![1.0, 0.0],
            serde_json::json!({"sdk": "rust", "mode": "grpc"}),
        )
        .await
        .expect("rust insert");
    let rust_results = rust_client
        .search("sdk-rust-grpc", vec![1.0, 0.0], 1)
        .await
        .expect("rust search");
    assert_eq!(rust_results.len(), 1);
    assert_eq!(rust_results[0]["id"], "rust-doc");

    run_command(
        "python grpc smoke",
        &workspace_root.join("barq-sdk-python"),
        &[
            ("PYTHONPATH", "."),
            ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
            ("BARQ_TEST_COLLECTION", "sdk-python-grpc"),
        ],
        "python3",
        ["-m", "unittest", "discover", "-s", "tests", "-p", "test_grpc_smoke.py"],
    )
    .await;

    run_command(
        "go grpc smoke",
        &workspace_root.join("barq-sdk-go"),
        &[
            ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
            ("BARQ_TEST_COLLECTION", "sdk-go-grpc"),
        ],
        "go",
        ["test", "./...", "-run", "TestGrpcSmoke", "-count=1"],
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
        "typescript grpc smoke",
        &workspace_root.join("barq-sdk-ts"),
        &[
            ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
            ("BARQ_TEST_COLLECTION", "sdk-ts-grpc"),
        ],
        "node",
        ["--test", "test/grpc_smoke.test.js"],
    )
    .await;

    tx.send(()).unwrap();
    handle.await.unwrap();
}
