use barq_api::grpc::GrpcService;
use barq_api::{ApiAuth, AppState, ClusterConfig, ClusterRouter};
use barq_core::DistanceMetric;
use barq_proto::barq::barq_server::BarqServer;
use barq_sdk_rust::{BarqClient, InsertOptions, SearchConsistency, SearchOptions};
use barq_storage::Storage;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;
use tokio::process::Command;
use tonic::transport::Server;

fn sdk_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: impl Into<String>) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value.into());
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

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
async fn test_sdk_advanced_options_end_to_end() {
    let _env_guard = sdk_env_lock().lock().unwrap();
    let (addr, handle, tx) = start_test_grpc_server().await;
    let grpc_addr = addr.to_string();
    let base_url = "http://127.0.0.1:8080";
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();

    let _grpc_override = EnvVarGuard::set("BARQ_GRPC_ADDR", &grpc_addr);
    let rust_client = BarqClient::new(base_url, "");
    rust_client.health().await.expect("rust sdk health");
    rust_client
        .create_collection("sdk-rust-options", 2, DistanceMetric::Cosine, None, None)
        .await
        .expect("rust sdk create collection");
    rust_client
        .collection("sdk-rust-options")
        .insert_with_options(
            "rust-options-doc",
            vec![1.0, 0.0],
            Some(serde_json::json!({"sdk": "rust", "mode": "options"})),
            InsertOptions::new().wait_for_commit(true),
        )
        .await
        .expect("rust sdk insert with options");
    let rust_results = rust_client
        .collection("sdk-rust-options")
        .search_with_options(
            vec![1.0, 0.0],
            1,
            SearchOptions::new()
                .consistency(SearchConsistency::Primary)
                .allow_fallback(true),
        )
        .await
        .expect("rust sdk search with options");
    assert_eq!(rust_results.len(), 1);
    assert_eq!(
        rust_results[0]["id"],
        serde_json::json!({ "Str": "rust-options-doc" })
    );
    drop(_grpc_override);

    run_command(
        "python sdk options smoke",
        &workspace_root.join("barq-sdk-python"),
        &[
            ("PYTHONPATH", "."),
            ("BARQ_BASE_URL", base_url),
            ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
            ("BARQ_TEST_COLLECTION", "sdk-python-options"),
        ],
        "python3",
        [
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_options_smoke.py",
        ],
    )
    .await;

    run_command(
        "go sdk options smoke",
        &workspace_root.join("barq-sdk-go"),
        &[
            ("BARQ_BASE_URL", base_url),
            ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
            ("BARQ_TEST_COLLECTION", "sdk-go-options"),
        ],
        "go",
        [
            "test",
            "./...",
            "-run",
            "TestOptionsClientInsertAndSearch",
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
        "typescript sdk options smoke",
        &workspace_root.join("barq-sdk-ts"),
        &[
            ("BARQ_BASE_URL", base_url),
            ("BARQ_GRPC_ADDR", grpc_addr.as_str()),
            ("BARQ_TEST_COLLECTION", "sdk-ts-options"),
        ],
        "node",
        ["--test", "test/options_smoke.test.js"],
    )
    .await;

    tx.send(()).unwrap();
    handle.await.unwrap();
}
