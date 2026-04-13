use axum::Router;
use barq_api::grpc::GrpcService;
use barq_api::{build_router_from_state, ApiAuth, ApiRole, AppState, ClusterConfig, ClusterRouter};
use barq_core::{DistanceMetric, TenantId};
use barq_proto::barq::barq_server::BarqServer;
use barq_sdk_rust::BarqClient;
use barq_storage::Storage;
use reqwest::Client;
use serde_json::json;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::process::Command;
use tonic::transport::Server;

fn grpc_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn start_http_server(
    app: Router,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    (local_addr, handle, tx)
}

async fn start_grpc_server(
    service: GrpcService,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
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

async fn rest_create_collection(client: &Client, base_url: &str, collection: &str) {
    let response = client
        .post(format!("{base_url}/collections"))
        .json(&json!({
            "name": collection,
            "dimension": 2,
            "metric": "Cosine",
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "create collection failed");
}

async fn rest_insert_document(
    client: &Client,
    base_url: &str,
    collection: &str,
    id: &str,
    vector: [f32; 2],
) {
    let response = client
        .post(format!("{base_url}/collections/{collection}/documents"))
        .json(&json!({
            "id": id,
            "vector": vector,
            "payload": {
                "sdk": "rest",
                "mode": "golden",
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "insert document failed");
}

async fn rest_search_results(client: &Client, base_url: &str, collection: &str) -> serde_json::Value {
    let response = client
        .post(format!("{base_url}/collections/{collection}/search"))
        .json(&json!({
            "vector": [1.0, 0.0],
            "top_k": 2,
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "search request failed");

    let body: serde_json::Value = response.json().await.unwrap();
    body["results"].clone()
}

#[tokio::test]
async fn test_sdk_public_clients_match_rest_golden_results() {
    let dir = tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let auth = ApiAuth::new().require_keys();
    auth.insert("sdk-key", TenantId::new("tenant-sdk"), ApiRole::TenantAdmin);
    let cluster = ClusterRouter::from_config(ClusterConfig::single_node()).unwrap();
    let state = AppState::new(storage, auth, cluster);

    let (http_addr, http_handle, http_tx) = start_http_server(build_router_from_state(state.clone())).await;
    let (grpc_addr, grpc_handle, grpc_tx) = start_grpc_server(GrpcService::new(state)).await;

    let http_base_url = format!("http://{http_addr}");
    let grpc_host = grpc_addr.to_string();
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();

    let rest_client = Client::builder()
        .default_headers(
            [("x-api-key", "sdk-key")]
                .into_iter()
                .map(|(key, value)| {
                    (
                        reqwest::header::HeaderName::from_static(key),
                        reqwest::header::HeaderValue::from_static(value),
                    )
                })
                .collect(),
        )
        .build()
        .unwrap();

    rest_create_collection(&rest_client, &http_base_url, "sdk-rest-golden").await;
    rest_insert_document(
        &rest_client,
        &http_base_url,
        "sdk-rest-golden",
        "golden-primary",
        [1.0, 0.0],
    )
    .await;
    rest_insert_document(
        &rest_client,
        &http_base_url,
        "sdk-rest-golden",
        "golden-secondary",
        [0.0, 1.0],
    )
    .await;
    let expected_results = rest_search_results(&rest_client, &http_base_url, "sdk-rest-golden").await;
    assert_eq!(
        expected_results,
        json!([
            {"id": {"Str": "golden-primary"}, "score": 1.0},
            {"id": {"Str": "golden-secondary"}, "score": 0.0}
        ])
    );

    {
        let _guard = grpc_env_lock().lock().unwrap();
        std::env::set_var("BARQ_GRPC_ADDR", &grpc_host);

        let client = BarqClient::new(http_base_url.clone(), "sdk-key");
        client.health().await.expect("rust health");
        client
            .create_collection("sdk-rust-golden", 2, DistanceMetric::Cosine, None, None)
            .await
            .expect("rust create collection");

        let collection = client.collection("sdk-rust-golden");
        collection
            .insert(
                barq_core::DocumentId::from("golden-primary"),
                vec![1.0, 0.0],
                Some(json!({"sdk": "rust", "mode": "golden"})),
            )
            .await
            .expect("rust insert primary");
        collection
            .insert(
                barq_core::DocumentId::from("golden-secondary"),
                vec![0.0, 1.0],
                Some(json!({"sdk": "rust", "mode": "golden"})),
            )
            .await
            .expect("rust insert secondary");

        let rust_results = collection
            .search(Some(vec![1.0, 0.0]), None, 2, None, None)
            .await
            .expect("rust search");
        assert_eq!(serde_json::Value::Array(rust_results), expected_results);

        std::env::remove_var("BARQ_GRPC_ADDR");
    }

    let expected_json = expected_results.to_string();
    let common_env = [
        ("BARQ_GRPC_ADDR", grpc_host.as_str()),
        ("BARQ_BASE_URL", http_base_url.as_str()),
        ("BARQ_API_KEY", "sdk-key"),
        ("BARQ_EXPECTED_RESULTS", expected_json.as_str()),
    ];

    run_command(
        "python golden client",
        &workspace_root.join("barq-sdk-python"),
        &[
            common_env[0],
            common_env[1],
            common_env[2],
            common_env[3],
            ("BARQ_TEST_COLLECTION", "sdk-python-golden"),
        ],
        "python3",
        ["-m", "unittest", "discover", "-s", "tests", "-p", "test_golden_client.py"],
    )
    .await;

    run_command(
        "go golden client",
        &workspace_root.join("barq-sdk-go"),
        &[
            common_env[0],
            common_env[1],
            common_env[2],
            common_env[3],
            ("BARQ_TEST_COLLECTION", "sdk-go-golden"),
        ],
        "go",
        ["test", "./...", "-run", "TestGoldenClientMatchesRestBaseline", "-count=1"],
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
        "typescript golden client",
        &workspace_root.join("barq-sdk-ts"),
        &[
            common_env[0],
            common_env[1],
            common_env[2],
            common_env[3],
            ("BARQ_TEST_COLLECTION", "sdk-ts-golden"),
        ],
        "node",
        ["--test", "test/golden_client.test.js"],
    )
    .await;

    http_tx.send(()).unwrap();
    grpc_tx.send(()).unwrap();
    http_handle.await.unwrap();
    grpc_handle.await.unwrap();
}
