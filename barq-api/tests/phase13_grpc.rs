use barq_api::{AppState, ApiAuth, ClusterConfig, ClusterRouter};
use barq_api::grpc::GrpcService;
use barq_proto::barq::barq_server::BarqServer;
use barq_proto::barq::barq_client::BarqClient;
use barq_proto::barq::{CreateCollectionRequest, InsertDocumentRequest, SearchRequest, HealthRequest};
use barq_storage::Storage;
use tonic::transport::Server;
use std::net::SocketAddr;
use tempfile::tempdir;

async fn start_test_grpc_server() -> (SocketAddr, tokio::task::JoinHandle<()>, tokio::sync::oneshot::Sender<()>) {
    let dir = tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let auth = ApiAuth::new();
    let cluster_config = ClusterConfig::single_node();
    let cluster = ClusterRouter::from_config(cluster_config).unwrap();
    
    let state = AppState::new(storage, auth, cluster);
    let service = GrpcService::new(state);

    // Bind to random port
    let addr: SocketAddr = "[::1]:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(BarqServer::new(service))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    (local_addr, handle, tx)
}

#[tokio::test]
async fn test_grpc_health() {
    let (addr, handle, tx) = start_test_grpc_server().await;
    
    let dst = format!("http://{}", addr);
    let mut client = BarqClient::connect(dst).await.expect("failed to connect");

    let request = tonic::Request::new(HealthRequest {});
    let response = client.health(request).await.expect("health check failed");
    
    assert!(response.get_ref().ok);
    
    tx.send(()).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_grpc_create_insert_search() {
    let (addr, handle, tx) = start_test_grpc_server().await;
    
    let dst = format!("http://{}", addr);
    let mut client = BarqClient::connect(dst).await.expect("failed to connect");

    // 1. Create Collection
    let create_req = CreateCollectionRequest {
        name: "grpc_test".to_string(),
        dimension: 2,
        metric: "Cosine".to_string(),
    };
    let _ = client.create_collection(create_req).await.expect("create collection failed");

    // 2. Insert Document
    let insert_req = InsertDocumentRequest {
        collection: "grpc_test".to_string(),
        id: "doc1".to_string(),
        vector: vec![1.0, 0.0],
        payload_json: "{\"test\": \"ok\"}".to_string(),
    };
    let _ = client.insert_document(insert_req).await.expect("insert failed");

    // 3. Search
    let search_req = SearchRequest {
        collection: "grpc_test".to_string(),
        vector: vec![1.0, 0.0],
        top_k: 1,
    };
    let response = client.search(search_req).await.expect("search failed");
    let results = response.into_inner().results;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "doc1");
    // Verify score roughly 1.0 (Cosine similarity of identical vectors)
    assert!((results[0].score - 1.0).abs() < 0.0001);

    tx.send(()).unwrap();
    handle.await.unwrap();
}
