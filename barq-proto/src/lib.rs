pub mod barq {
    tonic::include_proto!("barq");
}

#[cfg(test)]
mod tests {
    use super::barq::{
        ClusterMode, CollectionSegmentInfo, CollectionSegmentStateSample, Consistency,
        GetClusterStatusRequest, GetClusterStatusResponse, GetInsertStatusRequest,
        GetInsertStatusResponse, GetMetricsRequest, GetMetricsResponse, GetSegmentInfoRequest,
        GetSegmentInfoResponse, IndexState, InsertAsyncResponse, InsertOptions, InsertRequest,
        InsertResponse, InsertStatusState, MetricDefinition, MetricKind, SearchOptions,
        SearchRequest, SegmentCount, SegmentState, StatusRequest, StatusResponse, StorageMetrics,
        WriteDurability,
    };

    #[test]
    fn canonical_proto_messages_compile() {
        let _ = StatusRequest {};
        let status = StatusResponse {
            ok: true,
            version: "0.1.0".to_string(),
        };
        assert!(status.ok);

        let insert = InsertRequest {
            collection: "docs".to_string(),
            id: "1".to_string(),
            vector: vec![1.0, 0.0],
            payload_json: "{}".to_string(),
            options: None,
        };
        assert_eq!(insert.collection, "docs");

        let response = InsertResponse { success: true };
        assert!(response.success);

        let async_response = InsertAsyncResponse {
            accepted: true,
            request_id: "ingest-1".to_string(),
        };
        assert!(async_response.accepted);
        assert_eq!(async_response.request_id, "ingest-1");
    }

    #[test]
    fn generated_search_request_is_available() {
        let request = SearchRequest {
            collection: "docs".to_string(),
            vector: vec![1.0, 0.0],
            top_k: 3,
            options: None,
        };
        assert_eq!(request.top_k, 3);
    }

    #[test]
    fn advanced_option_fields_are_backward_compatible() {
        let insert = InsertRequest {
            collection: "docs".to_string(),
            id: "legacy".to_string(),
            vector: vec![1.0, 0.0],
            payload_json: "{}".to_string(),
            options: None,
        };
        assert!(insert.options.is_none());

        let search = SearchRequest {
            collection: "docs".to_string(),
            vector: vec![1.0, 0.0],
            top_k: 5,
            options: None,
        };
        assert!(search.options.is_none());
    }

    #[test]
    fn advanced_option_fields_default_correctly() {
        let insert_options = InsertOptions {
            wait_for_commit: false,
        };
        assert!(!insert_options.wait_for_commit);

        let search_options = SearchOptions {
            consistency: Consistency::Unspecified as i32,
            allow_fallback: false,
        };
        assert_eq!(search_options.consistency, Consistency::Unspecified as i32);
        assert!(!search_options.allow_fallback);
    }

    #[test]
    fn async_insert_response_exposes_request_handle() {
        let response = InsertAsyncResponse {
            accepted: true,
            request_id: "ingest-42".to_string(),
        };

        assert!(response.accepted);
        assert_eq!(response.request_id, "ingest-42");
    }

    #[test]
    fn insert_status_messages_compile() {
        let request = GetInsertStatusRequest {
            request_id: "ingest-7".to_string(),
        };
        assert_eq!(request.request_id, "ingest-7");

        let response = GetInsertStatusResponse {
            request_id: "ingest-7".to_string(),
            state: InsertStatusState::Succeeded as i32,
            error_message: String::new(),
        };
        assert_eq!(response.state, InsertStatusState::Succeeded as i32);
        assert!(response.error_message.is_empty());
    }

    #[test]
    fn observability_messages_compile() {
        let metrics_request = GetMetricsRequest {};
        let cluster_request = GetClusterStatusRequest {};
        let segment_request = GetSegmentInfoRequest {
            collection: "docs".to_string(),
        };

        let metrics_response = GetMetricsResponse {
            definitions: vec![MetricDefinition {
                name: "search_requests_total".to_string(),
                kind: MetricKind::Counter as i32,
                description: "Total searches".to_string(),
                unit: String::new(),
                labels: vec!["type".to_string()],
            }],
            storage: Some(StorageMetrics {
                refresh_count: 1,
                total_resident_vector_memory_bytes: 128,
                wal_appends_total: 2,
                wal_bytes_written_total: 64,
                compactions_total: 0,
                tenant_memory_bytes: Vec::new(),
                collection_memory_bytes: Vec::new(),
                collection_wal: Vec::new(),
                collection_segment_files: Vec::new(),
                collection_segment_states: vec![CollectionSegmentStateSample {
                    tenant: "default".to_string(),
                    collection: "docs".to_string(),
                    state: SegmentState::Sealed as i32,
                    active: true,
                }],
            }),
        };

        let cluster_response = GetClusterStatusResponse {
            node_id: "local".to_string(),
            mode: ClusterMode::SingleNode as i32,
            write_durability: WriteDurability::NodeLocal as i32,
            shard_count: 1,
            node_count: 1,
        };

        let segment_response = GetSegmentInfoResponse {
            collections: vec![CollectionSegmentInfo {
                tenant: "default".to_string(),
                collection: "docs".to_string(),
                current_state: SegmentState::Growing as i32,
                index_state: IndexState::Ready as i32,
                segment_counts: vec![SegmentCount {
                    state: SegmentState::Sealed as i32,
                    count: 1,
                }],
            }],
        };

        let _ = metrics_request;
        let _ = cluster_request;
        assert_eq!(segment_request.collection, "docs");
        assert_eq!(metrics_response.definitions.len(), 1);
        assert_eq!(cluster_response.mode, ClusterMode::SingleNode as i32);
        assert_eq!(segment_response.collections[0].segment_counts[0].count, 1);
    }
}
