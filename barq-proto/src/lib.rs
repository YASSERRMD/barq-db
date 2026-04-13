pub mod barq {
    tonic::include_proto!("barq");
}

#[cfg(test)]
mod tests {
    use super::barq::{
        Consistency, InsertAsyncResponse, InsertOptions, InsertRequest, InsertResponse,
        SearchOptions, SearchRequest, StatusRequest, StatusResponse,
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
}
