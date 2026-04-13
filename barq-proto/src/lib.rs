pub mod barq {
    tonic::include_proto!("barq");
}

#[cfg(test)]
mod tests {
    use super::barq::{
        InsertRequest, InsertResponse, SearchRequest, StatusRequest, StatusResponse,
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
        };
        assert_eq!(insert.collection, "docs");

        let response = InsertResponse { success: true };
        assert!(response.success);
    }

    #[test]
    fn generated_search_request_is_available() {
        let request = SearchRequest {
            collection: "docs".to_string(),
            vector: vec![1.0, 0.0],
            top_k: 3,
        };
        assert_eq!(request.top_k, 3);
    }
}
