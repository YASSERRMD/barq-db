
#[derive(OpenApi)]
#[openapi(
    paths(
        health,
        create_collection,
        drop_collection,
        insert_document,
        get_document,
        delete_document,
        search_collection,
        batch_search_collection,
        search_text_collection,
        search_hybrid_collection,
        explain_hybrid_collection,
        rebuild_collection_index,
        tenant_usage,
        set_tenant_quota,
        register_api_key
    ),
    components(
        schemas(
            CreateCollectionRequest, TextFieldRequest, DocumentIdInput, InsertDocumentRequest,
            SearchRequest, SearchResponse, SearchQuery, BatchSearchRequest, BatchSearchResults, BatchSearchResponse,
            TextSearchRequest, TextSearchResponse, HybridSearchRequest, HybridSearchResponse,
            ExplainRequest, ExplainResponse, GetDocumentResponse, RebuildIndexRequest,
            TenantQuotaRequest, ApiKeyRequest,
            crate::openapi::UtoipaTenantUsageReport, crate::openapi::UtoipaTenantQuota,
            crate::openapi::UtoipaDistanceMetric, crate::openapi::UtoipaIndexType, crate::openapi::UtoipaApiRole,
            crate::openapi::UtoipaDocument, crate::openapi::UtoipaSearchResult, crate::openapi::UtoipaHybridSearchResult,
        )
    ),
    tags(
        (name = "collections", description = "Collection management endpoints"),
        (name = "documents", description = "Document operations"),
        (name = "search", description = "Search endpoints"),
        (name = "tenants", description = "Multi-tenancy management")
    )
)]
pub struct ApiDoc;
