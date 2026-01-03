use utoipa::{
    openapi::{
        schema::{Schema, SchemaType, ArrayBuilder},
        ObjectBuilder, RefOr,
    },
    ToSchema,
};

// Wrapper types or manual schema implementations

pub struct UtoipaDistanceMetric;
impl<'s> ToSchema<'s> for UtoipaDistanceMetric {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "DistanceMetric",
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .enum_values(Some(vec!["L2", "Cosine", "Dot"]))
                .into(),
        )
    }
}

pub struct UtoipaIndexType;
impl<'s> ToSchema<'s> for UtoipaIndexType {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "IndexType",
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .description(Some("Type of vector index (e.g., Flat, HNSW, IVF)"))
                .into(),
        )
    }
}

pub struct UtoipaApiRole;
impl<'s> ToSchema<'s> for UtoipaApiRole {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "ApiRole",
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .enum_values(Some(vec!["Admin", "TenantAdmin", "Read", "Write", "Ops"]))
                .into(),
        )
    }
}

// Schemas for complex external types

pub struct UtoipaDocument;
impl<'s> ToSchema<'s> for UtoipaDocument {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "Document",
            ObjectBuilder::new()
                .schema_type(SchemaType::Object)
                .property(
                    "id",
                    ObjectBuilder::new().schema_type(SchemaType::String).description(Some("Document ID")),
                )
                .property(
                    "vector",
                     ArrayBuilder::new()
                        .items(ObjectBuilder::new().schema_type(SchemaType::Number)),
                )
                .property(
                    "payload",
                    ObjectBuilder::new().schema_type(SchemaType::Object).description(Some("JSON payload")),
                )
                .required("id")
                .required("vector")
                .into(),
        )
    }
}

pub struct UtoipaSearchResult;
impl<'s> ToSchema<'s> for UtoipaSearchResult {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "SearchResult",
            ObjectBuilder::new()
                .schema_type(SchemaType::Object)
                .property(
                    "id",
                     ObjectBuilder::new().schema_type(SchemaType::String)
                )
                .property(
                    "score",
                    ObjectBuilder::new().schema_type(SchemaType::Number),
                )
                .required("id")
                .required("score")
                .into(),
        )
    }
}

pub struct UtoipaHybridSearchResult;
impl<'s> ToSchema<'s> for UtoipaHybridSearchResult {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "HybridSearchResult",
             ObjectBuilder::new()
                .schema_type(SchemaType::Object)
                .property("id", ObjectBuilder::new().schema_type(SchemaType::String))
                .property("score", ObjectBuilder::new().schema_type(SchemaType::Number))
                .property("vector_score", ObjectBuilder::new().schema_type(SchemaType::Number))
                .property("bm25_score", ObjectBuilder::new().schema_type(SchemaType::Number))
                .required("id")
                .required("score")
                .into(),
        )
    }
}

// Mirror structs for response bodies
#[derive(ToSchema)]
#[schema(as = TenantUsageReport)]
pub struct UtoipaTenantUsageReport {
    pub tenant: String,
    pub collections: usize,
    pub documents: usize,
    pub disk_bytes: u64,
    pub memory_bytes: u64,
    pub current_qps: u32,
    pub quota: UtoipaTenantQuota,
}

#[derive(ToSchema)]
#[schema(as = TenantQuota)]
pub struct UtoipaTenantQuota {
    pub max_collections: Option<usize>,
    pub max_disk_bytes: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_qps: Option<u32>,
}
