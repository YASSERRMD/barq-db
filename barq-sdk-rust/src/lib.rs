pub use barq_core::{
    CollectionSchema, DistanceMetric, DocumentId, Filter, HybridWeights, PayloadValue,
};
use barq_proto::barq::barq_client::BarqClient as TonicBarqClient;
use barq_proto::barq::{CreateCollectionRequest, InsertRequest, SearchRequest, StatusRequest};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

#[derive(Debug, thiserror::Error)]
pub enum BarqError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: StatusCode, message: String },
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

pub type Result<T> = std::result::Result<T, BarqError>;

fn ensure_supported_api_version() -> Result<()> {
    let version = env::var("API_VERSION").unwrap_or_else(|_| "v1".to_string());
    if version == "v1" {
        Ok(())
    } else {
        Err(BarqError::Api {
            status: StatusCode::BAD_REQUEST,
            message: format!("unsupported API_VERSION: {version}"),
        })
    }
}

fn grpc_endpoint(base_url: &str) -> Result<String> {
    if let Ok(value) = env::var("BARQ_GRPC_ADDR") {
        if value.contains("://") {
            return Ok(value);
        }
        return Ok(format!("http://{}", value));
    }

    let mut url = reqwest::Url::parse(base_url).map_err(|error| BarqError::Api {
        status: StatusCode::BAD_REQUEST,
        message: format!("invalid base url: {error}"),
    })?;
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    url.set_port(Some(50051)).map_err(|_| BarqError::Api {
        status: StatusCode::BAD_REQUEST,
        message: "invalid grpc port".to_string(),
    })?;
    Ok(url.to_string())
}

fn compat_document_id_json(id: &str) -> serde_json::Value {
    if let Ok(value) = id.parse::<u64>() {
        json!({ "U64": value })
    } else {
        json!({ "Str": id })
    }
}

#[derive(Clone, Debug)]
pub struct BarqClient {
    base_url: String,
    api_key: String,
    client: Client,
}

impl BarqClient {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    pub fn collection(&self, name: &str) -> Collection {
        Collection {
            client: self.clone(),
            name: name.to_string(),
        }
    }

    pub async fn health(&self) -> Result<()> {
        ensure_supported_api_version()?;
        let mut client = BarqGrpcClient::connect_with_api_key(
            grpc_endpoint(&self.base_url)?,
            self.api_key.clone(),
        )
        .await?;
        if client.status().await? {
            Ok(())
        } else {
            Err(BarqError::Api {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "grpc status returned not ok".to_string(),
            })
        }
    }

    pub async fn create_collection(
        &self,
        name: &str,
        dimension: usize,
        metric: DistanceMetric,
        index: Option<serde_json::Value>,
        text_fields: Option<Vec<TextFieldRequest>>,
    ) -> Result<()> {
        ensure_supported_api_version()?;
        if index.is_none() && text_fields.as_ref().map_or(true, Vec::is_empty) {
            let mut client = BarqGrpcClient::connect_with_api_key(
                grpc_endpoint(&self.base_url)?,
                self.api_key.clone(),
            )
            .await?;
            return client
                .create_collection(name, dimension as u32, metric)
                .await;
        }

        let url = format!("{}/collections", self.base_url);
        let payload = json!({
            "name": name,
            "dimension": dimension,
            "metric": metric,
            "index": index,
            "text_fields": text_fields.unwrap_or_default()
        });

        let res = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .json(&payload)
            .send()
            .await?;

        if res.status().is_success() {
            Ok(())
        } else {
            Err(BarqError::Api {
                status: res.status(),
                message: res.text().await?,
            })
        }
    }
}

#[derive(Clone, Debug)]
pub struct Collection {
    client: BarqClient,
    name: String,
}

impl Collection {
    pub async fn insert(
        &self,
        id: impl Into<DocumentId>,
        vector: Vec<f32>,
        payload: Option<serde_json::Value>,
    ) -> Result<()> {
        ensure_supported_api_version()?;
        let id_obj = id.into();
        let mut client = BarqGrpcClient::connect_with_api_key(
            grpc_endpoint(&self.client.base_url)?,
            self.client.api_key.clone(),
        )
        .await?;
        client
            .insert(
                &self.name,
                id_obj,
                vector,
                payload.unwrap_or_else(|| json!({})),
            )
            .await
    }

    pub async fn search(
        &self,
        vector: Option<Vec<f32>>,
        query: Option<String>,
        top_k: usize,
        filter: Option<Filter>,
        weights: Option<HybridWeights>,
    ) -> Result<Vec<serde_json::Value>> {
        ensure_supported_api_version()?;
        if vector.is_some() && query.is_none() && filter.is_none() && weights.is_none() {
            let mut client = BarqGrpcClient::connect_with_api_key(
                grpc_endpoint(&self.client.base_url)?,
                self.client.api_key.clone(),
            )
            .await?;
            let results = client
                .search(&self.name, vector.unwrap_or_default(), top_k as u32)
                .await?;
            return Ok(results
                .into_iter()
                .map(|result| {
                    json!({
                        "id": compat_document_id_json(result["id"].as_str().unwrap_or_default()),
                        "score": result["score"],
                    })
                })
                .collect());
        }

        let mut url = format!("{}/collections/{}/search", self.client.base_url, self.name);

        if vector.is_some() && query.is_some() {
            url.push_str("/hybrid");
        } else if query.is_some() {
            url.push_str("/text");
        }

        let payload = json!({
            "vector": vector,
            "query": query,
            "top_k": top_k,
            "filter": filter,
            "weights": weights
        });

        let res = self
            .client
            .client
            .post(&url)
            .header("x-api-key", &self.client.api_key)
            .json(&payload)
            .send()
            .await?;

        if res.status().is_success() {
            let body: serde_json::Value = res.json().await?;
            let mut results = Vec::new();
            if let Some(arr) = body["results"].as_array() {
                results = arr.clone();
            }
            Ok(results)
        } else {
            Err(BarqError::Api {
                status: res.status(),
                message: res.text().await?,
            })
        }
    }

    pub async fn batch_search(
        &self,
        queries: Vec<SearchQuery>,
        top_k: usize,
    ) -> Result<Vec<Vec<serde_json::Value>>> {
        ensure_supported_api_version()?;
        let url = format!(
            "{}/collections/{}/batch_search",
            self.client.base_url, self.name
        );

        let payload = BatchSearchRequest { queries, top_k };

        let res = self
            .client
            .client
            .post(&url)
            .header("x-api-key", &self.client.api_key)
            .json(&payload)
            .send()
            .await?;

        if res.status().is_success() {
            let body: serde_json::Value = res.json().await?;
            let mut results = Vec::new();
            if let Some(arr) = body["results"].as_array() {
                for batch in arr {
                    if let Some(hits) = batch["hits"].as_array() {
                        results.push(hits.clone());
                    } else {
                        results.push(Vec::new());
                    }
                }
            }
            Ok(results)
        } else {
            Err(BarqError::Api {
                status: res.status(),
                message: res.text().await?,
            })
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextFieldRequest {
    pub name: String,
    pub indexed: bool,
    pub required: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchQuery {
    pub vector: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSearchRequest {
    pub queries: Vec<SearchQuery>,
    pub top_k: usize,
}

// gRPC Client Implementation
#[derive(Clone, Debug)]
pub struct BarqGrpcClient {
    client: TonicBarqClient<Channel>,
    api_key: Option<String>,
    tenant: Option<String>,
}

impl BarqGrpcClient {
    pub async fn connect(dst: String) -> Result<Self> {
        Self::connect_with_metadata(dst, None, None).await
    }

    pub async fn connect_with_api_key(dst: String, api_key: impl Into<String>) -> Result<Self> {
        Self::connect_with_metadata(dst, Some(api_key.into()), None).await
    }

    pub async fn connect_with_metadata(
        dst: String,
        api_key: Option<String>,
        tenant: Option<String>,
    ) -> Result<Self> {
        let client = TonicBarqClient::connect(dst).await?;
        Ok(Self {
            client,
            api_key,
            tenant,
        })
    }

    fn request<T>(&self, message: T) -> Result<Request<T>> {
        let mut request = Request::new(message);
        if let Some(api_key) = &self.api_key {
            let value =
                MetadataValue::try_from(api_key.as_str()).map_err(|error| BarqError::Api {
                    status: StatusCode::BAD_REQUEST,
                    message: format!("invalid api key metadata: {error}"),
                })?;
            request.metadata_mut().insert("x-api-key", value);
        }
        if let Some(tenant) = &self.tenant {
            let value =
                MetadataValue::try_from(tenant.as_str()).map_err(|error| BarqError::Api {
                    status: StatusCode::BAD_REQUEST,
                    message: format!("invalid tenant metadata: {error}"),
                })?;
            request.metadata_mut().insert("x-tenant-id", value);
        }
        Ok(request)
    }

    pub async fn status(&mut self) -> Result<bool> {
        let response = self.client.status(self.request(StatusRequest {})?).await?;
        Ok(response.into_inner().ok)
    }

    pub async fn health(&mut self) -> Result<bool> {
        self.status().await
    }

    pub async fn create_collection(
        &mut self,
        name: &str,
        dimension: u32,
        metric: DistanceMetric,
    ) -> Result<()> {
        let metric_str = match metric {
            DistanceMetric::Cosine => "Cosine",
            DistanceMetric::Dot => "Dot",
            DistanceMetric::L2 => "L2",
        };

        self.client
            .create_collection(self.request(CreateCollectionRequest {
                name: name.to_string(),
                dimension: dimension,
                metric: metric_str.to_string(),
            })?)
            .await?;
        Ok(())
    }

    pub async fn insert(
        &mut self,
        collection: &str,
        id: impl Into<DocumentId>,
        vector: Vec<f32>,
        payload: serde_json::Value,
    ) -> Result<()> {
        let id_str = match id.into() {
            DocumentId::U64(v) => v.to_string(),
            DocumentId::Str(s) => s,
        };

        self.client
            .insert(self.request(InsertRequest {
                collection: collection.to_string(),
                id: id_str,
                vector,
                payload_json: payload.to_string(),
            })?)
            .await?;
        Ok(())
    }

    pub async fn insert_document(
        &mut self,
        collection: &str,
        id: impl Into<DocumentId>,
        vector: Vec<f32>,
        payload: serde_json::Value,
    ) -> Result<()> {
        self.insert(collection, id, vector, payload).await
    }

    pub async fn search(
        &mut self,
        collection: &str,
        vector: Vec<f32>,
        top_k: u32,
    ) -> Result<Vec<serde_json::Value>> {
        // Simplification: return basic result
        let res = self
            .client
            .search(self.request(SearchRequest {
                collection: collection.to_string(),
                vector,
                top_k,
            })?)
            .await?;

        let results = res.into_inner().results;
        let mut json_results = Vec::new();

        for r in results {
            json_results.push(json!({
                 "id": r.id,
                 "score": r.score,
                 "payload": serde_json::from_str::<serde_json::Value>(&r.payload_json).unwrap_or(json!({}))
             }));
        }

        Ok(json_results)
    }

    pub async fn batch_search(
        &mut self,
        collection: &str,
        queries: Vec<SearchQuery>,
        top_k: u32,
    ) -> Result<Vec<Vec<serde_json::Value>>> {
        let proto_queries = queries
            .into_iter()
            .map(|q| barq_proto::barq::SearchQuery {
                vector: q.vector,
                filter_json: q
                    .filter
                    .map(|f| serde_json::to_string(&f).unwrap_or_default())
                    .unwrap_or_default(),
            })
            .collect();

        let req = barq_proto::barq::BatchSearchRequest {
            collection: collection.to_string(),
            queries: proto_queries,
            top_k,
        };

        let response = self.client.batch_search(self.request(req)?).await?;
        let batch_results = response.into_inner().results;

        let mut final_results = Vec::new();
        for batch in batch_results {
            let mut hits = Vec::new();
            for hit in batch.hits {
                hits.push(json!({
                    "id": hit.id,
                    "score": hit.score,
                    "payload": serde_json::from_str::<serde_json::Value>(&hit.payload_json).unwrap_or(json!({}))
                }));
            }
            final_results.push(hits);
        }

        Ok(final_results)
    }
}
