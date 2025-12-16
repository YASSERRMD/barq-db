use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
pub use barq_core::{CollectionSchema, DistanceMetric, DocumentId, Filter, HybridWeights};

#[derive(Debug, thiserror::Error)]
pub enum BarqError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: StatusCode, message: String },
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, BarqError>;

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
        let url = format!("{}/health", self.base_url);
        let res = self.client.get(&url).send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(BarqError::Api {
                status: res.status(),
                message: res.text().await?,
            })
        }
    }

    pub async fn create_collection(
        &self, 
        name: &str, 
        dimension: usize, 
        metric: DistanceMetric,
        index: Option<serde_json::Value>, // Using Value for flexibility with HNSW/IVF params
        text_fields: Option<Vec<TextFieldRequest>>,
    ) -> Result<()> {
        let url = format!("{}/collections", self.base_url);
        let payload = json!({
            "name": name,
            "dimension": dimension,
            "metric": metric,
            "index": index,
            "text_fields": text_fields.unwrap_or_default()
        });

        let res = self.client.post(&url)
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
    pub async fn insert(&self, id: impl Into<DocumentId>, vector: Vec<f32>, payload: Option<serde_json::Value>) -> Result<()> {
        let url = format!("{}/collections/{}/documents", self.client.base_url, self.name);
        
        let id_obj = id.into();
        let id_val = match id_obj {
            DocumentId::U64(v) => json!(v),
            DocumentId::Str(s) => json!(s),
        };

        let json_payload = json!({
            "id": id_val,
            "vector": vector,
            "payload": payload
        });

        let res = self.client.client.post(&url)
            .header("x-api-key", &self.client.api_key)
            .json(&json_payload)
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

    pub async fn search(
        &self, 
        vector: Option<Vec<f32>>, 
        query: Option<String>, 
        top_k: usize, 
        filter: Option<Filter>,
        weights: Option<HybridWeights>
    ) -> Result<Vec<serde_json::Value>> {
        let mut url = format!("{}/collections/{}/search", self.client.base_url, self.name);
        
        // Determine endpoint based on what's provided
        // Logic mirroring the API routing preference
        if vector.is_some() && query.is_some() {
            url.push_str("/hybrid");
        } else if query.is_some() {
            url.push_str("/text");
        } else {
            // Default vector search
        }

        let payload = json!({
            "vector": vector,
            "query": query,
            "top_k": top_k,
            "filter": filter,
            "weights": weights
        });

        let res = self.client.client.post(&url)
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextFieldRequest {
    pub name: String,
    pub indexed: bool,
    pub required: bool,
}
