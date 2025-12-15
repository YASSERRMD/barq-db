use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use barq_core::TenantId;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;
use axum_server::tls_rustls::RustlsConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ApiRole {
    Admin,
    Ops,
    TenantAdmin,
    Writer,
    Reader,
}

impl ApiRole {
    pub fn allows(&self, required: &ApiPermission) -> bool {
        match (self, required) {
            (ApiRole::Admin, _) => true,
            (ApiRole::Ops, ApiPermission::Ops) => true,
            (ApiRole::TenantAdmin, ApiPermission::TenantAdmin)
            | (ApiRole::TenantAdmin, ApiPermission::Write)
            | (ApiRole::TenantAdmin, ApiPermission::Read) => true,
            (ApiRole::Writer, ApiPermission::Write) | (ApiRole::Writer, ApiPermission::Read) => {
                true
            }
            (ApiRole::Reader, ApiPermission::Read) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiPermission {
    Admin,
    Ops,
    TenantAdmin,
    Write,
    Read,
}

#[derive(Debug, Clone)]
struct ApiKey {
    tenant: TenantId,
    role: ApiRole,
}

#[derive(Clone, Default)]
pub struct ApiAuth {
    keys: Arc<RwLock<HashMap<String, ApiKey>>>,
    require_keys: bool,
    jwt_verifier: Option<Arc<dyn JwtVerifier>>,
}

impl std::fmt::Debug for ApiAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiAuth")
            .field("keys", &"<redacted>")
            .field("require_keys", &self.require_keys)
            .field("jwt_verifier", &self.jwt_verifier.is_some())
            .finish()
    }
}

impl ApiAuth {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn require_keys(mut self) -> Self {
        self.require_keys = true;
        self
    }

    pub fn insert(&self, key: impl Into<String>, tenant: TenantId, role: ApiRole) {
        let mut guard = self.keys.write().expect("auth lock poisoned");
        guard.insert(key.into(), ApiKey { tenant, role });
    }

    pub fn with_jwt_verifier(mut self, verifier: Arc<dyn JwtVerifier>) -> Self {
        self.jwt_verifier = Some(verifier);
        self
    }

    pub fn authenticate(
        &self,
        headers: &HeaderMap,
        required: ApiPermission,
        path_tenant: Option<&TenantId>,
    ) -> Result<ApiIdentity, ApiError> {
        let guard = self.keys.read().expect("auth lock poisoned");
        let fallback_allowed =
            !self.require_keys && guard.is_empty() && self.jwt_verifier.is_none();
        let requested_header_tenant = tenant_header(headers);
        if fallback_allowed {
            let tenant = path_tenant
                .cloned()
                .or_else(|| requested_header_tenant.clone())
                .unwrap_or_default();
            return Ok(ApiIdentity {
                tenant,
                role: ApiRole::Admin,
                actor: Some("anonymous".to_string()),
                method: AuthMethod::Anonymous,
            });
        }

        if let Some(token) = bearer_token(headers) {
            let verifier = self
                .jwt_verifier
                .as_ref()
                .ok_or_else(|| ApiError::Unauthorized("jwt auth not configured".into()))?;
            let claims = verifier.verify(token)?;
            enforce_tenant_constraints(
                path_tenant,
                requested_header_tenant.as_ref(),
                &claims.tenant,
            )?;
            if !claims.role.allows(&required) {
                return Err(ApiError::Forbidden("insufficient role".into()));
            }
            return Ok(ApiIdentity {
                tenant: claims.tenant,
                role: claims.role,
                actor: claims.subject,
                method: AuthMethod::Jwt,
            });
        }

        let api_key = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .ok_or(ApiError::Unauthorized("missing api key".into()))?;

        let record = guard
            .get(api_key)
            .ok_or_else(|| ApiError::Unauthorized("invalid api key".into()))?;
        enforce_tenant_constraints(
            path_tenant,
            requested_header_tenant.as_ref(),
            &record.tenant,
        )?;
        if !record.role.allows(&required) {
            return Err(ApiError::Forbidden("insufficient role".into()));
        }

        Ok(ApiIdentity {
            tenant: record.tenant.clone(),
            role: record.role.clone(),
            actor: Some(redact_key(api_key)),
            method: AuthMethod::ApiKey,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JwtClaims {
    pub tenant: TenantId,
    pub role: ApiRole,
    pub subject: Option<String>,
}

pub trait JwtVerifier: Send + Sync {
    fn verify(&self, token: &str) -> Result<JwtClaims, ApiError>;
}

#[derive(Debug, Clone)]
pub struct ApiIdentity {
    pub tenant: TenantId,
    pub role: ApiRole,
    pub actor: Option<String>,
    pub method: AuthMethod,
}

fn tenant_header(headers: &HeaderMap) -> Option<TenantId> {
    headers
        .get("x-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(TenantId::new)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Anonymous,
    ApiKey,
    Jwt,
}

impl AuthMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthMethod::Anonymous => "anonymous",
            AuthMethod::ApiKey => "api-key",
            AuthMethod::Jwt => "jwt",
        }
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn enforce_tenant_constraints(
    path_tenant: Option<&TenantId>,
    header_tenant: Option<&TenantId>,
    identity_tenant: &TenantId,
) -> Result<(), ApiError> {
    if let Some(path) = path_tenant {
        if path != identity_tenant {
            return Err(ApiError::Forbidden("tenant mismatch".into()));
        }
    }

    if let Some(header) = header_tenant {
        if header != identity_tenant {
            return Err(ApiError::Forbidden("tenant header mismatch".into()));
        }
    }

    Ok(())
}

fn redact_key(key: &str) -> String {
    let len = key.chars().count();
    if len <= 4 {
        return "****".to_string();
    }
    let prefix: String = key.chars().take(4).collect();
    format!("{}***", prefix)
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,
}

impl TlsConfig {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            client_ca_path: None,
        }
    }

    pub fn with_client_ca(mut self, ca_path: impl Into<PathBuf>) -> Self {
        self.client_ca_path = Some(ca_path.into());
        self
    }

    pub fn validate(&self) -> Result<(), ApiError> {
        if !self.cert_path.exists() {
            return Err(ApiError::Tls("certificate path does not exist".into()));
        }
        if !self.key_path.exists() {
            return Err(ApiError::Tls("private key path does not exist".into()));
        }
        if let Some(ca) = &self.client_ca_path {
            if !ca.exists() {
                return Err(ApiError::Tls("client CA path does not exist".into()));
            }
        }
        Ok(())
    }

    fn load_certificates(&self) -> Result<Vec<CertificateDer<'static>>, ApiError> {
        let cert_file = File::open(&self.cert_path)
            .map_err(|err| ApiError::Tls(format!("failed to open cert: {}", err)))?;
        let mut reader = BufReader::new(cert_file);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| ApiError::Tls(format!("failed to parse cert: {}", err)))?;
        if certs.is_empty() {
            return Err(ApiError::Tls("no certificates found".into()));
        }
        Ok(certs)
    }

    fn load_private_key(&self) -> Result<PrivateKeyDer<'static>, ApiError> {
        let key_file = File::open(&self.key_path)
            .map_err(|err| ApiError::Tls(format!("failed to open key: {}", err)))?;
        let mut reader = BufReader::new(key_file);
        rustls_pemfile::private_key(&mut reader)
            .map_err(|err| ApiError::Tls(format!("failed to parse key: {}", err)))?
            .ok_or_else(|| ApiError::Tls("no private key found".into()))
    }

    fn load_client_ca(&self) -> Result<RootCertStore, ApiError> {
        let mut store = RootCertStore::empty();
        if let Some(path) = &self.client_ca_path {
            let ca_file = File::open(path)
                .map_err(|err| ApiError::Tls(format!("failed to open client CA: {}", err)))?;
            let mut reader = BufReader::new(ca_file);
            let certs = rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| ApiError::Tls(format!("failed to parse client CA: {}", err)))?;
            for cert in certs {
                store
                    .add(cert)
                    .map_err(|err| ApiError::Tls(format!("invalid client CA: {}", err)))?;
            }
        }
        Ok(store)
    }

    pub fn build_server_config(&self) -> Result<rustls::ServerConfig, ApiError> {
        self.validate()?;
        let certs = self.load_certificates()?;
        let key = self.load_private_key()?;

        let builder = rustls::ServerConfig::builder();
        let server_config = if self.client_ca_path.is_some() {
            let client_ca = self.load_client_ca()?;
            let verifier = WebPkiClientVerifier::builder(client_ca.into())
                .build()
                .map_err(|err| ApiError::Tls(format!("invalid client verifier: {}", err)))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|err| ApiError::Tls(format!("invalid tls config: {}", err)))?
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|err| ApiError::Tls(format!("invalid tls config: {}", err)))?
        };

        Ok(server_config)
    }

    // Moved this to barq-admin as well, assuming axum_server/RustlsConfig not needed or passed
    // Actually RustlsConfig is from axum_server.
    // I need to decide if TlsConfig should generate RustlsConfig.
    // For now, I'll keep the struct here.
    pub async fn into_rustls_config(&self) -> Result<RustlsConfig, ApiError> {
        let server_config = self.build_server_config()?;
        Ok(RustlsConfig::from_config(Arc::new(server_config)))
    }
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("storage error: {0}")]
    Storage(#[from] barq_storage::StorageError),

    #[error("cluster error: {0}")]
    Cluster(#[from] barq_cluster::ClusterError),

    #[error("document id error: {0}")]
    DocumentId(#[from] barq_index::DocumentIdError),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("tls configuration error: {0}")]
    Tls(String),

    #[error("redirecting to leader at {0}")]
    Redirect(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::Storage(barq_storage::StorageError::Catalog(_))
            | ApiError::DocumentId(_)
            | ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::Redirect(_) => StatusCode::TEMPORARY_REDIRECT,
            ApiError::Cluster(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Tls(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Storage(barq_storage::StorageError::QuotaExceeded { .. }) => {
                StatusCode::TOO_MANY_REQUESTS
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut response = (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response();

        if let ApiError::Redirect(address) = &self {
            if let Ok(header_value) = axum::http::HeaderValue::from_str(address) {
                response
                    .headers_mut()
                    .insert(axum::http::header::LOCATION, header_value);
            }
        }

        response
    }
}
