//! Comprehensive tests for barq-admin crate
//!
//! Tests cover:
//! - API authentication and authorization (roles, permissions)
//! - TLS configuration validation
//! - Admin routes behavior
//! - Error handling

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue};
use barq_admin::auth::{ApiAuth, ApiError, ApiPermission, ApiRole, AuthMethod, TlsConfig};
use barq_core::TenantId;

// ============================================================================
// Authentication Tests
// ============================================================================

#[test]
fn test_api_auth_anonymous_fallback_when_no_keys() {
    let auth = ApiAuth::new();
    let headers = HeaderMap::new();

    // Without require_keys and no keys registered, should allow anonymous
    let result = auth.authenticate(&headers, ApiPermission::Read, None);
    assert!(result.is_ok());

    let identity = result.unwrap();
    assert_eq!(identity.role, ApiRole::Admin);
    assert_eq!(identity.method, AuthMethod::Anonymous);
    assert_eq!(identity.actor, Some("anonymous".to_string()));
}

#[test]
fn test_api_auth_rejects_when_keys_required() {
    let auth = ApiAuth::new().require_keys();
    let headers = HeaderMap::new();

    let result = auth.authenticate(&headers, ApiPermission::Read, None);
    assert!(result.is_err());

    match result.unwrap_err() {
        ApiError::Unauthorized(msg) => assert!(msg.contains("missing api key")),
        _ => panic!("Expected Unauthorized error"),
    }
}

#[test]
fn test_api_auth_with_valid_api_key() {
    let auth = ApiAuth::new().require_keys();
    let tenant = TenantId::new("test-tenant");
    auth.insert("test-key-12345", tenant.clone(), ApiRole::Writer);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("test-key-12345"));

    let result = auth.authenticate(&headers, ApiPermission::Write, None);
    assert!(result.is_ok());

    let identity = result.unwrap();
    assert_eq!(identity.tenant, tenant);
    assert_eq!(identity.role, ApiRole::Writer);
    assert_eq!(identity.method, AuthMethod::ApiKey);
}

#[test]
fn test_api_auth_with_invalid_api_key() {
    let auth = ApiAuth::new().require_keys();
    auth.insert("valid-key", TenantId::new("tenant"), ApiRole::Writer);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("invalid-key"));

    let result = auth.authenticate(&headers, ApiPermission::Write, None);
    assert!(result.is_err());

    match result.unwrap_err() {
        ApiError::Unauthorized(msg) => assert!(msg.contains("invalid api key")),
        _ => panic!("Expected Unauthorized error"),
    }
}

#[test]
fn test_api_auth_tenant_mismatch_in_path() {
    let auth = ApiAuth::new().require_keys();
    let tenant = TenantId::new("tenant-a");
    auth.insert("key-a", tenant.clone(), ApiRole::Writer);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("key-a"));

    // Path tenant differs from key's tenant
    let different_tenant = TenantId::new("tenant-b");
    let result = auth.authenticate(&headers, ApiPermission::Write, Some(&different_tenant));

    assert!(result.is_err());
    match result.unwrap_err() {
        ApiError::Forbidden(msg) => assert!(msg.contains("tenant mismatch")),
        _ => panic!("Expected Forbidden error"),
    }
}

#[test]
fn test_api_auth_tenant_header_mismatch() {
    let auth = ApiAuth::new().require_keys();
    let tenant = TenantId::new("tenant-a");
    auth.insert("key-a", tenant.clone(), ApiRole::Writer);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("key-a"));
    headers.insert("x-tenant-id", HeaderValue::from_static("tenant-b"));

    let result = auth.authenticate(&headers, ApiPermission::Write, None);

    assert!(result.is_err());
    match result.unwrap_err() {
        ApiError::Forbidden(msg) => assert!(msg.contains("tenant header mismatch")),
        _ => panic!("Expected Forbidden error"),
    }
}

// ============================================================================
// Role Permission Tests
// ============================================================================

#[test]
fn test_admin_role_allows_all_permissions() {
    let role = ApiRole::Admin;

    assert!(role.allows(&ApiPermission::Admin));
    assert!(role.allows(&ApiPermission::Ops));
    assert!(role.allows(&ApiPermission::TenantAdmin));
    assert!(role.allows(&ApiPermission::Write));
    assert!(role.allows(&ApiPermission::Read));
}

#[test]
fn test_ops_role_permissions() {
    let role = ApiRole::Ops;

    assert!(!role.allows(&ApiPermission::Admin));
    assert!(role.allows(&ApiPermission::Ops));
    assert!(!role.allows(&ApiPermission::TenantAdmin));
    assert!(!role.allows(&ApiPermission::Write));
    assert!(!role.allows(&ApiPermission::Read));
}

#[test]
fn test_tenant_admin_role_permissions() {
    let role = ApiRole::TenantAdmin;

    assert!(!role.allows(&ApiPermission::Admin));
    assert!(!role.allows(&ApiPermission::Ops));
    assert!(role.allows(&ApiPermission::TenantAdmin));
    assert!(role.allows(&ApiPermission::Write));
    assert!(role.allows(&ApiPermission::Read));
}

#[test]
fn test_writer_role_permissions() {
    let role = ApiRole::Writer;

    assert!(!role.allows(&ApiPermission::Admin));
    assert!(!role.allows(&ApiPermission::Ops));
    assert!(!role.allows(&ApiPermission::TenantAdmin));
    assert!(role.allows(&ApiPermission::Write));
    assert!(role.allows(&ApiPermission::Read));
}

#[test]
fn test_reader_role_permissions() {
    let role = ApiRole::Reader;

    assert!(!role.allows(&ApiPermission::Admin));
    assert!(!role.allows(&ApiPermission::Ops));
    assert!(!role.allows(&ApiPermission::TenantAdmin));
    assert!(!role.allows(&ApiPermission::Write));
    assert!(role.allows(&ApiPermission::Read));
}

#[test]
fn test_insufficient_permissions_rejected() {
    let auth = ApiAuth::new().require_keys();
    auth.insert("reader-key", TenantId::new("tenant"), ApiRole::Reader);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("reader-key"));

    // Reader trying to write should be forbidden
    let result = auth.authenticate(&headers, ApiPermission::Write, None);
    assert!(result.is_err());

    match result.unwrap_err() {
        ApiError::Forbidden(msg) => assert!(msg.contains("insufficient role")),
        _ => panic!("Expected Forbidden error"),
    }
}

// ============================================================================
// TLS Configuration Tests
// ============================================================================

#[test]
fn test_tls_config_validation_missing_cert() {
    let config = TlsConfig::new("/nonexistent/cert.pem", "/nonexistent/key.pem");

    let result = config.validate();
    assert!(result.is_err());

    match result.unwrap_err() {
        ApiError::Tls(msg) => assert!(msg.contains("certificate path does not exist")),
        _ => panic!("Expected Tls error"),
    }
}

#[test]
fn test_tls_config_with_client_ca_validation() {
    let config = TlsConfig::new("/nonexistent/cert.pem", "/nonexistent/key.pem")
        .with_client_ca("/nonexistent/ca.pem");

    // Should fail at cert path first
    let result = config.validate();
    assert!(result.is_err());
}

// ============================================================================
// AuthMethod Tests
// ============================================================================

#[test]
fn test_auth_method_as_str() {
    assert_eq!(AuthMethod::Anonymous.as_str(), "anonymous");
    assert_eq!(AuthMethod::ApiKey.as_str(), "api-key");
    assert_eq!(AuthMethod::Jwt.as_str(), "jwt");
}

// ============================================================================
// API Error Tests
// ============================================================================

#[test]
fn test_api_error_display() {
    let err = ApiError::BadRequest("invalid parameter".to_string());
    assert_eq!(err.to_string(), "bad request: invalid parameter");

    let err = ApiError::Unauthorized("no token".to_string());
    assert_eq!(err.to_string(), "unauthorized: no token");

    let err = ApiError::Forbidden("no access".to_string());
    assert_eq!(err.to_string(), "forbidden: no access");

    let err = ApiError::Tls("invalid cert".to_string());
    assert_eq!(err.to_string(), "tls configuration error: invalid cert");

    let err = ApiError::Redirect("http://leader:8080".to_string());
    assert_eq!(err.to_string(), "redirecting to leader at http://leader:8080");
}

// ============================================================================
// Multiple Key Management Tests
// ============================================================================

#[test]
fn test_multiple_api_keys_different_tenants() {
    let auth = ApiAuth::new().require_keys();

    let tenant_a = TenantId::new("tenant-a");
    let tenant_b = TenantId::new("tenant-b");

    auth.insert("key-a", tenant_a.clone(), ApiRole::Writer);
    auth.insert("key-b", tenant_b.clone(), ApiRole::Reader);

    // Key A should access tenant A
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("key-a"));

    let identity = auth.authenticate(&headers, ApiPermission::Write, None).unwrap();
    assert_eq!(identity.tenant, tenant_a);
    assert_eq!(identity.role, ApiRole::Writer);

    // Key B should access tenant B
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("key-b"));

    let identity = auth.authenticate(&headers, ApiPermission::Read, None).unwrap();
    assert_eq!(identity.tenant, tenant_b);
    assert_eq!(identity.role, ApiRole::Reader);
}

#[test]
fn test_api_key_overwrite() {
    let auth = ApiAuth::new().require_keys();
    let tenant = TenantId::new("tenant");

    // Insert same key twice with different roles
    auth.insert("same-key", tenant.clone(), ApiRole::Reader);
    auth.insert("same-key", tenant.clone(), ApiRole::Admin);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("same-key"));

    // Should have admin role now
    let identity = auth.authenticate(&headers, ApiPermission::Admin, None).unwrap();
    assert_eq!(identity.role, ApiRole::Admin);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_anonymous_uses_path_tenant() {
    let auth = ApiAuth::new(); // No keys, allows anonymous
    let headers = HeaderMap::new();

    let path_tenant = TenantId::new("from-path");
    let result = auth.authenticate(&headers, ApiPermission::Read, Some(&path_tenant));

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.tenant, path_tenant);
}

#[test]
fn test_anonymous_uses_header_tenant_when_no_path() {
    let auth = ApiAuth::new();
    let mut headers = HeaderMap::new();
    headers.insert("x-tenant-id", HeaderValue::from_static("from-header"));

    let result = auth.authenticate(&headers, ApiPermission::Read, None);

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.tenant, TenantId::new("from-header"));
}

#[test]
fn test_anonymous_uses_default_tenant_when_none_specified() {
    let auth = ApiAuth::new();
    let headers = HeaderMap::new();

    let result = auth.authenticate(&headers, ApiPermission::Read, None);

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.tenant, TenantId::default());
}

#[test]
fn test_bearer_token_without_jwt_verifier_fails() {
    let auth = ApiAuth::new().require_keys();

    let mut headers = HeaderMap::new();
    headers.insert("Authorization", HeaderValue::from_static("Bearer some-token"));

    let result = auth.authenticate(&headers, ApiPermission::Read, None);
    assert!(result.is_err());

    match result.unwrap_err() {
        ApiError::Unauthorized(msg) => assert!(msg.contains("jwt auth not configured")),
        _ => panic!("Expected Unauthorized error"),
    }
}

#[test]
fn test_empty_bearer_token_falls_through_to_api_key() {
    let auth = ApiAuth::new().require_keys();
    auth.insert("fallback-key", TenantId::new("tenant"), ApiRole::Writer);

    let mut headers = HeaderMap::new();
    // Empty bearer token should be ignored
    headers.insert("Authorization", HeaderValue::from_static("Bearer "));
    headers.insert("x-api-key", HeaderValue::from_static("fallback-key"));

    let result = auth.authenticate(&headers, ApiPermission::Write, None);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().method, AuthMethod::ApiKey);
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[test]
fn test_concurrent_key_insertion() {
    use std::thread;

    let auth = Arc::new(ApiAuth::new().require_keys());
    let mut handles = vec![];

    // Spawn multiple threads inserting keys
    for i in 0..10 {
        let auth = Arc::clone(&auth);
        handles.push(thread::spawn(move || {
            let key = format!("key-{}", i);
            let tenant = TenantId::new(&format!("tenant-{}", i));
            auth.insert(&key, tenant, ApiRole::Writer);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all keys work
    for i in 0..10 {
        let mut headers = HeaderMap::new();
        let key = format!("key-{}", i);
        headers.insert("x-api-key", HeaderValue::from_str(&key).unwrap());

        let result = auth.authenticate(&headers, ApiPermission::Write, None);
        assert!(result.is_ok(), "Key {} should authenticate", i);
    }
}
