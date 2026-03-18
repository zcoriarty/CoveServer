//! Supabase Storage access helpers.

use cove_common::error::{CoveError, CoveResult};
use reqwest::{header::CONTENT_TYPE, StatusCode};

#[derive(Clone)]
pub struct SupabaseStorageService {
    client: reqwest::Client,
    endpoint: String,
    bucket: String,
    api_key: String,
}

impl SupabaseStorageService {
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        api_key: impl Into<String>,
    ) -> CoveResult<Self> {
        let endpoint = normalize_endpoint(endpoint.into())?;
        let bucket = bucket.into().trim().to_string();
        let api_key = api_key.into().trim().to_string();

        if bucket.is_empty() {
            return Err(CoveError::InvalidInput(
                "storage bucket cannot be empty".to_string(),
            ));
        }
        if api_key.is_empty() {
            return Err(CoveError::InvalidInput(
                "storage API key cannot be empty".to_string(),
            ));
        }

        Ok(Self {
            client: reqwest::Client::builder()
                .use_rustls_tls()
                .http2_adaptive_window(true)
                .build()
                .map_err(|e| CoveError::Unavailable(format!("storage client init: {}", e)))?,
            endpoint,
            bucket,
            api_key,
        })
    }

    fn auth_headers(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        req.header("apikey", &self.api_key)
    }

    fn object_url(&self, key: &str) -> String {
        format!(
            "{}/object/{}/{}",
            self.endpoint,
            self.bucket,
            key.trim_start_matches('/')
        )
    }

    fn bucket_url(&self) -> String {
        format!("{}/bucket/{}", self.endpoint, self.bucket)
    }

    fn authenticated_object_url(&self, key: &str) -> String {
        format!(
            "{}/object/authenticated/{}/{}",
            self.endpoint,
            self.bucket,
            key.trim_start_matches('/')
        )
    }

    fn bucket_collection_url(&self) -> String {
        format!("{}/bucket", self.endpoint)
    }

    pub async fn upload_object(
        &self,
        key: &str,
        data: &[u8],
        content_type: &str,
    ) -> CoveResult<()> {
        let resp = self
            .auth_headers(
                self.client
                    .post(self.object_url(key))
                    .header("x-upsert", "true")
                    .header(CONTENT_TYPE, content_type)
                    .body(data.to_vec()),
            )
            .send()
            .await
            .map_err(|e| CoveError::Unavailable(format!("storage upload request failed: {}", e)))?;

        if resp.status().is_success() {
            return Ok(());
        }

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Err(CoveError::Storage(format!(
            "storage upload failed: status={} body={}",
            status, body
        )))
    }

    pub async fn download_object(&self, key: &str) -> CoveResult<Vec<u8>> {
        let resp = self
            .auth_headers(self.client.get(self.authenticated_object_url(key)))
            .send()
            .await
            .map_err(|e| {
                CoveError::Unavailable(format!("storage download request failed: {}", e))
            })?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(CoveError::NotFound(format!("object {} not found", key)));
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CoveError::Storage(format!(
                "storage download failed: status={} body={}",
                status, body
            )));
        }

        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| CoveError::Storage(format!("storage download body read failed: {}", e)))
    }

    pub async fn health_check(&self) -> CoveResult<()> {
        let resp = self
            .auth_headers(self.client.get(self.bucket_url()))
            .send()
            .await
            .map_err(|e| CoveError::Unavailable(format!("storage health request failed: {}", e)))?;

        if resp.status().is_success() {
            return Ok(());
        }

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Err(CoveError::Unavailable(format!(
            "storage health failed: status={} body={}",
            status, body
        )))
    }

    pub async fn ensure_bucket_exists(&self) -> CoveResult<()> {
        let lookup = self
            .auth_headers(self.client.get(self.bucket_url()))
            .send()
            .await
            .map_err(|e| CoveError::Unavailable(format!("bucket lookup failed: {}", e)))?;

        let lookup_status = lookup.status();
        if lookup_status.is_success() {
            return Ok(());
        }

        let lookup_body = lookup.text().await.unwrap_or_default();
        if !is_bucket_missing_response(lookup_status, &lookup_body) {
            return Err(CoveError::Unavailable(format!(
                "bucket lookup failed: status={} body={}",
                lookup_status, lookup_body
            )));
        }

        let create = self
            .auth_headers(
                self.client
                    .post(self.bucket_collection_url())
                    .header(CONTENT_TYPE, "application/json")
                    .body(format!(r#"{{"name":"{}","public":false}}"#, self.bucket)),
            )
            .send()
            .await
            .map_err(|e| CoveError::Unavailable(format!("bucket create failed: {}", e)))?;

        if create.status().is_success() || create.status() == StatusCode::CONFLICT {
            return Ok(());
        }

        let status = create.status();
        let body = create.text().await.unwrap_or_default();

        // Supabase may occasionally return 400 for duplicate bucket creation attempts.
        if status == StatusCode::BAD_REQUEST && body.to_ascii_lowercase().contains("exists") {
            return Ok(());
        }

        Err(CoveError::Unavailable(format!(
            "bucket create failed: status={} body={}",
            status, body
        )))
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

fn is_bucket_missing_response(status: StatusCode, body: &str) -> bool {
    if status == StatusCode::NOT_FOUND {
        return true;
    }

    if status != StatusCode::BAD_REQUEST {
        return false;
    }

    let lower = body.to_ascii_lowercase();
    lower.contains("bucket not found")
        || lower.contains("\"statuscode\":\"404\"")
        || lower.contains("\"statuscode\":404")
        || lower.contains("\"code\":\"not_found\"")
}

fn normalize_endpoint(endpoint: String) -> CoveResult<String> {
    let endpoint = endpoint.trim().trim_end_matches('/').to_string();
    if endpoint.is_empty() {
        return Err(CoveError::InvalidInput(
            "storage endpoint cannot be empty".to_string(),
        ));
    }

    if endpoint.ends_with("/storage/v1") {
        return Ok(endpoint);
    }

    if endpoint.ends_with("/storage/v1/s3") {
        return Ok(endpoint.trim_end_matches("/s3").to_string());
    }

    if endpoint.ends_with(".supabase.co") {
        return Ok(format!("{}/storage/v1", endpoint));
    }

    Err(CoveError::InvalidInput(
        "SUPABASE_STORAGE_ENDPOINT must end with /storage/v1 or /storage/v1/s3".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{is_bucket_missing_response, normalize_endpoint};
    use reqwest::StatusCode;

    #[test]
    fn normalizes_supabase_s3_endpoint() {
        let endpoint =
            normalize_endpoint("https://project.storage.supabase.co/storage/v1/s3".to_string())
                .unwrap();
        assert_eq!(endpoint, "https://project.storage.supabase.co/storage/v1");
    }

    #[test]
    fn accepts_storage_v1_endpoint() {
        let endpoint =
            normalize_endpoint("https://project.supabase.co/storage/v1".to_string()).unwrap();
        assert_eq!(endpoint, "https://project.supabase.co/storage/v1");
    }

    #[test]
    fn accepts_project_base_url() {
        let endpoint = normalize_endpoint("https://project.supabase.co".to_string()).unwrap();
        assert_eq!(endpoint, "https://project.supabase.co/storage/v1");
    }

    #[test]
    fn rejects_invalid_endpoint() {
        let err = normalize_endpoint("https://example.com/api".to_string()).unwrap_err();
        assert!(err
            .to_string()
            .contains("SUPABASE_STORAGE_ENDPOINT must end with /storage/v1"));
    }

    #[test]
    fn treats_supabase_bucket_not_found_400_as_missing() {
        let body = r#"{"statusCode":"404","error":"Bucket not found","message":"Bucket not found"}"#;
        assert!(is_bucket_missing_response(StatusCode::BAD_REQUEST, body));
    }

    #[test]
    fn does_not_treat_generic_400_as_missing_bucket() {
        let body = r#"{"statusCode":"400","error":"Bad request","message":"Invalid request"}"#;
        assert!(!is_bucket_missing_response(StatusCode::BAD_REQUEST, body));
    }

}
