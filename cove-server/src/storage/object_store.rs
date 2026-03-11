//! S3-compatible object storage helper for media files.

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client as S3Client;
use cove_common::error::{CoveError, CoveResult};
use std::time::Duration;

#[derive(Clone)]
pub struct ObjectStoreService {
    client: S3Client,
    bucket: String,
}

impl ObjectStoreService {
    pub fn new(client: S3Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    pub async fn presigned_put_url(
        &self,
        key: &str,
        content_type: &str,
        ttl_secs: u64,
    ) -> CoveResult<String> {
        let presign_config = PresigningConfig::builder()
            .expires_in(Duration::from_secs(ttl_secs))
            .build()
            .map_err(|e| CoveError::Storage(e.to_string()))?;

        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .presigned(presign_config)
            .await
            .map_err(|e| CoveError::Storage(e.to_string()))?;

        Ok(request.uri().to_string())
    }

    pub async fn presigned_get_url(&self, key: &str, ttl_secs: u64) -> CoveResult<String> {
        let presign_config = PresigningConfig::builder()
            .expires_in(Duration::from_secs(ttl_secs))
            .build()
            .map_err(|e| CoveError::Storage(e.to_string()))?;

        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(presign_config)
            .await
            .map_err(|e| CoveError::Storage(e.to_string()))?;

        Ok(request.uri().to_string())
    }

    pub async fn delete_object(&self, key: &str) -> CoveResult<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| CoveError::Storage(e.to_string()))?;
        Ok(())
    }

    pub async fn head_object(&self, key: &str) -> CoveResult<Option<i64>> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => Ok(output.content_length()),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_not_found() {
                    Ok(None)
                } else {
                    Err(CoveError::Storage(service_err.to_string()))
                }
            }
        }
    }

    pub async fn health_check(&self) -> CoveResult<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| CoveError::Unavailable(format!("s3: {}", e)))?;
        Ok(())
    }
}
