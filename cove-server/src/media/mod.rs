//! Media service and gRPC handler.

use crate::auth;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client;
use cove_common::id::{MediaId, UserId};
use cove_proto::cove::media::{
    media_service_server::MediaService, CompleteUploadRequest, CompleteUploadResponse,
    GetMediaAccessRequest, GetMediaAccessResponse, InitiateUploadRequest, InitiateUploadResponse,
    MediaVariant, ProcessingState,
};
use prost_types::Timestamp;
use std::time::Duration;
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};

/// Allowed content types for uploads.
const ALLOWED_IMAGE_TYPES: &[&str] = &[
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
];

const ALLOWED_VIDEO_TYPES: &[&str] = &[
    "video/mp4",
    "video/quicktime",
];

const MAX_IMAGE_SIZE_BYTES: i64 = 20 * 1024 * 1024; // 20 MB
const MAX_VIDEO_SIZE_BYTES: i64 = 100 * 1024 * 1024; // 100 MB

/// Media service implementation.
pub struct MediaServiceImpl {
    pub pool: PgPool,
    pub s3_client: Client,
    pub bucket: String,
    pub jwt_secret: String,
}

impl MediaServiceImpl {
    pub fn new(pool: PgPool, s3_client: Client, bucket: String, jwt_secret: String) -> Self {
        Self {
            pool,
            s3_client,
            bucket,
            jwt_secret,
        }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<cove_common::auth_context::AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }

    fn validate_upload(&self, media_type: i32, file_size: i64, content_type: &str) -> Result<(), Status> {
        let (allowed_types, max_size) = match media_type {
            1 => (ALLOWED_IMAGE_TYPES, MAX_IMAGE_SIZE_BYTES),
            2 => (ALLOWED_VIDEO_TYPES, MAX_VIDEO_SIZE_BYTES),
            _ => return Err(Status::invalid_argument("unsupported media type")),
        };

        if file_size <= 0 || file_size > max_size {
            return Err(Status::invalid_argument(format!(
                "file size must be between 1 and {} bytes",
                max_size
            )));
        }

        let content_type_lower = content_type.to_lowercase();
        let allowed = allowed_types
            .iter()
            .any(|t| content_type_lower.starts_with(t) || content_type_lower == *t);

        if !allowed {
            return Err(Status::invalid_argument(format!(
                "content type {} not allowed; allowed: {:?}",
                content_type, allowed_types
            )));
        }

        Ok(())
    }

    fn media_type_to_str(media_type: i32) -> &'static str {
        match media_type {
            1 => "photo",
            2 => "video",
            _ => "photo",
        }
    }

}

#[tonic::async_trait]
impl MediaService for MediaServiceImpl {
    async fn initiate_upload(
        &self,
        request: Request<InitiateUploadRequest>,
    ) -> Result<Response<InitiateUploadResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        self.validate_upload(
            req.media_type,
            req.file_size_bytes,
            &req.content_type,
        )?;

        let media_id = MediaId::new();
        let media_type_str = Self::media_type_to_str(req.media_type);

        let ext = req
            .filename
            .rsplit('.')
            .next()
            .unwrap_or("bin")
            .to_lowercase();
        let sanitized_ext = if matches!(ext.as_str(), "jpg" | "jpeg" | "png" | "gif" | "webp" | "mp4" | "mov") {
            ext
        } else {
            match req.media_type {
                2 => "mp4".to_string(),
                _ => "jpg".to_string(),
            }
        };

        let s3_key = format!(
            "media/{}/original.{}",
            media_id.as_uuid(),
            sanitized_ext
        );

        sqlx::query(
            r#"
            INSERT INTO media_items (
                id, owner_id, media_type, original_key, content_type,
                file_size_bytes, processing_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'pending')
            "#,
        )
        .bind(media_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .bind(media_type_str)
        .bind(&s3_key)
        .bind(&req.content_type)
        .bind(req.file_size_bytes)
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let presigning_config = PresigningConfig::expires_in(Duration::from_secs(3600))
            .map_err(|e| Status::internal(e.to_string()))?;

        let presigned = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .presigned(presigning_config)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(3600);

        let mut upload_headers = std::collections::HashMap::new();
        upload_headers.insert(
            "Content-Type".to_string(),
            req.content_type,
        );

        Ok(Response::new(InitiateUploadResponse {
            media_id: media_id.to_string(),
            upload_url: presigned.uri().to_string(),
            upload_headers,
            expires_at: Some(Timestamp {
                seconds: expires_at.timestamp(),
                nanos: expires_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn complete_upload(
        &self,
        request: Request<CompleteUploadRequest>,
    ) -> Result<Response<CompleteUploadResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let media_id = MediaId::parse(&req.media_id)
            .map_err(|_| Status::invalid_argument("invalid media_id"))?;

        let row = sqlx::query(
            r#"
            SELECT id, owner_id, processing_state FROM media_items WHERE id = $1
            "#,
        )
        .bind(media_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let row = row.ok_or_else(|| Status::not_found("media not found"))?;

        let owner_id: uuid::Uuid = row.get(1);
        let owner_id = UserId::from_uuid(owner_id);
        let processing_state: String = row.get(2);

        if owner_id != auth.user_id {
            return Err(Status::permission_denied("not the media owner"));
        }

        if processing_state != "pending" {
            return Err(Status::invalid_argument(
                "media already processed or upload not in pending state",
            ));
        }

        sqlx::query(
            r#"
            UPDATE media_items
            SET checksum = $1, processing_state = 'processing'
            WHERE id = $2
            "#,
        )
        .bind(&req.checksum)
        .bind(media_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let job_payload = serde_json::json!({
            "media_id": media_id.to_string(),
        });

        sqlx::query(
            r#"
            INSERT INTO jobs (id, job_type, payload, state)
            VALUES ($1, 'media_processing', $2, 'pending')
            "#,
        )
        .bind(uuid::Uuid::now_v7())
        .bind(job_payload)
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CompleteUploadResponse {
            media_id: media_id.to_string(),
            state: ProcessingState::Processing as i32,
        }))
    }

    async fn get_media_access(
        &self,
        request: Request<GetMediaAccessRequest>,
    ) -> Result<Response<GetMediaAccessResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let media_id = MediaId::parse(&req.media_id)
            .map_err(|_| Status::invalid_argument("invalid media_id"))?;

        let row = sqlx::query(
            r#"
            SELECT m.id, m.owner_id, m.post_id, m.original_key, m.thumbnail_key,
                   m.feed_key, m.display_key, m.processing_state
            FROM media_items m
            WHERE m.id = $1
            "#,
        )
        .bind(media_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let row = row.ok_or_else(|| Status::not_found("media not found"))?;

        let owner_id: uuid::Uuid = row.get(1);
        let owner_id = UserId::from_uuid(owner_id);
        let post_id: Option<uuid::Uuid> = row.get(2);
        let original_key: String = row.get(3);
        let thumbnail_key: Option<String> = row.get(4);
        let feed_key: Option<String> = row.get(5);
        let display_key: Option<String> = row.get(6);
        let processing_state: String = row.get(7);

        if processing_state != "completed" {
            return Err(Status::failed_precondition(
                "media is still processing",
            ));
        }

        let authorized = if auth.user_id == owner_id {
            true
        } else if let Some(pid) = post_id {
            let post_row = sqlx::query(
                r#"
                SELECT author_id, visibility FROM posts WHERE id = $1 AND NOT is_deleted
                "#,
            )
            .bind(pid)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            if let Some(pr) = post_row {
                let author_id: uuid::Uuid = pr.get(0);
                let author_id = UserId::from_uuid(author_id);
                let visibility: String = pr.get(1);

                if auth.user_id == author_id {
                    true
                } else if visibility == "followers" {
                    let is_follower = sqlx::query_scalar::<_, bool>(
                        r#"
                        SELECT EXISTS(
                            SELECT 1 FROM follows
                            WHERE follower_id = $1 AND followee_id = $2 AND state = 'accepted'
                        )
                        "#,
                    )
                    .bind(auth.user_id.as_uuid())
                    .bind(author_id.as_uuid())
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                    is_follower
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if !authorized {
            return Err(Status::permission_denied("not authorized to access this media"));
        }

        let variant = req.variant();
        let s3_key = match variant {
            MediaVariant::Original => original_key,
            MediaVariant::Thumbnail => thumbnail_key.unwrap_or(original_key),
            MediaVariant::Feed => feed_key.unwrap_or(original_key),
            MediaVariant::Display => display_key.unwrap_or(original_key),
            _ => original_key,
        };

        let presigning_config = PresigningConfig::expires_in(Duration::from_secs(3600))
            .map_err(|e| Status::internal(e.to_string()))?;

        let presigned = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .presigned(presigning_config)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(3600);

        let mut headers = std::collections::HashMap::new();
        headers.insert("Location".to_string(), presigned.uri().to_string());

        Ok(Response::new(GetMediaAccessResponse {
            url: presigned.uri().to_string(),
            expires_at: Some(Timestamp {
                seconds: expires_at.timestamp(),
                nanos: expires_at.timestamp_subsec_nanos() as i32,
            }),
            headers,
        }))
    }
}
