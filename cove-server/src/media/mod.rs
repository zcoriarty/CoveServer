//! Media service and gRPC handler.

use crate::auth;
use crate::storage::object_store::LocalStorageService;
use cove_common::id::{MediaId, UserId};
use cove_proto::cove::media::{
    media_service_server::MediaService, upload_media_request, DownloadMediaHeader,
    DownloadMediaRequest, DownloadMediaResponse, download_media_response, GetMediaStatusRequest,
    GetMediaStatusResponse, MediaVariant, ProcessingState, UploadMediaResponse,
    UploadMediaRequest,
};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status, Streaming};
use tokio_stream::wrappers::ReceiverStream;

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

const DOWNLOAD_CHUNK_SIZE: usize = 64 * 1024; // 64 KB

pub struct MediaServiceImpl {
    pub pool: PgPool,
    pub storage: LocalStorageService,
    pub jwt_secret: String,
}

impl MediaServiceImpl {
    pub fn new(pool: PgPool, storage: LocalStorageService, jwt_secret: String) -> Self {
        Self {
            pool,
            storage,
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
    type DownloadMediaStream = ReceiverStream<Result<DownloadMediaResponse, Status>>;

    async fn upload_media(
        &self,
        request: Request<Streaming<UploadMediaRequest>>,
    ) -> Result<Response<UploadMediaResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let mut stream = request.into_inner();

        let first_msg = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty upload stream"))?;

        let metadata = match first_msg.payload {
            Some(upload_media_request::Payload::Metadata(m)) => m,
            _ => return Err(Status::invalid_argument("first message must be metadata")),
        };

        self.validate_upload(
            metadata.media_type,
            metadata.file_size_bytes,
            &metadata.content_type,
        )?;

        let media_id = MediaId::new();
        let media_type_str = Self::media_type_to_str(metadata.media_type);

        let ext = metadata
            .filename
            .rsplit('.')
            .next()
            .unwrap_or("bin")
            .to_lowercase();
        let sanitized_ext = if matches!(ext.as_str(), "jpg" | "jpeg" | "png" | "gif" | "webp" | "mp4" | "mov") {
            ext
        } else {
            match metadata.media_type {
                2 => "mp4".to_string(),
                _ => "jpg".to_string(),
            }
        };

        let storage_key = format!(
            "media/{}/original.{}",
            media_id.as_uuid(),
            sanitized_ext
        );

        let mut file_data = Vec::with_capacity(metadata.file_size_bytes as usize);
        while let Some(msg) = stream.message().await? {
            match msg.payload {
                Some(upload_media_request::Payload::ChunkData(chunk)) => {
                    file_data.extend_from_slice(&chunk);
                    if file_data.len() as i64 > metadata.file_size_bytes {
                        return Err(Status::invalid_argument("upload exceeds declared file size"));
                    }
                }
                _ => return Err(Status::invalid_argument("expected chunk_data after metadata")),
            }
        }

        self.storage
            .write_file(&storage_key, &file_data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO media_items (
                id, owner_id, media_type, original_key, content_type,
                file_size_bytes, processing_state, checksum
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)
            "#,
        )
        .bind(media_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .bind(media_type_str)
        .bind(&storage_key)
        .bind(&metadata.content_type)
        .bind(file_data.len() as i64)
        .bind(&metadata.checksum)
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        sqlx::query(
            r#"
            UPDATE media_items SET processing_state = 'processing' WHERE id = $1
            "#,
        )
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

        Ok(Response::new(UploadMediaResponse {
            media_id: media_id.to_string(),
            state: ProcessingState::Processing as i32,
        }))
    }

    async fn download_media(
        &self,
        request: Request<DownloadMediaRequest>,
    ) -> Result<Response<Self::DownloadMediaStream>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let media_id = MediaId::parse(&req.media_id)
            .map_err(|_| Status::invalid_argument("invalid media_id"))?;

        let row = sqlx::query(
            r#"
            SELECT m.id, m.owner_id, m.post_id, m.original_key, m.thumbnail_key,
                   m.feed_key, m.display_key, m.processing_state, m.content_type
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
        let content_type: String = row.get(8);

        if processing_state != "completed" {
            return Err(Status::failed_precondition("media is still processing"));
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
        let storage_key = match variant {
            MediaVariant::Original | MediaVariant::Unspecified => original_key,
            MediaVariant::Thumbnail => thumbnail_key.unwrap_or(original_key),
            MediaVariant::Feed => feed_key.unwrap_or(original_key),
            MediaVariant::Display => display_key.unwrap_or(original_key),
        };

        let file_data = self
            .storage
            .read_file(&storage_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = tokio::sync::mpsc::channel(8);

        let file_content_type = content_type;
        let file_len = file_data.len() as i64;

        tokio::spawn(async move {
            let header = DownloadMediaResponse {
                payload: Some(download_media_response::Payload::Header(DownloadMediaHeader {
                    content_type: file_content_type,
                    file_size_bytes: file_len,
                })),
            };
            if tx.send(Ok(header)).await.is_err() {
                return;
            }

            for chunk in file_data.chunks(DOWNLOAD_CHUNK_SIZE) {
                let msg = DownloadMediaResponse {
                    payload: Some(download_media_response::Payload::ChunkData(chunk.to_vec())),
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_media_status(
        &self,
        request: Request<GetMediaStatusRequest>,
    ) -> Result<Response<GetMediaStatusResponse>, Status> {
        let _auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let media_id = MediaId::parse(&req.media_id)
            .map_err(|_| Status::invalid_argument("invalid media_id"))?;

        let row = sqlx::query(
            "SELECT processing_state FROM media_items WHERE id = $1",
        )
        .bind(media_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let row = row.ok_or_else(|| Status::not_found("media not found"))?;
        let state_str: String = row.get(0);

        let state = match state_str.as_str() {
            "pending" => ProcessingState::Pending,
            "processing" => ProcessingState::Processing,
            "completed" => ProcessingState::Completed,
            "failed" => ProcessingState::Failed,
            _ => ProcessingState::Unspecified,
        };

        Ok(Response::new(GetMediaStatusResponse {
            media_id: media_id.to_string(),
            state: state as i32,
        }))
    }
}
