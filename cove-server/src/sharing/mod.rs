//! Share service gRPC implementation.

use crate::auth;
use crate::authorization::{build_user_summary, can_view_post};
use cove_common::error::CoveResult;
use cove_common::id::{PostId, ShareId, UserId};
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::share::{
    share_service_server::ShareService, GetSharedPostsRequest, GetSharedPostsResponse,
    SharePostRequest, SharePostResponse, SharedPostItem,
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Share service implementation.
pub struct ShareServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
}

impl ShareServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String) -> Self {
        Self { pool, jwt_secret }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<cove_common::auth_context::AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }

    async fn enqueue_notification_job(
        pool: &PgPool,
        recipient_id: UserId,
        actor_id: UserId,
        notification_type: &str,
        target_id: Option<Uuid>,
        message: &str,
    ) -> CoveResult<()> {
        let payload = serde_json::json!({
            "recipient_id": recipient_id.to_string(),
            "actor_id": actor_id.to_string(),
            "notification_type": notification_type,
            "target_id": target_id.map(|u| u.to_string()),
            "message": message
        });

        sqlx::query(
            r#"
            INSERT INTO jobs (id, job_type, payload, state, run_at)
            VALUES (gen_random_uuid(), 'notification', $1, 'pending', NOW())
            "#,
        )
        .bind(sqlx::types::Json(&payload))
        .execute(pool)
        .await
        .map_err(|e| cove_common::error::CoveError::Database(e.to_string()))?;

        Ok(())
    }
}

#[tonic::async_trait]
impl ShareService for ShareServiceImpl {
    async fn share_post(
        &self,
        request: Request<SharePostRequest>,
    ) -> Result<Response<SharePostResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let recipient_id = UserId::parse(&req.recipient_user_id)
            .map_err(|_| Status::invalid_argument("invalid recipient_user_id"))?;

        let can_view = can_view_post(&self.pool, &auth.user_id, &post_id)
            .await
            .map_err(|e| Into::<Status>::into(e))?;

        if !can_view {
            return Err(Status::permission_denied("cannot view post"));
        }

        let recipient_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND account_state = 'active')",
        )
        .bind(recipient_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        if !recipient_exists {
            return Err(Status::not_found("recipient not found"));
        }

        if recipient_id == auth.user_id {
            return Err(Status::invalid_argument("cannot share post with yourself"));
        }

        let share_id = ShareId::new();
        let now = chrono::Utc::now();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

        sqlx::query(
            r#"
            INSERT INTO shares (id, sender_id, recipient_id, post_id, created_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(share_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .bind(recipient_id.as_uuid())
        .bind(post_id.as_uuid())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        sqlx::query(
            "UPDATE posts SET share_count = share_count + 1 WHERE id = $1",
        )
        .bind(post_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        tx.commit()
            .await
            .map_err(|_| Status::internal("database error"))?;

        let _ = Self::enqueue_notification_job(
            &self.pool,
            recipient_id,
            auth.user_id,
            "share",
            Some(post_id.into_uuid()),
            &format!("{} shared a post with you", auth.user_id),
        )
        .await;

        Ok(Response::new(SharePostResponse {
            share_id: share_id.to_string(),
            shared_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn get_shared_posts(
        &self,
        request: Request<GetSharedPostsRequest>,
    ) -> Result<Response<GetSharedPostsResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let pagination = PaginationParams::from_proto(
            req.pagination.as_ref().map(|p| p.page_size).unwrap_or(20),
            req.pagination.as_ref().map(|p| p.cursor.as_str()).unwrap_or(""),
        );

        let limit = (pagination.limit + 1) as i64;

        type ShareRow = (
            uuid::Uuid,
            uuid::Uuid,
            uuid::Uuid,
            chrono::DateTime<chrono::Utc>,
        );

        let rows: Vec<ShareRow> = if let Some(ref cursor) = pagination.cursor {
            sqlx::query_as(
                r#"
                SELECT s.id, s.sender_id, s.post_id, s.created_at
                FROM shares s
                WHERE s.recipient_id = $1
                  AND (s.created_at, s.id) < ($2, $3)
                ORDER BY s.created_at DESC, s.id DESC
                LIMIT $4
                "#,
            )
            .bind(auth.user_id.as_uuid())
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?
        } else {
            sqlx::query_as(
                r#"
                SELECT s.id, s.sender_id, s.post_id, s.created_at
                FROM shares s
                WHERE s.recipient_id = $1
                ORDER BY s.created_at DESC, s.id DESC
                LIMIT $2
                "#,
            )
            .bind(auth.user_id.as_uuid())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?
        };

        let has_more = rows.len() as i64 > pagination.limit as i64;
        let truncated: Vec<_> = rows.iter().take(pagination.limit as usize).collect();

        let sender_ids: Vec<uuid::Uuid> = truncated
            .iter()
            .map(|r| r.1)
            .fold(vec![], |mut acc, id| {
                if !acc.contains(&id) {
                    acc.push(id);
                }
                acc
            });

        let mut sender_map: std::collections::HashMap<uuid::Uuid, (String, String, Option<uuid::Uuid>)> =
            std::collections::HashMap::new();

        for sid in &sender_ids {
            if let Ok(row) = sqlx::query_as::<_, (String, String, Option<uuid::Uuid>)>(
                r#"
                SELECT u.username, COALESCE(u.display_name, ''), p.avatar_media_id
                FROM users u
                LEFT JOIN profiles p ON p.user_id = u.id
                WHERE u.id = $1
                "#,
            )
            .bind(sid)
            .fetch_one(&self.pool)
            .await
            {
                sender_map.insert(*sid, row);
            }
        }

        let items: Vec<SharedPostItem> = truncated
            .iter()
            .map(|(share_id, sender_id, post_id, created_at)| {
                let (username, display_name, avatar_media_id) = sender_map
                    .get(sender_id)
                    .cloned()
                    .unwrap_or_default();
                let avatar_url = avatar_media_id
                    .map(|media_id| format!("/media/{}", media_id))
                    .unwrap_or_default();
                let sender = build_user_summary(
                    &UserId::from_uuid(*sender_id),
                    username,
                    display_name,
                    avatar_url,
                    false,
                );
                SharedPostItem {
                    share_id: share_id.to_string(),
                    sender: Some(sender),
                    post_id: post_id.to_string(),
                    shared_at: Some(prost_types::Timestamp {
                        seconds: created_at.timestamp(),
                        nanos: created_at.timestamp_subsec_nanos() as i32,
                    }),
                }
            })
            .collect();

        let next_cursor = if has_more && rows.len() > pagination.limit as usize {
            rows.get(pagination.limit as usize - 1)
                .map(|r| {
                    PaginationParams::encode_cursor(&CursorValue {
                        timestamp: r.3,
                        id: r.0,
                    })
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(GetSharedPostsResponse {
            items,
            pagination: Some(cove_proto::cove::common::PaginationResponse {
                next_cursor,
                has_more,
                total_count: -1,
            }),
        }))
    }
}
