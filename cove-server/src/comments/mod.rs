//! Comment service gRPC implementation.

use crate::auth;
use crate::authorization::{build_user_summary, can_view_post};
use cove_common::error::CoveResult;
use cove_common::id::{CommentId, PostId, UserId};
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::comment::{
    comment_service_server::CommentService, AddCommentRequest, AddCommentResponse,
    CommentDetail, DeleteCommentRequest, DeleteCommentResponse, ListCommentsRequest,
    ListCommentsResponse,
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Comment service implementation.
pub struct CommentServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
}

impl CommentServiceImpl {
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
impl CommentService for CommentServiceImpl {
    async fn add_comment(
        &self,
        request: Request<AddCommentRequest>,
    ) -> Result<Response<AddCommentResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let body = req.body.trim();
        if body.is_empty() {
            return Err(Status::invalid_argument("body required"));
        }

        let can_view = can_view_post(&self.pool, &auth.user_id, &post_id)
            .await
            .map_err(|e| Into::<Status>::into(e))?;

        if !can_view {
            return Err(Status::permission_denied("cannot view post"));
        }

        let parent_id: Option<Uuid> = req
            .parent_comment_id
            .as_ref()
            .and_then(|s| Uuid::parse_str(s).ok());

        if let Some(pid) = parent_id {
            let parent_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM comments WHERE id = $1 AND post_id = $2 AND NOT is_deleted)",
            )
            .bind(pid)
            .bind(post_id.as_uuid())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            if !parent_exists {
                return Err(Status::invalid_argument("parent comment not found"));
            }
        }

        let comment_id = CommentId::new();
        let now = chrono::Utc::now();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

        sqlx::query(
            r#"
            INSERT INTO comments (id, post_id, author_id, parent_id, body, reply_count, is_deleted, created_at)
            VALUES ($1, $2, $3, $4, $5, 0, FALSE, $6)
            "#,
        )
        .bind(comment_id.as_uuid())
        .bind(post_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .bind(parent_id)
        .bind(body)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        sqlx::query(
            "UPDATE posts SET comment_count = comment_count + 1 WHERE id = $1",
        )
        .bind(post_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        if parent_id.is_some() {
            sqlx::query(
                "UPDATE comments SET reply_count = reply_count + 1 WHERE id = $1",
            )
            .bind(parent_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| Status::internal("database error"))?;
        }

        tx.commit()
            .await
            .map_err(|_| Status::internal("database error"))?;

        let post_row = sqlx::query(
            "SELECT author_id FROM posts WHERE id = $1",
        )
        .bind(post_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        let post_author_id: uuid::Uuid = post_row.get(0);

        if post_author_id != *auth.user_id.as_uuid() {
            let _ = Self::enqueue_notification_job(
                &self.pool,
                UserId::from_uuid(post_author_id),
                auth.user_id,
                "comment",
                Some(post_id.into_uuid()),
                &format!("{} commented on your post", auth.user_id),
            )
            .await;
        }

        Ok(Response::new(AddCommentResponse {
            comment_id: comment_id.to_string(),
            created_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn list_comments(
        &self,
        request: Request<ListCommentsRequest>,
    ) -> Result<Response<ListCommentsResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let can_view = can_view_post(&self.pool, &auth.user_id, &post_id)
            .await
            .map_err(|e| Into::<Status>::into(e))?;

        if !can_view {
            return Err(Status::permission_denied("cannot view post"));
        }

        let pagination = PaginationParams::from_proto(
            req.pagination.as_ref().map(|p| p.page_size).unwrap_or(20),
            req.pagination.as_ref().map(|p| p.cursor.as_str()).unwrap_or(""),
        );

        let limit = (pagination.limit + 1) as i64;

        let (rows, has_more): (Vec<_>, bool) = if let Some(ref cursor) = pagination.cursor {
            let rows: Vec<(uuid::Uuid, uuid::Uuid, String, Option<uuid::Uuid>, i32, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
                r#"
                SELECT c.id, c.author_id, c.body, c.parent_id, c.reply_count, c.created_at
                FROM comments c
                WHERE c.post_id = $1 AND NOT c.is_deleted
                  AND (c.created_at, c.id) < ($2, $3)
                ORDER BY c.created_at DESC, c.id DESC
                LIMIT $4
                "#,
            )
            .bind(post_id.as_uuid())
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?;

            let has_more = rows.len() as i64 > pagination.limit as i64;
            (rows, has_more)
        } else {
            let rows: Vec<(uuid::Uuid, uuid::Uuid, String, Option<uuid::Uuid>, i32, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
                r#"
                SELECT c.id, c.author_id, c.body, c.parent_id, c.reply_count, c.created_at
                FROM comments c
                WHERE c.post_id = $1 AND NOT c.is_deleted
                ORDER BY c.created_at DESC, c.id DESC
                LIMIT $2
                "#,
            )
            .bind(post_id.as_uuid())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?;

            let has_more = rows.len() as i64 > pagination.limit as i64;
            (rows, has_more)
        };

        let comments: Vec<CommentDetail> = {
            let truncated: Vec<_> = rows
                .iter()
                .take(pagination.limit as usize)
                .collect();

            let author_ids: Vec<uuid::Uuid> = truncated
                .iter()
                .map(|r| r.1)
                .fold(vec![], |mut acc, id| {
                    if !acc.contains(id) {
                        acc.push(*id);
                    }
                    acc
                });

            let mut author_map: std::collections::HashMap<uuid::Uuid, (String, String)> =
                std::collections::HashMap::new();

            for aid in &author_ids {
                if let Ok(row) = sqlx::query_as::<_, (String, String)>(
                    "SELECT u.username, COALESCE(u.display_name, '') FROM users u WHERE u.id = $1",
                )
                .bind(aid)
                .fetch_one(&self.pool)
                .await
                {
                    author_map.insert(*aid, row);
                }
            }

            let following_ids: Vec<uuid::Uuid> = if author_ids.is_empty() {
                vec![]
            } else {
                sqlx::query_scalar(
                    "SELECT followee_id FROM follows WHERE follower_id = $1 AND followee_id = ANY($2) AND state = 'accepted'",
                )
                .bind(auth.user_id.as_uuid())
                .bind(&author_ids)
                .fetch_all(&self.pool)
                .await
                .unwrap_or_default()
            };
            let following_set: std::collections::HashSet<uuid::Uuid> =
                following_ids.into_iter().collect();

            truncated
                .iter()
                .map(|(id, author_id, body, parent_id, reply_count, created_at)| {
                    let (username, display_name) = author_map
                        .get(author_id)
                        .cloned()
                        .unwrap_or_default();
                    let author = build_user_summary(
                        &UserId::from_uuid(*author_id),
                        username,
                        display_name,
                        String::new(),
                        following_set.contains(author_id),
                    );
                    CommentDetail {
                        comment_id: id.to_string(),
                        author: Some(author),
                        body: body.clone(),
                        parent_comment_id: parent_id.map(|u| u.to_string()),
                        reply_count: *reply_count,
                        created_at: Some(prost_types::Timestamp {
                            seconds: created_at.timestamp(),
                            nanos: created_at.timestamp_subsec_nanos() as i32,
                        }),
                    }
                })
                .collect()
        };

        let next_cursor = if has_more && rows.len() > pagination.limit as usize {
            rows.get(pagination.limit as usize - 1).map(|r| {
                PaginationParams::encode_cursor(&CursorValue {
                    timestamp: r.5,
                    id: r.0,
                })
            }).unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(ListCommentsResponse {
            comments,
            pagination: Some(cove_proto::cove::common::PaginationResponse {
                next_cursor,
                has_more,
                total_count: -1,
            }),
        }))
    }

    async fn delete_comment(
        &self,
        request: Request<DeleteCommentRequest>,
    ) -> Result<Response<DeleteCommentResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let comment_id = CommentId::parse(&req.comment_id)
            .map_err(|_| Status::invalid_argument("invalid comment_id"))?;

        let row = sqlx::query(
            r#"
            SELECT c.author_id, c.post_id, p.author_id as post_author_id
            FROM comments c
            JOIN posts p ON p.id = c.post_id
            WHERE c.id = $1 AND NOT c.is_deleted
            "#,
        )
        .bind(comment_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        let row = row.ok_or_else(|| Status::not_found("comment not found"))?;

        let comment_author: uuid::Uuid = row.get(0);
        let post_author: uuid::Uuid = row.get(2);

        if comment_author != *auth.user_id.as_uuid() && post_author != *auth.user_id.as_uuid() {
            return Err(Status::permission_denied(
                "only comment author or post author can delete",
            ));
        }

        sqlx::query(
            "UPDATE comments SET is_deleted = TRUE WHERE id = $1",
        )
        .bind(comment_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        Ok(Response::new(DeleteCommentResponse {}))
    }
}
