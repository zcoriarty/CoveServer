//! Like service gRPC implementation.

use crate::auth;
use crate::authorization::can_view_post;
use cove_common::error::CoveResult;
use cove_common::id::{PostId, UserId};
use cove_proto::cove::like::{
    like_service_server::LikeService, GetLikeStatusRequest, GetLikeStatusResponse, LikePostRequest,
    LikePostResponse, UnlikePostRequest, UnlikePostResponse,
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Like service implementation.
pub struct LikeServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
}

#[derive(Clone, Copy)]
enum LikeTarget {
    Post {
        post_id: PostId,
        post_author_id: Uuid,
    },
    Comment {
        comment_id: Uuid,
        post_id: PostId,
        comment_author_id: Uuid,
    },
}

impl LikeServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String) -> Self {
        Self { pool, jwt_secret }
    }

    fn auth(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<cove_common::auth_context::AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }

    async fn enqueue_notification_job(
        pool: &PgPool,
        recipient_id: UserId,
        actor_id: UserId,
        notification_type: &str,
        target_id: Option<Uuid>,
        message: &str,
        push_type: Option<&str>,
        push_title: Option<&str>,
        push_body: Option<&str>,
    ) -> CoveResult<()> {
        let mut payload = serde_json::json!({
            "recipient_id": recipient_id.to_string(),
            "actor_id": actor_id.to_string(),
            "notification_type": notification_type,
            "target_id": target_id.map(|u| u.to_string()),
            "message": message
        });

        if let Some(push_type) = push_type {
            payload["push_type"] = serde_json::Value::String(push_type.to_string());
        }

        if let Some(push_title) = push_title {
            payload["push_title"] = serde_json::Value::String(push_title.to_string());
        }

        if let Some(push_body) = push_body {
            payload["push_body"] = serde_json::Value::String(push_body.to_string());
        }

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

    async fn resolve_like_target(&self, viewer_id: &UserId, raw_target_id: &str) -> Result<LikeTarget, Status> {
        let target_uuid = Uuid::parse_str(raw_target_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let post_author_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT author_id FROM posts WHERE id = $1 AND NOT is_deleted",
        )
        .bind(target_uuid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        if let Some(post_author_id) = post_author_id {
            let post_id = PostId::from_uuid(target_uuid);
            let can_view = can_view_post(&self.pool, viewer_id, &post_id)
                .await
                .map_err(|e| Into::<Status>::into(e))?;

            if !can_view {
                return Err(Status::permission_denied("cannot view post"));
            }

            return Ok(LikeTarget::Post {
                post_id,
                post_author_id,
            });
        }

        let comment_row: Option<(Uuid, Uuid)> = sqlx::query_as(
            r#"
            SELECT c.author_id, c.post_id
            FROM comments c
            JOIN posts p ON p.id = c.post_id
            WHERE c.id = $1
              AND NOT c.is_deleted
              AND NOT p.is_deleted
            "#,
        )
        .bind(target_uuid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        let Some((comment_author_id, comment_post_id)) = comment_row else {
            return Err(Status::permission_denied("cannot view post"));
        };

        let post_id = PostId::from_uuid(comment_post_id);
        let can_view = can_view_post(&self.pool, viewer_id, &post_id)
            .await
            .map_err(|e| Into::<Status>::into(e))?;

        if !can_view {
            return Err(Status::permission_denied("cannot view post"));
        }

        Ok(LikeTarget::Comment {
            comment_id: target_uuid,
            post_id,
            comment_author_id,
        })
    }
}

#[tonic::async_trait]
impl LikeService for LikeServiceImpl {
    async fn like_post(
        &self,
        request: Request<LikePostRequest>,
    ) -> Result<Response<LikePostResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target = self
            .resolve_like_target(&auth.user_id, &req.post_id)
            .await?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

        let (inserted, like_count) = match target {
            LikeTarget::Post { post_id, .. } => {
                let insert_result = sqlx::query(
                    r#"
                    INSERT INTO likes (user_id, post_id, created_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (user_id, post_id) DO NOTHING
                    "#,
                )
                .bind(auth.user_id.as_uuid())
                .bind(post_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(|_| Status::internal("database error"))?;

                let inserted = insert_result.rows_affected() > 0;

                if inserted {
                    sqlx::query("UPDATE posts SET like_count = like_count + 1 WHERE id = $1")
                        .bind(post_id.as_uuid())
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| Status::internal("database error"))?;
                }

                let like_count: i32 = sqlx::query_scalar("SELECT like_count FROM posts WHERE id = $1")
                    .bind(post_id.as_uuid())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?;

                (inserted, like_count)
            }
            LikeTarget::Comment { comment_id, .. } => {
                let insert_result = sqlx::query(
                    r#"
                    INSERT INTO comment_likes (user_id, comment_id, created_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (user_id, comment_id) DO NOTHING
                    "#,
                )
                .bind(auth.user_id.as_uuid())
                .bind(comment_id)
                .execute(&mut *tx)
                .await
                .map_err(|_| Status::internal("database error"))?;

                let inserted = insert_result.rows_affected() > 0;

                if inserted {
                    sqlx::query("UPDATE comments SET like_count = like_count + 1 WHERE id = $1")
                        .bind(comment_id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| Status::internal("database error"))?;
                }

                let like_count: i32 = sqlx::query_scalar("SELECT like_count FROM comments WHERE id = $1")
                    .bind(comment_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?;

                (inserted, like_count)
            }
        };

        tx.commit()
            .await
            .map_err(|_| Status::internal("database error"))?;

        if inserted {
            match target {
                LikeTarget::Post {
                    post_id,
                    post_author_id,
                } => {
                    if post_author_id != *auth.user_id.as_uuid() {
                        let _ = Self::enqueue_notification_job(
                            &self.pool,
                            UserId::from_uuid(post_author_id),
                            auth.user_id,
                            "like",
                            Some(post_id.into_uuid()),
                            "liked your post",
                            None,
                            None,
                            None,
                        )
                        .await;
                    }
                }
                LikeTarget::Comment {
                    post_id,
                    comment_author_id,
                    ..
                } => {
                    if comment_author_id != *auth.user_id.as_uuid() {
                        let _ = Self::enqueue_notification_job(
                            &self.pool,
                            UserId::from_uuid(comment_author_id),
                            auth.user_id,
                            "like",
                            Some(post_id.into_uuid()),
                            "liked your comment",
                            Some("like"),
                            Some("New like"),
                            Some("{actor} liked your comment"),
                        )
                        .await;
                    }
                }
            }
        }

        Ok(Response::new(LikePostResponse { like_count }))
    }

    async fn unlike_post(
        &self,
        request: Request<UnlikePostRequest>,
    ) -> Result<Response<UnlikePostResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target = self
            .resolve_like_target(&auth.user_id, &req.post_id)
            .await?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

        let like_count: i32 = match target {
            LikeTarget::Post { post_id, .. } => {
                let delete_result = sqlx::query("DELETE FROM likes WHERE user_id = $1 AND post_id = $2")
                    .bind(auth.user_id.as_uuid())
                    .bind(post_id.as_uuid())
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?;

                if delete_result.rows_affected() > 0 {
                    sqlx::query("UPDATE posts SET like_count = GREATEST(0, like_count - 1) WHERE id = $1")
                        .bind(post_id.as_uuid())
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| Status::internal("database error"))?;
                }

                sqlx::query_scalar("SELECT like_count FROM posts WHERE id = $1")
                    .bind(post_id.as_uuid())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?
            }
            LikeTarget::Comment { comment_id, .. } => {
                let delete_result = sqlx::query(
                    "DELETE FROM comment_likes WHERE user_id = $1 AND comment_id = $2",
                )
                .bind(auth.user_id.as_uuid())
                .bind(comment_id)
                .execute(&mut *tx)
                .await
                .map_err(|_| Status::internal("database error"))?;

                if delete_result.rows_affected() > 0 {
                    sqlx::query(
                        "UPDATE comments SET like_count = GREATEST(0, like_count - 1) WHERE id = $1",
                    )
                    .bind(comment_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?;
                }

                sqlx::query_scalar("SELECT like_count FROM comments WHERE id = $1")
                    .bind(comment_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?
            }
        };

        tx.commit()
            .await
            .map_err(|_| Status::internal("database error"))?;

        Ok(Response::new(UnlikePostResponse { like_count }))
    }

    async fn get_like_status(
        &self,
        request: Request<GetLikeStatusRequest>,
    ) -> Result<Response<GetLikeStatusResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target = self
            .resolve_like_target(&auth.user_id, &req.post_id)
            .await?;

        let (liked, like_count) = match target {
            LikeTarget::Post { post_id, .. } => {
                let liked: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM likes WHERE user_id = $1 AND post_id = $2)",
                )
                .bind(auth.user_id.as_uuid())
                .bind(post_id.as_uuid())
                .fetch_one(&self.pool)
                .await
                .map_err(|_| Status::internal("database error"))?;

                let like_count: i32 = sqlx::query_scalar("SELECT like_count FROM posts WHERE id = $1")
                    .bind(post_id.as_uuid())
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|_| Status::internal("database error"))?;

                (liked, like_count)
            }
            LikeTarget::Comment { comment_id, .. } => {
                let liked: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM comment_likes WHERE user_id = $1 AND comment_id = $2)",
                )
                .bind(auth.user_id.as_uuid())
                .bind(comment_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|_| Status::internal("database error"))?;

                let like_count: i32 = sqlx::query_scalar("SELECT like_count FROM comments WHERE id = $1")
                    .bind(comment_id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|_| Status::internal("database error"))?;

                (liked, like_count)
            }
        };

        Ok(Response::new(GetLikeStatusResponse { liked, like_count }))
    }
}
