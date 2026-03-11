//! Like service gRPC implementation.

use crate::auth;
use crate::authorization::can_view_post;
use cove_common::error::CoveResult;
use cove_common::id::{PostId, UserId};
use cove_proto::cove::like::{
    like_service_server::LikeService, GetLikeStatusRequest, GetLikeStatusResponse,
    LikePostRequest, LikePostResponse, UnlikePostRequest, UnlikePostResponse,
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Like service implementation.
pub struct LikeServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
}

impl LikeServiceImpl {
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
impl LikeService for LikeServiceImpl {
    async fn like_post(
        &self,
        request: Request<LikePostRequest>,
    ) -> Result<Response<LikePostResponse>, Status> {
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

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

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
            sqlx::query(
                "UPDATE posts SET like_count = like_count + 1 WHERE id = $1",
            )
            .bind(post_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|_| Status::internal("database error"))?;
        }

        let like_count: i32 = sqlx::query_scalar(
            "SELECT like_count FROM posts WHERE id = $1",
        )
        .bind(post_id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        tx.commit()
            .await
            .map_err(|_| Status::internal("database error"))?;

        if inserted {
            let post_row = sqlx::query("SELECT author_id FROM posts WHERE id = $1")
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
                    "like",
                    Some(post_id.into_uuid()),
                    &format!("{} liked your post", auth.user_id),
                )
                .await;
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

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

        let delete_result = sqlx::query(
            "DELETE FROM likes WHERE user_id = $1 AND post_id = $2",
        )
        .bind(auth.user_id.as_uuid())
        .bind(post_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        if delete_result.rows_affected() > 0 {
            sqlx::query(
                "UPDATE posts SET like_count = GREATEST(0, like_count - 1) WHERE id = $1",
            )
            .bind(post_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|_| Status::internal("database error"))?;
        }

        let like_count: i32 = sqlx::query_scalar(
            "SELECT like_count FROM posts WHERE id = $1",
        )
        .bind(post_id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

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

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let can_view = can_view_post(&self.pool, &auth.user_id, &post_id)
            .await
            .map_err(|e| Into::<Status>::into(e))?;

        if !can_view {
            return Err(Status::permission_denied("cannot view post"));
        }

        let liked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM likes WHERE user_id = $1 AND post_id = $2)",
        )
        .bind(auth.user_id.as_uuid())
        .bind(post_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        let like_count: i32 = sqlx::query_scalar(
            "SELECT like_count FROM posts WHERE id = $1",
        )
        .bind(post_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        Ok(Response::new(GetLikeStatusResponse {
            liked,
            like_count,
        }))
    }
}
