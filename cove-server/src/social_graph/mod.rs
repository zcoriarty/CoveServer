//! Follow service and gRPC handler.

use crate::auth;
use crate::notification_preferences;
use crate::push::PushService;
use cove_common::auth_context::AuthContext;
use cove_common::id::UserId;
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::common::{PaginationResponse, UserSummary};
use cove_proto::cove::follow::{
    follow_service_server::FollowService, AcceptFollowRequestReq, AcceptFollowRequestResp,
    FollowRequest, FollowResponse, FollowState, GetFollowStatusRequest, GetFollowStatusResponse,
    GetFollowersRequest, GetFollowersResponse, GetFollowingRequest, GetFollowingResponse,
    GetPendingRequestsRequest, GetPendingRequestsResponse, PendingFollowRequest,
    RejectFollowRequestReq, RejectFollowRequestResp, UnfollowRequest, UnfollowResponse,
};
use sqlx::{PgPool, Postgres, Row};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Follow service implementation.
pub struct FollowServiceImpl {
    pool: PgPool,
    jwt_secret: String,
    push: Arc<PushService>,
}

impl FollowServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String, push: Arc<PushService>) -> Self {
        Self {
            pool,
            jwt_secret,
            push,
        }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }

    fn state_to_proto(state: &str) -> i32 {
        match state {
            "accepted" => FollowState::Accepted as i32,
            "pending" => FollowState::Pending as i32,
            "blocked" => FollowState::Blocked as i32,
            _ => FollowState::None as i32,
        }
    }

    async fn backfill_home_feed_for_follow(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        follower_id: &UserId,
        followee_id: &UserId,
    ) -> Result<(), Status> {
        sqlx::query(
            r#"
            INSERT INTO feed_entries (id, user_id, post_id, created_at)
            SELECT gen_random_uuid(), $1, p.id, p.created_at
            FROM posts p
            WHERE p.author_id = $2
              AND p.visibility = 'followers'
              AND NOT p.is_deleted
            ON CONFLICT (user_id, post_id) DO NOTHING
            "#,
        )
        .bind(follower_id.as_uuid())
        .bind(followee_id.as_uuid())
        .execute(&mut **tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        Ok(())
    }

    async fn remove_followee_posts_from_home_feed(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        follower_id: &UserId,
        followee_id: &UserId,
    ) -> Result<(), Status> {
        sqlx::query(
            r#"
            DELETE FROM feed_entries fe
            USING posts p
            WHERE fe.post_id = p.id
              AND fe.user_id = $1
              AND p.author_id = $2
              AND p.visibility = 'followers'
            "#,
        )
        .bind(follower_id.as_uuid())
        .bind(followee_id.as_uuid())
        .execute(&mut **tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        Ok(())
    }
}

#[tonic::async_trait]
impl FollowService for FollowServiceImpl {
    async fn follow(
        &self,
        request: Request<FollowRequest>,
    ) -> Result<Response<FollowResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id = UserId::parse(&req.target_user_id)
            .map_err(|_| Status::invalid_argument("invalid target_user_id"))?;

        if auth.user_id == target_id {
            return Err(Status::invalid_argument("cannot follow self"));
        }

        let existing =
            sqlx::query(r#"SELECT state FROM follows WHERE follower_id = $1 AND followee_id = $2"#)
                .bind(auth.user_id.as_uuid())
                .bind(target_id.as_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Status::internal("database error"))?;

        if let Some(row) = existing {
            let state: String = row.get(0);
            let proto_state = Self::state_to_proto(&state);
            if proto_state != FollowState::None as i32 {
                return Ok(Response::new(FollowResponse { state: proto_state }));
            }
        }

        let is_private: bool = sqlx::query(r#"SELECT is_private FROM profiles WHERE user_id = $1"#)
            .bind(target_id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?
            .map(|r| r.get::<bool, _>(0))
            .unwrap_or(true);

        let state = if is_private { "pending" } else { "accepted" };
        let accepted_at = if is_private {
            None::<chrono::DateTime<chrono::Utc>>
        } else {
            Some(chrono::Utc::now())
        };
        let mut follow_request_inserted = false;
        let follow_request_notifications_enabled = if state == "pending" {
            notification_preferences::is_enabled_for_notification_type(
                &self.pool,
                target_id,
                "follow_request",
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        } else {
            false
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal("database error"))?;

        sqlx::query(
            r#"
            INSERT INTO follows (follower_id, followee_id, state, accepted_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (follower_id, followee_id)
            DO UPDATE SET state = EXCLUDED.state, accepted_at = COALESCE(EXCLUDED.accepted_at, follows.accepted_at)
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .bind(target_id.as_uuid())
        .bind(state)
        .bind(accepted_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal("database error"))?;

        if state == "accepted" {
            sqlx::query(
                r#"
                UPDATE profiles SET follower_count = follower_count + 1
                WHERE user_id = $1
                "#,
            )
            .bind(target_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal("database error"))?;

            sqlx::query(
                r#"
                UPDATE profiles SET following_count = following_count + 1
                WHERE user_id = $1
                "#,
            )
            .bind(auth.user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal("database error"))?;

            Self::backfill_home_feed_for_follow(&mut tx, &auth.user_id, &target_id).await?;
        } else if follow_request_notifications_enabled {
            let inserted = sqlx::query(
                r#"
                INSERT INTO notifications (id, recipient_id, actor_id, notification_type, target_type, target_id, message, is_read, created_at)
                SELECT gen_random_uuid(), $1, $2, 'follow_request', 'user', $3, '', FALSE, NOW()
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM notifications
                    WHERE recipient_id = $1
                      AND actor_id = $2
                      AND notification_type = 'follow_request'
                      AND target_id = $3
                      AND NOT is_read
                )
                "#,
            )
            .bind(target_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal("database error"))?;
            follow_request_inserted = inserted.rows_affected() > 0;
        }

        tx.commit()
            .await
            .map_err(|e| Status::internal("database error"))?;

        if follow_request_inserted {
            if let Err(error) = self
                .push
                .send_follow_request_push(target_id, auth.user_id)
                .await
            {
                tracing::warn!(
                    error = %error,
                    recipient_id = %target_id,
                    actor_id = %auth.user_id,
                    "failed to send follow request push"
                );
            }
        }

        let proto_state = if state == "accepted" {
            FollowState::Accepted as i32
        } else {
            FollowState::Pending as i32
        };

        Ok(Response::new(FollowResponse { state: proto_state }))
    }

    async fn unfollow(
        &self,
        request: Request<UnfollowRequest>,
    ) -> Result<Response<UnfollowResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id = UserId::parse(&req.target_user_id)
            .map_err(|_| Status::invalid_argument("invalid target_user_id"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal("database error"))?;

        let row = sqlx::query(
            r#"
            DELETE FROM follows
            WHERE follower_id = $1 AND followee_id = $2
            RETURNING state
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .bind(target_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Status::internal("database error"))?;

        if let Some(row) = row {
            let state: String = row.get(0);
            if state == "accepted" {
                sqlx::query(
                    r#"UPDATE profiles SET follower_count = follower_count - 1 WHERE user_id = $1"#,
                )
                .bind(target_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(|e| Status::internal("database error"))?;

                sqlx::query(
                    r#"UPDATE profiles SET following_count = following_count - 1 WHERE user_id = $1"#,
                )
            .bind(auth.user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal("database error"))?;

                Self::remove_followee_posts_from_home_feed(&mut tx, &auth.user_id, &target_id)
                    .await?;
            } else if state == "pending" {
                // Treat canceling an outgoing follow request as resolving the stale request notification,
                // so a future re-request can generate a fresh notification and push.
                sqlx::query(
                    r#"
                    UPDATE notifications
                    SET is_read = TRUE
                    WHERE recipient_id = $1
                      AND actor_id = $2
                      AND notification_type = 'follow_request'
                      AND NOT is_read
                    "#,
                )
                .bind(target_id.as_uuid())
                .bind(auth.user_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(|e| Status::internal("database error"))?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| Status::internal("database error"))?;

        Ok(Response::new(UnfollowResponse {}))
    }

    async fn accept_follow_request(
        &self,
        request: Request<AcceptFollowRequestReq>,
    ) -> Result<Response<AcceptFollowRequestResp>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let follower_id = UserId::parse(&req.follower_user_id)
            .map_err(|_| Status::invalid_argument("invalid follower_user_id"))?;

        let follow_accepted_notifications_enabled =
            notification_preferences::is_enabled_for_notification_type(
                &self.pool,
                follower_id,
                "follow_accepted",
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal("database error"))?;

        let row = sqlx::query(
            r#"
            UPDATE follows
            SET state = 'accepted', accepted_at = NOW()
            WHERE follower_id = $1 AND followee_id = $2 AND state = 'pending'
            RETURNING 1
            "#,
        )
        .bind(follower_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Status::internal("database error"))?;

        if row.is_none() {
            return Err(Status::not_found("no pending follow request found"));
        }

        sqlx::query(
            r#"UPDATE profiles SET follower_count = follower_count + 1 WHERE user_id = $1"#,
        )
        .bind(auth.user_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal("database error"))?;

        sqlx::query(
            r#"UPDATE profiles SET following_count = following_count + 1 WHERE user_id = $1"#,
        )
        .bind(follower_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal("database error"))?;

        sqlx::query(
            r#"
            UPDATE notifications
            SET is_read = TRUE
            WHERE recipient_id = $1
              AND actor_id = $2
              AND notification_type = 'follow_request'
              AND NOT is_read
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .bind(follower_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal("database error"))?;

        Self::backfill_home_feed_for_follow(&mut tx, &follower_id, &auth.user_id).await?;

        let follow_accepted_inserted = if follow_accepted_notifications_enabled {
            let inserted = sqlx::query(
                r#"
                INSERT INTO notifications (id, recipient_id, actor_id, notification_type, target_type, target_id, message, is_read, created_at)
                SELECT gen_random_uuid(), $1, $2, 'follow_accepted', 'user', $3, '', FALSE, NOW()
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM notifications
                    WHERE recipient_id = $1
                      AND actor_id = $2
                      AND notification_type = 'follow_accepted'
                      AND target_id = $3
                      AND NOT is_read
                )
                "#,
            )
            .bind(follower_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal("database error"))?;
            inserted.rows_affected() > 0
        } else {
            false
        };

        tx.commit()
            .await
            .map_err(|e| Status::internal("database error"))?;

        if follow_accepted_inserted {
            if let Err(error) = self
                .push
                .send_follow_accepted_push(follower_id, auth.user_id)
                .await
            {
                tracing::warn!(
                    error = %error,
                    recipient_id = %follower_id,
                    actor_id = %auth.user_id,
                    "failed to send follow accepted push"
                );
            }
        }

        Ok(Response::new(AcceptFollowRequestResp {}))
    }

    async fn reject_follow_request(
        &self,
        request: Request<RejectFollowRequestReq>,
    ) -> Result<Response<RejectFollowRequestResp>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let follower_id = UserId::parse(&req.follower_user_id)
            .map_err(|_| Status::invalid_argument("invalid follower_user_id"))?;

        sqlx::query(
            r#"
            DELETE FROM follows
            WHERE follower_id = $1 AND followee_id = $2 AND state = 'pending'
            "#,
        )
        .bind(follower_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        sqlx::query(
            r#"
            UPDATE notifications
            SET is_read = TRUE
            WHERE recipient_id = $1
              AND actor_id = $2
              AND notification_type = 'follow_request'
              AND NOT is_read
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .bind(follower_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        Ok(Response::new(RejectFollowRequestResp {}))
    }

    async fn get_followers(
        &self,
        request: Request<GetFollowersRequest>,
    ) -> Result<Response<GetFollowersResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id =
            UserId::parse(&req.user_id).map_err(|_| Status::invalid_argument("invalid user_id"))?;

        let is_own = auth.user_id == target_id;
        if !is_own {
            let profile = sqlx::query(r#"SELECT is_private FROM profiles WHERE user_id = $1"#)
                .bind(target_id.as_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Status::internal("database error"))?;

            let is_private: bool = profile.map(|r| r.get(0)).unwrap_or(true);

            if is_private {
                let following = sqlx::query(
                    r#"
                    SELECT 1 FROM follows
                    WHERE follower_id = $1 AND followee_id = $2 AND state = 'accepted'
                    "#,
                )
                .bind(auth.user_id.as_uuid())
                .bind(target_id.as_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Status::internal("database error"))?;

                if following.is_none() {
                    return Err(Status::permission_denied(
                        "must follow to view followers of private account",
                    ));
                }
            }
        }

        let (page_size, cursor_str) = req
            .pagination
            .as_ref()
            .map_or((20, ""), |p| (p.page_size.clamp(1, 50), p.cursor.as_str()));
        let pagination = PaginationParams::from_proto(page_size, cursor_str);

        let (rows, has_more) = if let Some(cursor) = &pagination.cursor {
            let rows = sqlx::query(
                r#"
                SELECT u.id, u.username, u.display_name, p.avatar_media_id, f.created_at, f.follower_id,
                       EXISTS (SELECT 1 FROM follows af WHERE af.follower_id = $5 AND af.followee_id = f.follower_id AND af.state = 'accepted') as is_following
                FROM follows f
                JOIN users u ON f.follower_id = u.id
                LEFT JOIN profiles p ON u.id = p.user_id
                WHERE f.followee_id = $1 AND f.state = 'accepted'
                  AND u.account_state = 'active'
                  AND (f.created_at, f.follower_id) < ($2, $3)
                ORDER BY f.created_at DESC, f.follower_id DESC
                LIMIT $4
                "#,
            )
            .bind(target_id.as_uuid())
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .bind(pagination.limit + 1)
            .bind(auth.user_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let has_more = rows.len() > pagination.limit as usize;
            let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();
            (rows, has_more)
        } else {
            let rows = sqlx::query(
                r#"
                SELECT u.id, u.username, u.display_name, p.avatar_media_id, f.created_at, f.follower_id,
                       EXISTS (SELECT 1 FROM follows af WHERE af.follower_id = $3 AND af.followee_id = f.follower_id AND af.state = 'accepted') as is_following
                FROM follows f
                JOIN users u ON f.follower_id = u.id
                LEFT JOIN profiles p ON u.id = p.user_id
                WHERE f.followee_id = $1 AND f.state = 'accepted'
                  AND u.account_state = 'active'
                ORDER BY f.created_at DESC, f.follower_id DESC
                LIMIT $2
                "#,
            )
            .bind(target_id.as_uuid())
            .bind(pagination.limit + 1)
            .bind(auth.user_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let has_more = rows.len() > pagination.limit as usize;
            let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();
            (rows, has_more)
        };

        let followers: Vec<UserSummary> = rows
            .iter()
            .map(|row: &sqlx::postgres::PgRow| {
                let user_id: uuid::Uuid = row.get(0);
                let username: String = row.get(1);
                let display_name: String = row.get(2);
                let avatar_media_id: Option<uuid::Uuid> = row.get(3);
                let is_following: bool = row.get(6);
                let avatar_url = avatar_media_id
                    .map(|id| format!("/media/{}", id))
                    .unwrap_or_default();

                UserSummary {
                    user_id: user_id.to_string(),
                    username,
                    display_name,
                    avatar_url,
                    is_following,
                }
            })
            .collect();

        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            let created_at: chrono::DateTime<chrono::Utc> = last.get(4);
            let follower_id: uuid::Uuid = last.get(5);
            let cv = CursorValue {
                timestamp: created_at,
                id: follower_id,
            };
            PaginationParams::encode_cursor(&cv)
        } else {
            String::new()
        };

        Ok(Response::new(GetFollowersResponse {
            followers,
            pagination: Some(PaginationResponse {
                next_cursor,
                has_more,
                total_count: 0,
            }),
        }))
    }

    async fn get_following(
        &self,
        request: Request<GetFollowingRequest>,
    ) -> Result<Response<GetFollowingResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id =
            UserId::parse(&req.user_id).map_err(|_| Status::invalid_argument("invalid user_id"))?;

        let is_own = auth.user_id == target_id;
        if !is_own {
            let profile = sqlx::query(r#"SELECT is_private FROM profiles WHERE user_id = $1"#)
                .bind(target_id.as_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Status::internal("database error"))?;

            let is_private: bool = profile.map(|r| r.get(0)).unwrap_or(true);

            if is_private {
                let following = sqlx::query(
                    r#"
                    SELECT 1 FROM follows
                    WHERE follower_id = $1 AND followee_id = $2 AND state = 'accepted'
                    "#,
                )
                .bind(auth.user_id.as_uuid())
                .bind(target_id.as_uuid())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Status::internal("database error"))?;

                if following.is_none() {
                    return Err(Status::permission_denied(
                        "must follow to view following of private account",
                    ));
                }
            }
        }

        let (page_size, cursor_str) = req
            .pagination
            .as_ref()
            .map_or((20, ""), |p| (p.page_size.clamp(1, 50), p.cursor.as_str()));
        let pagination = PaginationParams::from_proto(page_size, cursor_str);

        let (rows, has_more) = if let Some(cursor) = &pagination.cursor {
            let rows = sqlx::query(
                r#"
                SELECT u.id, u.username, u.display_name, p.avatar_media_id, f.created_at, f.followee_id,
                       EXISTS (SELECT 1 FROM follows af WHERE af.follower_id = $5 AND af.followee_id = f.followee_id AND af.state = 'accepted') as is_following
                FROM follows f
                JOIN users u ON f.followee_id = u.id
                LEFT JOIN profiles p ON u.id = p.user_id
                WHERE f.follower_id = $1 AND f.state = 'accepted'
                  AND u.account_state = 'active'
                  AND (f.created_at, f.followee_id) < ($2, $3)
                ORDER BY f.created_at DESC, f.followee_id DESC
                LIMIT $4
                "#,
            )
            .bind(target_id.as_uuid())
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .bind(pagination.limit + 1)
            .bind(auth.user_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let has_more = rows.len() > pagination.limit as usize;
            let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();
            (rows, has_more)
        } else {
            let rows = sqlx::query(
                r#"
                SELECT u.id, u.username, u.display_name, p.avatar_media_id, f.created_at, f.followee_id,
                       EXISTS (SELECT 1 FROM follows af WHERE af.follower_id = $3 AND af.followee_id = f.followee_id AND af.state = 'accepted') as is_following
                FROM follows f
                JOIN users u ON f.followee_id = u.id
                LEFT JOIN profiles p ON u.id = p.user_id
                WHERE f.follower_id = $1 AND f.state = 'accepted'
                  AND u.account_state = 'active'
                ORDER BY f.created_at DESC, f.followee_id DESC
                LIMIT $2
                "#,
            )
            .bind(target_id.as_uuid())
            .bind(pagination.limit + 1)
            .bind(auth.user_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let has_more = rows.len() > pagination.limit as usize;
            let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();
            (rows, has_more)
        };

        let following: Vec<UserSummary> = rows
            .iter()
            .map(|row: &sqlx::postgres::PgRow| {
                let user_id: uuid::Uuid = row.get(0);
                let username: String = row.get(1);
                let display_name: String = row.get(2);
                let avatar_media_id: Option<uuid::Uuid> = row.get(3);
                let is_following: bool = row.get(6);
                let avatar_url = avatar_media_id
                    .map(|id| format!("/media/{}", id))
                    .unwrap_or_default();

                UserSummary {
                    user_id: user_id.to_string(),
                    username,
                    display_name,
                    avatar_url,
                    is_following,
                }
            })
            .collect();

        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            let created_at: chrono::DateTime<chrono::Utc> = last.get(4);
            let followee_id: uuid::Uuid = last.get(5);
            let cv = CursorValue {
                timestamp: created_at,
                id: followee_id,
            };
            PaginationParams::encode_cursor(&cv)
        } else {
            String::new()
        };

        Ok(Response::new(GetFollowingResponse {
            following,
            pagination: Some(PaginationResponse {
                next_cursor,
                has_more,
                total_count: 0,
            }),
        }))
    }

    async fn get_pending_requests(
        &self,
        request: Request<GetPendingRequestsRequest>,
    ) -> Result<Response<GetPendingRequestsResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let (page_size, cursor_str) = req
            .pagination
            .as_ref()
            .map_or((20, ""), |p| (p.page_size.clamp(1, 50), p.cursor.as_str()));
        let pagination = PaginationParams::from_proto(page_size, cursor_str);

        let (rows, has_more) = if let Some(cursor) = &pagination.cursor {
            let rows = sqlx::query(
                r#"
                SELECT u.id, u.username, u.display_name, p.avatar_media_id, f.created_at, f.follower_id
                FROM follows f
                JOIN users u ON f.follower_id = u.id
                LEFT JOIN profiles p ON u.id = p.user_id
                WHERE f.followee_id = $1 AND f.state = 'pending'
                  AND u.account_state = 'active'
                  AND (f.created_at, f.follower_id) < ($2, $3)
                ORDER BY f.created_at DESC, f.follower_id DESC
                LIMIT $4
                "#,
            )
            .bind(auth.user_id.as_uuid())
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .bind(pagination.limit + 1)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let has_more = rows.len() > pagination.limit as usize;
            let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();
            (rows, has_more)
        } else {
            let rows = sqlx::query(
                r#"
                SELECT u.id, u.username, u.display_name, p.avatar_media_id, f.created_at, f.follower_id
                FROM follows f
                JOIN users u ON f.follower_id = u.id
                LEFT JOIN profiles p ON u.id = p.user_id
                WHERE f.followee_id = $1 AND f.state = 'pending'
                  AND u.account_state = 'active'
                ORDER BY f.created_at DESC, f.follower_id DESC
                LIMIT $2
                "#,
            )
            .bind(auth.user_id.as_uuid())
            .bind(pagination.limit + 1)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let has_more = rows.len() > pagination.limit as usize;
            let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();
            (rows, has_more)
        };

        let requests: Vec<PendingFollowRequest> = rows
            .iter()
            .map(|row: &sqlx::postgres::PgRow| {
                let user_id: uuid::Uuid = row.get(0);
                let username: String = row.get(1);
                let display_name: String = row.get(2);
                let avatar_media_id: Option<uuid::Uuid> = row.get(3);
                let requested_at: chrono::DateTime<chrono::Utc> = row.get(4);
                let avatar_url = avatar_media_id
                    .map(|id| format!("/media/{}", id))
                    .unwrap_or_default();

                PendingFollowRequest {
                    user: Some(UserSummary {
                        user_id: user_id.to_string(),
                        username,
                        display_name,
                        avatar_url,
                        is_following: false,
                    }),
                    requested_at: Some(prost_types::Timestamp {
                        seconds: requested_at.timestamp(),
                        nanos: requested_at.timestamp_subsec_nanos() as i32,
                    }),
                }
            })
            .collect();

        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            let created_at: chrono::DateTime<chrono::Utc> = last.get(4);
            let follower_id: uuid::Uuid = last.get(5);
            let cv = CursorValue {
                timestamp: created_at,
                id: follower_id,
            };
            PaginationParams::encode_cursor(&cv)
        } else {
            String::new()
        };

        Ok(Response::new(GetPendingRequestsResponse {
            requests,
            pagination: Some(PaginationResponse {
                next_cursor,
                has_more,
                total_count: 0,
            }),
        }))
    }

    async fn get_follow_status(
        &self,
        request: Request<GetFollowStatusRequest>,
    ) -> Result<Response<GetFollowStatusResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id = UserId::parse(&req.target_user_id)
            .map_err(|_| Status::invalid_argument("invalid target_user_id"))?;

        let outgoing = sqlx::query(
            r#"
            SELECT state FROM follows
            WHERE follower_id = $1 AND followee_id = $2
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .bind(target_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        let incoming = sqlx::query(
            r#"
            SELECT state FROM follows
            WHERE follower_id = $1 AND followee_id = $2
            "#,
        )
        .bind(target_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        let outgoing_state = outgoing
            .map(|r| r.get::<String, _>(0))
            .map(|s| Self::state_to_proto(&s))
            .unwrap_or(FollowState::None as i32);

        let incoming_state = incoming
            .map(|r| r.get::<String, _>(0))
            .map(|s| Self::state_to_proto(&s))
            .unwrap_or(FollowState::None as i32);

        Ok(Response::new(GetFollowStatusResponse {
            outgoing: outgoing_state,
            incoming: incoming_state,
        }))
    }
}
