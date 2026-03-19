//! Profile service and gRPC handler.

use crate::auth;
use cove_common::auth_context::AuthContext;
use cove_common::id::UserId;
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::common::{MediaReference, MediaType};
use cove_proto::cove::profile::{
    profile_service_server::ProfileService, GetProfileGridRequest, GetProfileGridResponse,
    GetProfileRequest, GetProfileResponse, ProfileGridItem, UpdateProfileRequest,
    UpdateProfileResponse,
};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};

/// Profile service implementation.
pub struct ProfileServiceImpl {
    pool: PgPool,
    jwt_secret: String,
}

impl ProfileServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String) -> Self {
        Self { pool, jwt_secret }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }
}

#[tonic::async_trait]
impl ProfileService for ProfileServiceImpl {
    async fn get_profile(
        &self,
        request: Request<GetProfileRequest>,
    ) -> Result<Response<GetProfileResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id = UserId::parse(&req.user_id)
            .map_err(|_| Status::invalid_argument("invalid user_id"))?;

        let user_row = sqlx::query(
            r#"
            SELECT u.id, u.username, u.display_name
            FROM users u
            WHERE u.id = $1 AND u.account_state = 'active'
            "#,
        )
        .bind(target_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        let user_row = user_row.ok_or_else(|| Status::not_found("user not found"))?;

        let user_id: uuid::Uuid = user_row.get(0);
        let username: String = user_row.get(1);
        let display_name: String = user_row.get(2);

        let profile_row = sqlx::query(
            r#"
            SELECT bio, avatar_media_id, is_private, follower_count, following_count, post_count
            FROM profiles
            WHERE user_id = $1
            "#,
        )
        .bind(target_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        let profile = profile_row.ok_or_else(|| Status::not_found("profile not found"))?;

        let bio: String = profile.get(0);
        let avatar_media_id: Option<uuid::Uuid> = profile.get(1);
        let is_private: bool = profile.get(2);
        let follower_count: i32 = profile.get(3);
        let following_count: i32 = profile.get(4);
        let post_count: i32 = profile.get(5);

        let is_own_profile = auth.user_id == target_id;

        let mut can_see_full = is_own_profile;
        if !is_own_profile && is_private {
            let follow_row = sqlx::query(
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
            can_see_full = follow_row.is_some();
        }

        let (bio_visible, follower_count_visible, following_count_visible, post_count_visible) = if can_see_full {
            (bio.clone(), follower_count, following_count, post_count)
        } else {
            (String::new(), 0, 0, 0)
        };

        let is_following = if is_own_profile {
            false
        } else {
            let row = sqlx::query(
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
            row.map(|r| r.get::<String, _>(0) == "accepted").unwrap_or(false)
        };

        let is_followed_by = if is_own_profile {
            false
        } else {
            let row = sqlx::query(
                r#"
                SELECT 1 FROM follows
                WHERE follower_id = $1 AND followee_id = $2 AND state = 'accepted'
                "#,
            )
            .bind(target_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;
            row.is_some()
        };

        let avatar_url = avatar_media_id
            .map(|id| format!("/media/{}", id))
            .unwrap_or_default();

        Ok(Response::new(GetProfileResponse {
            user_id: user_id.to_string(),
            username,
            display_name,
            bio: bio_visible,
            avatar_url,
            follower_count: follower_count_visible,
            following_count: following_count_visible,
            post_count: post_count_visible,
            is_private,
            is_following,
            is_followed_by,
            is_own_profile,
        }))
    }

    async fn update_profile(
        &self,
        request: Request<UpdateProfileRequest>,
    ) -> Result<Response<UpdateProfileResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

        if let Some(display_name) = &req.display_name {
            sqlx::query(r#"UPDATE users SET display_name = $1, updated_at = NOW() WHERE id = $2"#)
                .bind(display_name)
                .bind(auth.user_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(|_| Status::internal("database error"))?;
        }
        if let Some(bio) = &req.bio {
            sqlx::query(r#"UPDATE profiles SET bio = $1, updated_at = NOW() WHERE user_id = $2"#)
                .bind(bio)
                .bind(auth.user_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(|_| Status::internal("database error"))?;
        }
        if let Some(avatar_media_id) = &req.avatar_media_id {
            if avatar_media_id.is_empty() {
                sqlx::query(r#"UPDATE profiles SET avatar_media_id = NULL, updated_at = NOW() WHERE user_id = $1"#)
                    .bind(auth.user_id.as_uuid())
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?;
            } else {
                let parsed = uuid::Uuid::parse_str(avatar_media_id)
                    .map_err(|_| Status::invalid_argument("invalid avatar_media_id"))?;
                sqlx::query(r#"UPDATE profiles SET avatar_media_id = $1, updated_at = NOW() WHERE user_id = $2"#)
                    .bind(parsed)
                    .bind(auth.user_id.as_uuid())
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| Status::internal("database error"))?;
            }
        }
        if let Some(is_private) = req.is_private {
            sqlx::query(r#"UPDATE profiles SET is_private = $1, updated_at = NOW() WHERE user_id = $2"#)
                .bind(is_private)
                .bind(auth.user_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(|_| Status::internal("database error"))?;
        }

        let row = sqlx::query(
            r#"
            SELECT p.user_id, u.display_name, p.bio, p.is_private,
                   COALESCE(p.avatar_media_id::text, '')
            FROM profiles p
            JOIN users u ON p.user_id = u.id
            WHERE p.user_id = $1
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| Status::internal("database error"))?;

        let row = row.ok_or_else(|| Status::not_found("profile not found"))?;

        tx.commit()
            .await
            .map_err(|_| Status::internal("database error"))?;
        let user_id: uuid::Uuid = row.get(0);
        let display_name: String = row.get(1);
        let bio: String = row.get(2);
        let is_private: bool = row.get(3);
        let avatar_media_id_str: String = row.get(4);

        let avatar_url = if avatar_media_id_str.is_empty() {
            String::new()
        } else {
            format!("/media/{}", avatar_media_id_str)
        };

        Ok(Response::new(UpdateProfileResponse {
            user_id: user_id.to_string(),
            display_name,
            bio,
            avatar_url,
            is_private,
        }))
    }

    async fn get_profile_grid(
        &self,
        request: Request<GetProfileGridRequest>,
    ) -> Result<Response<GetProfileGridResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id = UserId::parse(&req.user_id)
            .map_err(|_| Status::invalid_argument("invalid user_id"))?;

        let is_own_profile = auth.user_id == target_id;

        if !is_own_profile {
            let profile_row = sqlx::query(
                r#"SELECT is_private FROM profiles WHERE user_id = $1"#,
            )
            .bind(target_id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let profile = profile_row.ok_or_else(|| Status::not_found("profile not found"))?;
            let is_private: bool = profile.get(0);

            if is_private {
                let follow_row = sqlx::query(
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

                if follow_row.is_none() {
                    return Err(Status::permission_denied(
                        "must follow to view profile grid of private account",
                    ));
                }
            }
        }

        let (page_size, cursor_str) = req.pagination.as_ref().map_or((20, ""), |p| {
            (p.page_size.clamp(1, 50), p.cursor.as_str())
        });
        let pagination = PaginationParams::from_proto(page_size, cursor_str);

        let (rows, has_more) = if let Some(cursor) = &pagination.cursor {
            let rows = sqlx::query(
                r#"
                SELECT p.id, p.created_at, p.post_type,
                       (SELECT m.id FROM media_items m
                        WHERE m.post_id = p.id
                        ORDER BY m.order_index ASC, m.created_at ASC
                        LIMIT 1) as first_media_id,
                       (SELECT m.media_type FROM media_items m
                        WHERE m.post_id = p.id
                        ORDER BY m.order_index ASC, m.created_at ASC
                        LIMIT 1) as first_media_type,
                       (SELECT COUNT(*)::int FROM media_items m WHERE m.post_id = p.id) as media_count
                FROM posts p
                WHERE p.author_id = $1 AND NOT p.is_deleted
                  AND (p.created_at, p.id) < ($2, $3)
                ORDER BY p.created_at DESC, p.id DESC
                LIMIT $4
                "#,
            )
            .bind(target_id.as_uuid())
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
                SELECT p.id, p.created_at, p.post_type,
                       (SELECT m.id FROM media_items m
                        WHERE m.post_id = p.id
                        ORDER BY m.order_index ASC, m.created_at ASC
                        LIMIT 1) as first_media_id,
                       (SELECT m.media_type FROM media_items m
                        WHERE m.post_id = p.id
                        ORDER BY m.order_index ASC, m.created_at ASC
                        LIMIT 1) as first_media_type,
                       (SELECT COUNT(*)::int FROM media_items m WHERE m.post_id = p.id) as media_count
                FROM posts p
                WHERE p.author_id = $1 AND NOT p.is_deleted
                ORDER BY p.created_at DESC, p.id DESC
                LIMIT $2
                "#,
            )
            .bind(target_id.as_uuid())
            .bind(pagination.limit + 1)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Status::internal("database error"))?;

            let has_more = rows.len() > pagination.limit as usize;
            let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();
            (rows, has_more)
        };

        let items: Vec<ProfileGridItem> = rows
            .iter()
            .map(|row| {
                let post_id: uuid::Uuid = row.get(0);
                let post_type: String = row.get(2);
                let first_media_id: Option<uuid::Uuid> = row.get(3);
                let first_media_type: Option<String> = row.get(4);
                let media_count: i32 = row.get(5);

                let media_type = match first_media_type.as_deref() {
                    Some("video") => MediaType::Video as i32,
                    _ => MediaType::Photo as i32,
                };

                let thumbnail = first_media_id.map(|id| MediaReference {
                    media_id: id.to_string(),
                    media_type,
                    url: format!("/media/{}", id),
                    width: 0,
                    height: 0,
                    aspect_ratio: 1.0,
                    duration_seconds: 0,
                    thumbnail_url: format!("/media/{}/thumb", id),
                });

                ProfileGridItem {
                    post_id: post_id.to_string(),
                    thumbnail,
                    is_video: post_type == "video",
                    is_multi: media_count > 1,
                }
            })
            .collect();

        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            let created_at: chrono::DateTime<chrono::Utc> = last.get(1);
            let post_id: uuid::Uuid = last.get(0);
            let cv = CursorValue {
                timestamp: created_at,
                id: post_id,
            };
            cove_common::pagination::PaginationParams::encode_cursor(&cv)
        } else {
            String::new()
        };

        let pagination_resp = cove_proto::cove::common::PaginationResponse {
            next_cursor,
            has_more,
            total_count: 0,
        };

        Ok(Response::new(GetProfileGridResponse {
            items,
            pagination: Some(pagination_resp),
        }))
    }
}
