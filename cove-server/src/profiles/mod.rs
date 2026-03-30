//! Profile service and gRPC handler.

use crate::auth;
use crate::posts::PostServiceImpl;
use cove_common::auth_context::AuthContext;
use cove_common::id::{PostId, UserId};
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::common::{MediaReference, MediaType};
use cove_proto::cove::profile::{
    profile_service_server::ProfileService, GetPortalFeedRequest, GetPortalFeedResponse,
    GetProfileGridRequest, GetProfileGridResponse, GetProfilePortalsRequest,
    GetProfilePortalsResponse, GetProfileRequest, GetProfileResponse, PortalSummary,
    ProfileGridItem, ReorderPortalsRequest, ReorderPortalsResponse, UpdatePortalRequest,
    UpdatePortalResponse, UpdateProfileRequest, UpdateProfileResponse,
};
use prost_types::Timestamp;
use sqlx::{PgPool, Row};
use std::collections::HashSet;
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

    fn normalize_portal_name(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }

        Some(trimmed.chars().take(60).collect())
    }

    async fn can_view_profile_posts(
        &self,
        auth: &AuthContext,
        target_id: UserId,
    ) -> Result<bool, Status> {
        if auth.user_id == target_id {
            return Ok(true);
        }

        let profile_row = sqlx::query(r#"SELECT is_private FROM profiles WHERE user_id = $1"#)
            .bind(target_id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?;

        let profile = profile_row.ok_or_else(|| Status::not_found("profile not found"))?;
        let is_private: bool = profile.get(0);

        if !is_private {
            return Ok(true);
        }

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
        .map_err(|_| Status::internal("database error"))?;

        Ok(follow_row.is_some())
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

        let target_id =
            UserId::parse(&req.user_id).map_err(|_| Status::invalid_argument("invalid user_id"))?;

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

        let (bio_visible, follower_count_visible, following_count_visible, post_count_visible) =
            if can_see_full {
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
            row.map(|r| r.get::<String, _>(0) == "accepted")
                .unwrap_or(false)
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
            sqlx::query(
                r#"UPDATE profiles SET is_private = $1, updated_at = NOW() WHERE user_id = $2"#,
            )
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

        let target_id =
            UserId::parse(&req.user_id).map_err(|_| Status::invalid_argument("invalid user_id"))?;

        let can_view_posts = self.can_view_profile_posts(&auth, target_id).await?;
        if !can_view_posts {
            return Err(Status::permission_denied(
                "must follow to view profile grid of private account",
            ));
        }

        let (page_size, cursor_str) = req
            .pagination
            .as_ref()
            .map_or((20, ""), |p| (p.page_size.clamp(1, 50), p.cursor.as_str()));
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
                    Some("audio") => MediaType::Audio as i32,
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

    async fn get_profile_portals(
        &self,
        request: Request<GetProfilePortalsRequest>,
    ) -> Result<Response<GetProfilePortalsResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id =
            UserId::parse(&req.user_id).map_err(|_| Status::invalid_argument("invalid user_id"))?;
        let can_edit = auth.user_id == target_id;
        let can_view_posts = self.can_view_profile_posts(&auth, target_id).await?;
        if !can_view_posts {
            return Err(Status::permission_denied(
                "must follow to view profile portals of private account",
            ));
        }

        let contains_post_id = if can_edit && !req.post_id.trim().is_empty() {
            Some(
                PostId::parse(req.post_id.trim())
                    .map_err(|_| Status::invalid_argument("invalid post_id"))?,
            )
        } else {
            None
        };

        if let Some(post_id) = contains_post_id {
            let owns_post = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM posts
                    WHERE id = $1 AND author_id = $2 AND NOT is_deleted
                )
                "#,
            )
            .bind(post_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .fetch_one(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?;

            if !owns_post {
                return Err(Status::invalid_argument(
                    "post_id does not belong to viewer",
                ));
            }
        }

        let include_private_posts = can_edit;
        let contains_post_uuid: Option<uuid::Uuid> = contains_post_id.map(Into::into);

        let rows = sqlx::query(
            r#"
            SELECT po.id, po.name, po.updated_at,
                   COALESCE(counts.post_count, 0) AS post_count,
                   cover.id AS cover_media_id,
                   cover.media_type AS cover_media_type,
                   cover.width AS cover_width,
                   cover.height AS cover_height,
                   cover.aspect_ratio AS cover_aspect_ratio,
                   cover.duration_seconds AS cover_duration_seconds,
                   CASE
                       WHEN $3::uuid IS NULL THEN FALSE
                       ELSE EXISTS(
                           SELECT 1
                           FROM portal_posts ppp
                           WHERE ppp.portal_id = po.id AND ppp.post_id = $3
                       )
                   END AS contains_post
            FROM portals po
            LEFT JOIN LATERAL (
                SELECT COUNT(*)::int AS post_count
                FROM portal_posts pp
                JOIN posts p ON p.id = pp.post_id AND NOT p.is_deleted
                WHERE pp.portal_id = po.id
                  AND ($2 OR p.visibility != 'private')
            ) counts ON true
            LEFT JOIN LATERAL (
                SELECT m.id, m.media_type, m.width, m.height, m.aspect_ratio, m.duration_seconds
                FROM portal_posts pp
                JOIN posts p ON p.id = pp.post_id AND NOT p.is_deleted
                JOIN media_items m ON m.post_id = p.id AND m.processing_state = 'completed'
                WHERE pp.portal_id = po.id
                  AND ($2 OR p.visibility != 'private')
                ORDER BY pp.added_at DESC, p.created_at DESC, m.order_index ASC, m.created_at ASC
                LIMIT 1
            ) cover ON true
            WHERE po.owner_id = $1
            ORDER BY po.order_index ASC, po.updated_at DESC, po.id DESC
            "#,
        )
        .bind(target_id.as_uuid())
        .bind(include_private_posts)
        .bind(contains_post_uuid)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        let portals: Vec<PortalSummary> = rows
            .iter()
            .map(|row| {
                let portal_id: uuid::Uuid = row.get(0);
                let name: String = row.get(1);
                let updated_at: chrono::DateTime<chrono::Utc> = row.get(2);
                let post_count: i32 = row.get(3);
                let cover_media_id: Option<uuid::Uuid> = row.get(4);
                let cover_media_type: Option<String> = row.get(5);
                let cover_width: Option<i32> = row.get(6);
                let cover_height: Option<i32> = row.get(7);
                let cover_aspect_ratio: Option<f64> = row.get(8);
                let cover_duration_seconds: Option<i32> = row.get(9);
                let contains_post: bool = row.get(10);

                let cover = cover_media_id.map(|media_id| {
                    let media_type = match cover_media_type.as_deref() {
                        Some("video") => MediaType::Video as i32,
                        Some("audio") => MediaType::Audio as i32,
                        _ => MediaType::Photo as i32,
                    };

                    MediaReference {
                        media_id: media_id.to_string(),
                        media_type,
                        url: format!("/media/{}", media_id),
                        width: cover_width.unwrap_or(0),
                        height: cover_height.unwrap_or(0),
                        aspect_ratio: cover_aspect_ratio.unwrap_or(1.0),
                        duration_seconds: cover_duration_seconds.unwrap_or(0),
                        thumbnail_url: format!("/media/{}/thumb", media_id),
                    }
                });

                PortalSummary {
                    portal_id: portal_id.to_string(),
                    name,
                    cover,
                    post_count,
                    updated_at: Some(Timestamp {
                        seconds: updated_at.timestamp(),
                        nanos: updated_at.timestamp_subsec_nanos() as i32,
                    }),
                    contains_post,
                }
            })
            .collect();

        Ok(Response::new(GetProfilePortalsResponse {
            portals,
            can_edit,
        }))
    }

    async fn update_portal(
        &self,
        request: Request<UpdatePortalRequest>,
    ) -> Result<Response<UpdatePortalResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let portal_id = uuid::Uuid::parse_str(req.portal_id.trim())
            .map_err(|_| Status::invalid_argument("invalid portal_id"))?;
        let name = Self::normalize_portal_name(&req.name)
            .ok_or_else(|| Status::invalid_argument("name is required"))?;

        let row = sqlx::query(
            r#"
            UPDATE portals
            SET name = $1, updated_at = NOW()
            WHERE id = $2 AND owner_id = $3
            RETURNING id, name, updated_at
            "#,
        )
        .bind(&name)
        .bind(portal_id)
        .bind(auth.user_id.as_uuid())
        .fetch_optional(&self.pool)
        .await;

        let row = match row {
            Ok(Some(row)) => row,
            Ok(None) => return Err(Status::not_found("portal not found")),
            Err(sqlx::Error::Database(db_error))
                if db_error.constraint() == Some("idx_portals_owner_name") =>
            {
                return Err(Status::already_exists("portal name already exists"));
            }
            Err(_) => return Err(Status::internal("database error")),
        };

        let updated_id: uuid::Uuid = row.get(0);
        let updated_name: String = row.get(1);
        let updated_at: chrono::DateTime<chrono::Utc> = row.get(2);

        Ok(Response::new(UpdatePortalResponse {
            portal_id: updated_id.to_string(),
            name: updated_name,
            updated_at: Some(Timestamp {
                seconds: updated_at.timestamp(),
                nanos: updated_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn reorder_portals(
        &self,
        request: Request<ReorderPortalsRequest>,
    ) -> Result<Response<ReorderPortalsResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        if req.portal_ids.is_empty() {
            return Err(Status::invalid_argument("portal_ids is required"));
        }

        let mut ordered_portal_ids = Vec::with_capacity(req.portal_ids.len());
        let mut requested_portal_ids = HashSet::with_capacity(req.portal_ids.len());
        for raw_portal_id in &req.portal_ids {
            let portal_id = uuid::Uuid::parse_str(raw_portal_id.trim())
                .map_err(|_| Status::invalid_argument("invalid portal_id"))?;
            if !requested_portal_ids.insert(portal_id) {
                return Err(Status::invalid_argument(
                    "portal_ids must not contain duplicates",
                ));
            }
            ordered_portal_ids.push(portal_id);
        }

        let existing_rows = sqlx::query(
            r#"
            SELECT id
            FROM portals
            WHERE owner_id = $1
            ORDER BY order_index ASC, updated_at DESC, id DESC
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        if existing_rows.len() != ordered_portal_ids.len() {
            return Err(Status::invalid_argument(
                "portal_ids must include every portal exactly once",
            ));
        }

        let existing_portal_ids: HashSet<uuid::Uuid> = existing_rows
            .into_iter()
            .map(|row| row.get::<uuid::Uuid, _>(0))
            .collect();

        if existing_portal_ids != requested_portal_ids {
            return Err(Status::invalid_argument(
                "portal_ids must include every portal exactly once",
            ));
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::internal("database error"))?;

        for (position, portal_id) in ordered_portal_ids.iter().enumerate() {
            sqlx::query(
                r#"
                UPDATE portals
                SET order_index = $1
                WHERE id = $2 AND owner_id = $3
                "#,
            )
            .bind(position as i32)
            .bind(portal_id)
            .bind(auth.user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|_| Status::internal("database error"))?;
        }

        tx.commit()
            .await
            .map_err(|_| Status::internal("database error"))?;

        Ok(Response::new(ReorderPortalsResponse {}))
    }

    async fn get_portal_feed(
        &self,
        request: Request<GetPortalFeedRequest>,
    ) -> Result<Response<GetPortalFeedResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id =
            UserId::parse(&req.user_id).map_err(|_| Status::invalid_argument("invalid user_id"))?;
        let portal_id = uuid::Uuid::parse_str(req.portal_id.trim())
            .map_err(|_| Status::invalid_argument("invalid portal_id"))?;
        let can_view_posts = self.can_view_profile_posts(&auth, target_id).await?;
        if !can_view_posts {
            return Err(Status::permission_denied(
                "must follow to view portal feed of private account",
            ));
        }

        let include_private_posts = auth.user_id == target_id;

        let portal_row = sqlx::query(
            r#"
            SELECT po.id, po.name, po.updated_at,
                   COALESCE(counts.post_count, 0) AS post_count,
                   cover.id AS cover_media_id,
                   cover.media_type AS cover_media_type,
                   cover.width AS cover_width,
                   cover.height AS cover_height,
                   cover.aspect_ratio AS cover_aspect_ratio,
                   cover.duration_seconds AS cover_duration_seconds
            FROM portals po
            LEFT JOIN LATERAL (
                SELECT COUNT(*)::int AS post_count
                FROM portal_posts pp
                JOIN posts p ON p.id = pp.post_id AND NOT p.is_deleted
                WHERE pp.portal_id = po.id
                  AND ($3 OR p.visibility != 'private')
            ) counts ON true
            LEFT JOIN LATERAL (
                SELECT m.id, m.media_type, m.width, m.height, m.aspect_ratio, m.duration_seconds
                FROM portal_posts pp
                JOIN posts p ON p.id = pp.post_id AND NOT p.is_deleted
                JOIN media_items m ON m.post_id = p.id AND m.processing_state = 'completed'
                WHERE pp.portal_id = po.id
                  AND ($3 OR p.visibility != 'private')
                ORDER BY pp.added_at DESC, p.created_at DESC, m.order_index ASC, m.created_at ASC
                LIMIT 1
            ) cover ON true
            WHERE po.id = $1 AND po.owner_id = $2
            "#,
        )
        .bind(portal_id)
        .bind(target_id.as_uuid())
        .bind(include_private_posts)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?
        .ok_or_else(|| Status::not_found("portal not found"))?;

        let portal_summary = {
            let id: uuid::Uuid = portal_row.get(0);
            let name: String = portal_row.get(1);
            let updated_at: chrono::DateTime<chrono::Utc> = portal_row.get(2);
            let post_count: i32 = portal_row.get(3);
            let cover_media_id: Option<uuid::Uuid> = portal_row.get(4);
            let cover_media_type: Option<String> = portal_row.get(5);
            let cover_width: Option<i32> = portal_row.get(6);
            let cover_height: Option<i32> = portal_row.get(7);
            let cover_aspect_ratio: Option<f64> = portal_row.get(8);
            let cover_duration_seconds: Option<i32> = portal_row.get(9);

            let cover = cover_media_id.map(|media_id| {
                let media_type = match cover_media_type.as_deref() {
                    Some("video") => MediaType::Video as i32,
                    Some("audio") => MediaType::Audio as i32,
                    _ => MediaType::Photo as i32,
                };

                MediaReference {
                    media_id: media_id.to_string(),
                    media_type,
                    url: format!("/media/{}", media_id),
                    width: cover_width.unwrap_or(0),
                    height: cover_height.unwrap_or(0),
                    aspect_ratio: cover_aspect_ratio.unwrap_or(1.0),
                    duration_seconds: cover_duration_seconds.unwrap_or(0),
                    thumbnail_url: format!("/media/{}/thumb", media_id),
                }
            });

            PortalSummary {
                portal_id: id.to_string(),
                name,
                cover,
                post_count,
                updated_at: Some(Timestamp {
                    seconds: updated_at.timestamp(),
                    nanos: updated_at.timestamp_subsec_nanos() as i32,
                }),
                contains_post: false,
            }
        };

        let (page_size, cursor_str) = req
            .pagination
            .as_ref()
            .map_or((20, ""), |p| (p.page_size.clamp(1, 50), p.cursor.as_str()));
        let pagination = PaginationParams::from_proto(page_size, cursor_str);
        let limit_plus_one = pagination.limit as i64 + 1;

        let rows = if let Some(cursor) = &pagination.cursor {
            sqlx::query(
                r#"
                SELECT pp.added_at, p.id, p.author_id, p.caption, p.visibility, p.like_count,
                       p.comment_count, p.share_count, p.created_at, p.edited_at,
                       p.location_lat, p.location_lng, p.location_name,
                       EXISTS(
                           SELECT 1
                           FROM likes l
                           WHERE l.user_id = $3 AND l.post_id = p.id
                       ) AS liked_by_viewer
                FROM portal_posts pp
                JOIN posts p ON p.id = pp.post_id AND NOT p.is_deleted
                WHERE pp.portal_id = $1
                  AND p.author_id = $2
                  AND ($4 OR p.visibility != 'private')
                  AND (pp.added_at, p.id) < ($5, $6)
                ORDER BY pp.added_at DESC, p.id DESC
                LIMIT $7
                "#,
            )
            .bind(portal_id)
            .bind(target_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .bind(include_private_posts)
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .bind(limit_plus_one)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT pp.added_at, p.id, p.author_id, p.caption, p.visibility, p.like_count,
                       p.comment_count, p.share_count, p.created_at, p.edited_at,
                       p.location_lat, p.location_lng, p.location_name,
                       EXISTS(
                           SELECT 1
                           FROM likes l
                           WHERE l.user_id = $3 AND l.post_id = p.id
                       ) AS liked_by_viewer
                FROM portal_posts pp
                JOIN posts p ON p.id = pp.post_id AND NOT p.is_deleted
                WHERE pp.portal_id = $1
                  AND p.author_id = $2
                  AND ($4 OR p.visibility != 'private')
                ORDER BY pp.added_at DESC, p.id DESC
                LIMIT $5
                "#,
            )
            .bind(portal_id)
            .bind(target_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .bind(include_private_posts)
            .bind(limit_plus_one)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|_| Status::internal("database error"))?;

        let has_more = rows.len() > pagination.limit as usize;
        let rows: Vec<_> = rows.into_iter().take(pagination.limit as usize).collect();

        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            let post_id: uuid::Uuid = row.get(1);
            let author_id: uuid::Uuid = row.get(2);
            let caption: String = row.get(3);
            let visibility: String = row.get(4);
            let like_count: i32 = row.get(5);
            let comment_count: i32 = row.get(6);
            let share_count: i32 = row.get(7);
            let created_at: chrono::DateTime<chrono::Utc> = row.get(8);
            let edited_at: Option<chrono::DateTime<chrono::Utc>> = row.get(9);
            let location_lat: Option<f64> = row.get(10);
            let location_lng: Option<f64> = row.get(11);
            let location_name: Option<String> = row.get(12);
            let liked_by_viewer: bool = row.get(13);

            let detail = PostServiceImpl::build_post_detail(
                &self.pool,
                PostId::from_uuid(post_id),
                UserId::from_uuid(author_id),
                &caption,
                &visibility,
                like_count,
                comment_count,
                share_count,
                created_at,
                edited_at,
                liked_by_viewer,
                location_lat,
                location_lng,
                location_name,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            items.push(detail);
        }

        let next_cursor = if has_more && !rows.is_empty() {
            let last = rows.last().unwrap();
            let added_at: chrono::DateTime<chrono::Utc> = last.get(0);
            let post_id: uuid::Uuid = last.get(1);
            let cursor_value = CursorValue {
                timestamp: added_at,
                id: post_id,
            };
            PaginationParams::encode_cursor(&cursor_value)
        } else {
            String::new()
        };

        Ok(Response::new(GetPortalFeedResponse {
            portal: Some(portal_summary),
            items,
            pagination: Some(cove_proto::cove::common::PaginationResponse {
                next_cursor,
                has_more,
                total_count: 0,
            }),
        }))
    }
}
