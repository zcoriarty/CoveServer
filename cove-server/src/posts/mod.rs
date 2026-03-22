//! Post service and gRPC handler.

use crate::auth;
use cove_common::error::{CoveError, CoveResult};
use cove_common::id::{FeedEntryId, MediaId, PostId, UserId};
use cove_proto::cove::common::{Location, MediaReference, MediaType, UserSummary, Visibility};
use cove_proto::cove::post::{
    post_service_server::PostService, AddPostToPortalRequest, AddPostToPortalResponse,
    CreatePostRequest, CreatePostResponse, DeletePostRequest, DeletePostResponse,
    EditCaptionRequest, EditCaptionResponse, GetPostRequest, GetPostResponse, PostDetail,
    RemovePostFromPortalRequest, RemovePostFromPortalResponse,
};
use prost_types::Timestamp;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tonic::{Request, Response, Status};

/// Post service implementation.
pub struct PostServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
}

impl PostServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String) -> Self {
        Self { pool, jwt_secret }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<cove_common::auth_context::AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }

    fn normalize_portal_name(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }

        Some(trimmed.chars().take(60).collect())
    }

    async fn ensure_post_owned_by_user(
        tx: &mut Transaction<'_, Postgres>,
        post_id: PostId,
        owner_id: UserId,
    ) -> Result<(), Status> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM posts
                WHERE id = $1 AND author_id = $2 AND NOT is_deleted
            )
            "#,
        )
        .bind(post_id.as_uuid())
        .bind(owner_id.as_uuid())
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        if !exists {
            return Err(Status::permission_denied(
                "only the post author can manage portals for this post",
            ));
        }

        Ok(())
    }

    async fn resolve_or_create_portal(
        tx: &mut Transaction<'_, Postgres>,
        owner_id: UserId,
        portal_id_raw: &str,
        portal_name_raw: &str,
    ) -> Result<Option<(uuid::Uuid, String)>, Status> {
        let portal_id_trimmed = portal_id_raw.trim();
        let portal_name = Self::normalize_portal_name(portal_name_raw);

        if !portal_id_trimmed.is_empty() && portal_name.is_some() {
            return Err(Status::invalid_argument(
                "provide either portal_id or portal_name, not both",
            ));
        }

        if !portal_id_trimmed.is_empty() {
            let parsed_portal_id = uuid::Uuid::parse_str(portal_id_trimmed)
                .map_err(|_| Status::invalid_argument("invalid portal_id"))?;
            let row = sqlx::query(
                r#"
                SELECT id, name
                FROM portals
                WHERE id = $1 AND owner_id = $2
                "#,
            )
            .bind(parsed_portal_id)
            .bind(owner_id.as_uuid())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("portal not found"))?;

            let portal_id: uuid::Uuid = row.get(0);
            let name: String = row.get(1);
            return Ok(Some((portal_id, name)));
        }

        if let Some(portal_name) = portal_name {
            let created_portal_id = uuid::Uuid::now_v7();
            let row = sqlx::query(
                r#"
                INSERT INTO portals (id, owner_id, name, order_index, created_at, updated_at)
                VALUES (
                    $1,
                    $2,
                    $3,
                    COALESCE(
                        (SELECT MAX(order_index) + 1 FROM portals WHERE owner_id = $2),
                        0
                    ),
                    NOW(),
                    NOW()
                )
                ON CONFLICT (owner_id, lower(name))
                DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
                RETURNING id, name
                "#,
            )
            .bind(created_portal_id)
            .bind(owner_id.as_uuid())
            .bind(&portal_name)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            let portal_id: uuid::Uuid = row.get(0);
            let name: String = row.get(1);
            return Ok(Some((portal_id, name)));
        }

        Ok(None)
    }

    /// Assembles a PostDetail proto from DB rows.
    pub async fn build_post_detail(
        pool: &PgPool,
        post_id: PostId,
        author_id: UserId,
        caption: &str,
        visibility: &str,
        like_count: i32,
        comment_count: i32,
        share_count: i32,
        created_at: chrono::DateTime<chrono::Utc>,
        edited_at: Option<chrono::DateTime<chrono::Utc>>,
        liked_by_viewer: bool,
        location_lat: Option<f64>,
        location_lng: Option<f64>,
        location_name: Option<String>,
    ) -> CoveResult<PostDetail> {
        let author_row = sqlx::query(
            r#"
            SELECT u.id, u.username, u.display_name, p.avatar_media_id
            FROM users u
            LEFT JOIN profiles p ON p.user_id = u.id
            WHERE u.id = $1
            "#,
        )
        .bind(author_id.as_uuid())
        .fetch_optional(pool)
        .await
        .map_err(|e| CoveError::Database(e.to_string()))?
        .ok_or_else(|| CoveError::NotFound("author not found".into()))?;

        let author_user_id: uuid::Uuid = author_row.get(0);
        let username: String = author_row.get(1);
        let display_name: String = author_row.get(2);
        let avatar_media_id: Option<uuid::Uuid> = author_row.get(3);
        let avatar_url = avatar_media_id
            .map(|media_id| format!("/media/{}", media_id))
            .unwrap_or_default();

        let author = UserSummary {
            user_id: author_user_id.to_string(),
            username,
            display_name,
            avatar_url,
            is_following: false,
        };

        let media_rows = sqlx::query(
            r#"
            SELECT id, media_type, width, height, aspect_ratio, duration_seconds
            FROM media_items
            WHERE post_id = $1 AND processing_state = 'completed'
            ORDER BY order_index ASC, created_at ASC
            "#,
        )
        .bind(post_id.as_uuid())
        .fetch_all(pool)
        .await
        .map_err(|e| CoveError::Database(e.to_string()))?;

        let media_refs: Vec<MediaReference> = media_rows
            .iter()
            .map(|r| {
                let id: uuid::Uuid = r.get(0);
                let mt: String = r.get(1);
                let w: i32 = r.get(2);
                let h: i32 = r.get(3);
                let ar: f64 = r.get(4);
                let dur: i32 = r.get(5);
                MediaReference {
                    media_id: id.to_string(),
                    media_type: media_type_from_str(&mt),
                    url: String::new(),
                    width: w,
                    height: h,
                    aspect_ratio: ar,
                    duration_seconds: dur,
                    thumbnail_url: String::new(),
                }
            })
            .collect();

        let visibility_proto = match visibility {
            "followers" => Visibility::Followers as i32,
            "private" => Visibility::Private as i32,
            _ => Visibility::Unspecified as i32,
        };

        let location = match (location_lat, location_lng) {
            (Some(lat), Some(lng)) => Some(Location {
                latitude: lat,
                longitude: lng,
                display_name: location_name.unwrap_or_default(),
            }),
            _ => None,
        };

        Ok(PostDetail {
            post_id: post_id.to_string(),
            author: Some(author),
            caption: caption.to_string(),
            media: media_refs,
            like_count,
            comment_count,
            share_count,
            liked_by_viewer,
            visibility: visibility_proto,
            created_at: Some(Timestamp {
                seconds: created_at.timestamp(),
                nanos: created_at.timestamp_subsec_nanos() as i32,
            }),
            edited_at: edited_at.map(|t| Timestamp {
                seconds: t.timestamp(),
                nanos: t.timestamp_subsec_nanos() as i32,
            }),
            location,
        })
    }
}

fn media_type_from_str(s: &str) -> i32 {
    match s {
        "photo" => MediaType::Photo as i32,
        "video" => MediaType::Video as i32,
        _ => MediaType::Unspecified as i32,
    }
}

#[tonic::async_trait]
impl PostService for PostServiceImpl {
    async fn create_post(
        &self,
        request: Request<CreatePostRequest>,
    ) -> Result<Response<CreatePostResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let visibility = match req.visibility() {
            Visibility::Followers => "followers",
            Visibility::Private => "private",
            _ => "followers",
        };
        let parsed_media_ids: Vec<MediaId> = req
            .media_ids
            .iter()
            .map(|media_id| {
                MediaId::parse(media_id)
                    .map_err(|_| Status::invalid_argument("invalid media_id"))
            })
            .collect::<Result<_, _>>()?;
        if parsed_media_ids.is_empty() {
            return Err(Status::invalid_argument(
                "at least one media_id is required",
            ));
        }

        let post_id = PostId::new();
        let created_at = chrono::Utc::now();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let (loc_lat, loc_lng, loc_name) = if let Some(ref loc) = req.location {
            if loc.latitude != 0.0 || loc.longitude != 0.0 {
                (Some(loc.latitude), Some(loc.longitude), Some(loc.display_name.clone()))
            } else {
                (None, None, None)
            }
        } else {
            (None, None, None)
        };

        let post_type = if parsed_media_ids.len() > 1 {
            "carousel"
        } else {
            let media_type = sqlx::query_scalar::<_, String>(
                r#"
                SELECT media_type
                FROM media_items
                WHERE id = $1 AND owner_id = $2 AND post_id IS NULL
                "#,
            )
            .bind(parsed_media_ids[0].as_uuid())
            .bind(auth.user_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            match media_type.as_deref() {
                Some("video") => "video",
                Some(_) => "photo",
                None => {
                    return Err(Status::invalid_argument(format!(
                        "media {} not found or already attached",
                        req.media_ids[0]
                    )));
                }
            }
        };

        sqlx::query(
            r#"
            INSERT INTO posts (id, author_id, caption, visibility, post_type, created_at, location_lat, location_lng, location_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(post_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .bind(req.caption.as_str())
        .bind(visibility)
        .bind(post_type)
        .bind(created_at)
        .bind(loc_lat)
        .bind(loc_lng)
        .bind(loc_name.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        for (idx, (media_id, media_id_str)) in parsed_media_ids
            .iter()
            .zip(req.media_ids.iter())
            .enumerate()
        {
            let result = sqlx::query(
                r#"
                UPDATE media_items
                SET post_id = $1, order_index = $2
                WHERE id = $3 AND owner_id = $4 AND post_id IS NULL
                "#,
            )
            .bind(post_id.as_uuid())
            .bind(idx as i32)
            .bind(media_id.as_uuid())
            .bind(auth.user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            if result.rows_affected() == 0 {
                return Err(Status::invalid_argument(format!(
                    "media {} not found or already attached",
                    media_id_str
                )));
            }
        }

        if let Some((portal_id, _)) = Self::resolve_or_create_portal(
            &mut tx,
            auth.user_id,
            &req.portal_id,
            &req.portal_name,
        )
        .await?
        {
            sqlx::query(
                r#"
                INSERT INTO portal_posts (portal_id, post_id, added_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (portal_id, post_id) DO NOTHING
                "#,
            )
            .bind(portal_id)
            .bind(post_id.as_uuid())
            .bind(created_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            sqlx::query("UPDATE portals SET updated_at = NOW() WHERE id = $1")
                .bind(portal_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        // Insert the author's own feed entry and bump post_count synchronously
        // so the author sees their post immediately without waiting for the worker.
        sqlx::query(
            r#"
            INSERT INTO feed_entries (id, user_id, post_id, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, post_id) DO NOTHING
            "#,
        )
        .bind(FeedEntryId::new().as_uuid())
        .bind(auth.user_id.as_uuid())
        .bind(post_id.as_uuid())
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        sqlx::query("UPDATE profiles SET post_count = post_count + 1 WHERE user_id = $1")
            .bind(auth.user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let job_payload = serde_json::json!({
            "job_type": "feed_fanout",
            "post_id": post_id.to_string(),
            "author_id": auth.user_id.to_string()
        });

        sqlx::query(
            r#"
            INSERT INTO jobs (id, job_type, payload, state)
            VALUES ($1, 'feed_fanout', $2, 'pending')
            "#,
        )
        .bind(uuid::Uuid::now_v7())
        .bind(job_payload)
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CreatePostResponse {
            post_id: post_id.to_string(),
            created_at: Some(Timestamp {
                seconds: created_at.timestamp(),
                nanos: created_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn get_post(
        &self,
        request: Request<GetPostRequest>,
    ) -> Result<Response<GetPostResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let post_row = sqlx::query(
            r#"
            SELECT id, author_id, caption, visibility, like_count, comment_count, share_count,
                   created_at, edited_at, is_deleted, location_lat, location_lng, location_name
            FROM posts
            WHERE id = $1
            "#,
        )
        .bind(post_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let post_row = post_row.ok_or_else(|| Status::not_found("post not found"))?;

        let author_id: uuid::Uuid = post_row.get(1);
        let author_id = UserId::from_uuid(author_id);
        let visibility: String = post_row.get(3);
        let like_count: i32 = post_row.get(4);
        let comment_count: i32 = post_row.get(5);
        let share_count: i32 = post_row.get(6);
        let created_at: chrono::DateTime<chrono::Utc> = post_row.get(7);
        let edited_at: Option<chrono::DateTime<chrono::Utc>> = post_row.get(8);
        let is_deleted: bool = post_row.get(9);
        let location_lat: Option<f64> = post_row.get(10);
        let location_lng: Option<f64> = post_row.get(11);
        let location_name: Option<String> = post_row.get(12);

        if is_deleted {
            return Err(Status::not_found("post not found"));
        }

        let can_view = if auth.user_id == author_id {
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
        };

        if !can_view {
            return Err(Status::permission_denied("not authorized to view this post"));
        }

        let liked_by_viewer = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM likes
                WHERE user_id = $1 AND post_id = $2
            )
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .bind(post_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let caption: String = post_row.get(2);

        let post_detail = PostServiceImpl::build_post_detail(
            &self.pool,
            post_id,
            author_id,
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

        Ok(Response::new(GetPostResponse {
            post: Some(post_detail),
        }))
    }

    async fn add_post_to_portal(
        &self,
        request: Request<AddPostToPortalRequest>,
    ) -> Result<Response<AddPostToPortalResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;
        let added_at = chrono::Utc::now();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Self::ensure_post_owned_by_user(&mut tx, post_id, auth.user_id).await?;

        let (portal_id, portal_name) = Self::resolve_or_create_portal(
            &mut tx,
            auth.user_id,
            &req.portal_id,
            &req.portal_name,
        )
        .await?
        .ok_or_else(|| Status::invalid_argument("portal_id or portal_name is required"))?;

        sqlx::query(
            r#"
            INSERT INTO portal_posts (portal_id, post_id, added_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (portal_id, post_id) DO NOTHING
            "#,
        )
        .bind(portal_id)
        .bind(post_id.as_uuid())
        .bind(added_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        sqlx::query("UPDATE portals SET updated_at = NOW() WHERE id = $1")
            .bind(portal_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(AddPostToPortalResponse {
            portal_id: portal_id.to_string(),
            portal_name,
        }))
    }

    async fn remove_post_from_portal(
        &self,
        request: Request<RemovePostFromPortalRequest>,
    ) -> Result<Response<RemovePostFromPortalResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;
        let portal_id = uuid::Uuid::parse_str(req.portal_id.trim())
            .map_err(|_| Status::invalid_argument("invalid portal_id"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Self::ensure_post_owned_by_user(&mut tx, post_id, auth.user_id).await?;

        let owns_portal = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM portals
                WHERE id = $1 AND owner_id = $2
            )
            "#,
        )
        .bind(portal_id)
        .bind(auth.user_id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        if !owns_portal {
            return Err(Status::not_found("portal not found"));
        }

        sqlx::query(
            r#"
            DELETE FROM portal_posts
            WHERE portal_id = $1 AND post_id = $2
            "#,
        )
        .bind(portal_id)
        .bind(post_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        sqlx::query("UPDATE portals SET updated_at = NOW() WHERE id = $1")
            .bind(portal_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RemovePostFromPortalResponse {}))
    }

    async fn delete_post(
        &self,
        request: Request<DeletePostRequest>,
    ) -> Result<Response<DeletePostResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        let row = sqlx::query(
            r#"
            SELECT author_id FROM posts WHERE id = $1 AND NOT is_deleted
            "#,
        )
        .bind(post_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let row = row.ok_or_else(|| Status::not_found("post not found"))?;
        let author_id: uuid::Uuid = row.get(0);
        let author_id = UserId::from_uuid(author_id);

        if auth.user_id != author_id && !auth.is_admin {
            return Err(Status::permission_denied("only author or admin can delete"));
        }

        sqlx::query(
            r#"
            UPDATE posts SET is_deleted = TRUE WHERE id = $1
            "#,
        )
        .bind(post_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DeletePostResponse {}))
    }

    async fn edit_caption(
        &self,
        request: Request<EditCaptionRequest>,
    ) -> Result<Response<EditCaptionResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let post_id = PostId::parse(&req.post_id)
            .map_err(|_| Status::invalid_argument("invalid post_id"))?;

        if req.clear_location && req.location.is_some() {
            return Err(Status::invalid_argument(
                "clear_location cannot be combined with location",
            ));
        }

        let location_update = if let Some(location) = req.location.as_ref() {
            if !location.latitude.is_finite()
                || !location.longitude.is_finite()
                || !(-90.0..=90.0).contains(&location.latitude)
                || !(-180.0..=180.0).contains(&location.longitude)
            {
                return Err(Status::invalid_argument("invalid location coordinates"));
            }

            Some((
                location.latitude,
                location.longitude,
                location.display_name.trim().to_string(),
            ))
        } else {
            None
        };
        let replace_location = location_update.is_some();
        let (location_lat, location_lng, location_name) = location_update
            .map(|(lat, lng, name)| (Some(lat), Some(lng), Some(name)))
            .unwrap_or((None, None, None));

        let edited_at = chrono::Utc::now();

        let result = sqlx::query(
            r#"
            UPDATE posts
            SET caption = $1,
                location_lat = CASE
                    WHEN $2 THEN NULL
                    WHEN $3 THEN $4
                    ELSE location_lat
                END,
                location_lng = CASE
                    WHEN $2 THEN NULL
                    WHEN $3 THEN $5
                    ELSE location_lng
                END,
                location_name = CASE
                    WHEN $2 THEN NULL
                    WHEN $3 THEN $6
                    ELSE location_name
                END,
                edited_at = $7
            WHERE id = $8 AND author_id = $9 AND NOT is_deleted
            "#,
        )
        .bind(&req.caption)
        .bind(req.clear_location)
        .bind(replace_location)
        .bind(location_lat)
        .bind(location_lng)
        .bind(location_name.as_deref())
        .bind(edited_at)
        .bind(post_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("post not found or not authorized"));
        }

        Ok(Response::new(EditCaptionResponse {
            edited_at: Some(Timestamp {
                seconds: edited_at.timestamp(),
                nanos: edited_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }
}
