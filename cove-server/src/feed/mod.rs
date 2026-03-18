//! Feed service and gRPC handler.

use crate::auth;
use cove_common::error::{CoveError, CoveResult};
use cove_common::id::{FeedEntryId, PostId, UserId};
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::common::{Location, MediaReference, MediaType, UserSummary, Visibility};
use cove_proto::cove::feed::{
    feed_service_server::FeedService, FeedItem, GetHomeFeedRequest, GetHomeFeedResponse,
};
use cove_proto::cove::post::PostDetail;
use prost_types::Timestamp;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use tonic::{Request, Response, Status};

/// Feed service implementation.
pub struct FeedServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
}

impl FeedServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String) -> Self {
        Self { pool, jwt_secret }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<cove_common::auth_context::AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }

    /// Builds complete FeedItem protos from DB data.
    pub async fn hydrate_feed_items(
        pool: &PgPool,
        rows: &[FeedEntryRow],
        viewer_id: UserId,
    ) -> CoveResult<Vec<FeedItem>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let post_ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.post_id).collect();
        let unique_author_ids: Vec<uuid::Uuid> = rows
            .iter()
            .map(|r| r.author_id)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let author_map: HashMap<uuid::Uuid, UserSummary> = if unique_author_ids.is_empty() {
            HashMap::new()
        } else {
            let placeholders: Vec<String> = unique_author_ids
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 1))
                .collect();
            let query = format!(
                r#"
                SELECT u.id, u.username, u.display_name, p.avatar_media_id
                FROM users u
                LEFT JOIN profiles p ON p.user_id = u.id
                WHERE u.id IN ({})
                "#,
                placeholders.join(", ")
            );
            let mut q = sqlx::query(&query);
            for aid in &unique_author_ids {
                q = q.bind(aid);
            }
            let author_rows = q
                .fetch_all(pool)
                .await
                .map_err(|e| CoveError::Database(e.to_string()))?;

            let mut m = HashMap::new();
            for r in author_rows {
                let id: uuid::Uuid = r.get(0);
                let username: String = r.get(1);
                let display_name: String = r.get(2);
                let avatar_media_id: Option<uuid::Uuid> = r.get(3);
                let avatar_url = avatar_media_id
                    .map(|media_id| format!("/media/{}", media_id))
                    .unwrap_or_default();
                m.insert(id, UserSummary {
                    user_id: id.to_string(),
                    username,
                    display_name,
                    avatar_url,
                    is_following: false,
                });
            }
            m
        };

        let media_rows = sqlx::query(
            r#"
            SELECT post_id, id, media_type, width, height, aspect_ratio, duration_seconds
            FROM media_items
            WHERE post_id = ANY($1) AND processing_state = 'completed'
            ORDER BY post_id, created_at ASC
            "#,
        )
        .bind(&post_ids)
        .fetch_all(pool)
        .await
        .map_err(|e| CoveError::Database(e.to_string()))?;

        let mut media_by_post: HashMap<uuid::Uuid, Vec<MediaReference>> = HashMap::new();
        for r in media_rows {
            let post_id: uuid::Uuid = r.get(0);
            let id: uuid::Uuid = r.get(1);
            let mt: String = r.get(2);
            let w: i32 = r.get(3);
            let h: i32 = r.get(4);
            let ar: f64 = r.get(5);
            let dur: i32 = r.get(6);
            let media_ref = MediaReference {
                media_id: id.to_string(),
                media_type: match mt.as_str() {
                    "photo" => MediaType::Photo as i32,
                    "video" => MediaType::Video as i32,
                    _ => MediaType::Unspecified as i32,
                },
                url: String::new(),
                width: w,
                height: h,
                aspect_ratio: ar,
                duration_seconds: dur,
                thumbnail_url: String::new(),
            };
            media_by_post.entry(post_id).or_default().push(media_ref);
        }

        let liked_post_ids: Vec<uuid::Uuid> = sqlx::query_scalar(
            r#"
            SELECT post_id FROM likes WHERE user_id = $1 AND post_id = ANY($2)
            "#,
        )
        .bind(viewer_id.as_uuid())
        .bind(&post_ids)
        .fetch_all(pool)
        .await
        .map_err(|e| CoveError::Database(e.to_string()))?;

        let liked_set: std::collections::HashSet<uuid::Uuid> =
            liked_post_ids.into_iter().collect();

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let author = author_map
                .get(&row.author_id)
                .cloned()
                .unwrap_or_else(|| UserSummary {
                    user_id: row.author_id.to_string(),
                    username: String::new(),
                    display_name: String::new(),
                    avatar_url: String::new(),
                    is_following: false,
                });

            let media_refs = media_by_post
                .get(&row.post_id)
                .cloned()
                .unwrap_or_default();

            let visibility_proto = match row.visibility.as_str() {
                "followers" => Visibility::Followers as i32,
                "private" => Visibility::Private as i32,
                _ => Visibility::Unspecified as i32,
            };

            let location = match (row.location_lat, row.location_lng) {
                (Some(lat), Some(lng)) => Some(Location {
                    latitude: lat,
                    longitude: lng,
                    display_name: row.location_name.clone().unwrap_or_default(),
                }),
                _ => None,
            };

            let post_detail = PostDetail {
                post_id: row.post_id.to_string(),
                author: Some(author),
                caption: row.caption.clone(),
                media: media_refs,
                like_count: row.like_count,
                comment_count: row.comment_count,
                share_count: row.share_count,
                liked_by_viewer: liked_set.contains(&row.post_id),
                visibility: visibility_proto,
                created_at: Some(Timestamp {
                    seconds: row.post_created_at.timestamp(),
                    nanos: row.post_created_at.timestamp_subsec_nanos() as i32,
                }),
                edited_at: row.edited_at.map(|t| Timestamp {
                    seconds: t.timestamp(),
                    nanos: t.timestamp_subsec_nanos() as i32,
                }),
                location,
            };

            items.push(FeedItem {
                post: Some(post_detail),
            });
        }

        Ok(items)
    }
}

struct FeedEntryRow {
    feed_entry_id: uuid::Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
    post_id: uuid::Uuid,
    author_id: uuid::Uuid,
    caption: String,
    visibility: String,
    like_count: i32,
    comment_count: i32,
    share_count: i32,
    post_created_at: chrono::DateTime<chrono::Utc>,
    edited_at: Option<chrono::DateTime<chrono::Utc>>,
    location_lat: Option<f64>,
    location_lng: Option<f64>,
    location_name: Option<String>,
}

/// Feed fanout: inserts feed_entries for all accepted followers of the author.
/// Called by the worker when processing a feed_fanout job.
pub async fn fanout_post(pool: &PgPool, post_id: PostId, author_id: UserId) -> CoveResult<()> {
    let followers = sqlx::query(
        r#"
        SELECT follower_id FROM follows
        WHERE followee_id = $1 AND state = 'accepted'
        "#,
    )
    .bind(author_id.as_uuid())
    .fetch_all(pool)
    .await
    .map_err(|e| CoveError::Database(e.to_string()))?;

    if followers.is_empty() {
        return Ok(());
    }

    let created_at = chrono::Utc::now();

    for row in followers {
        let follower_id: uuid::Uuid = row.get(0);
        let feed_entry_id = FeedEntryId::new();

        sqlx::query(
            r#"
            INSERT INTO feed_entries (id, user_id, post_id, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, post_id) DO NOTHING
            "#,
        )
        .bind(feed_entry_id.as_uuid())
        .bind(follower_id)
        .bind(post_id.as_uuid())
        .bind(created_at)
        .execute(pool)
        .await
        .map_err(|e| CoveError::Database(e.to_string()))?;
    }

    Ok(())
}

#[tonic::async_trait]
impl FeedService for FeedServiceImpl {
    async fn get_home_feed(
        &self,
        request: Request<GetHomeFeedRequest>,
    ) -> Result<Response<GetHomeFeedResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        tracing::info!(user_id = %auth.user_id, "get_home_feed called");
        let req = request.into_inner();

        let pagination = req.pagination.as_ref();
        let page_size = pagination.map(|p| p.page_size).unwrap_or(20).clamp(1, 50);
        let cursor_str = pagination
            .map(|p| p.cursor.as_str())
            .unwrap_or("");

        let params = PaginationParams::from_proto(page_size, cursor_str);
        // Caching is disabled: always query Postgres for fresh data.

        let limit_plus_one = params.limit as i64 + 1;

        let rows: Vec<sqlx::postgres::PgRow> = if let Some(ref c) = params.cursor {
            sqlx::query(
                r#"
                SELECT fe.id, fe.created_at, p.id as post_id, p.author_id, p.caption, p.visibility,
                       p.like_count, p.comment_count, p.share_count, p.created_at as post_created_at,
                       p.edited_at, p.location_lat, p.location_lng, p.location_name
                FROM feed_entries fe
                JOIN posts p ON p.id = fe.post_id AND NOT p.is_deleted
                WHERE fe.user_id = $1 AND (fe.created_at, fe.id) < ($2, $3)
                ORDER BY fe.created_at DESC, fe.id DESC
                LIMIT $4
                "#,
            )
            .bind(auth.user_id.as_uuid())
            .bind(c.timestamp)
            .bind(c.id)
            .bind(limit_plus_one)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT fe.id, fe.created_at, p.id as post_id, p.author_id, p.caption, p.visibility,
                       p.like_count, p.comment_count, p.share_count, p.created_at as post_created_at,
                       p.edited_at, p.location_lat, p.location_lng, p.location_name
                FROM feed_entries fe
                JOIN posts p ON p.id = fe.post_id AND NOT p.is_deleted
                WHERE fe.user_id = $1
                ORDER BY fe.created_at DESC, fe.id DESC
                LIMIT $2
                "#,
            )
            .bind(auth.user_id.as_uuid())
            .bind(limit_plus_one)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| Status::internal(e.to_string()))?;

        let has_more = rows.len() as i64 > params.limit as i64;
        let rows = if has_more {
            &rows[..params.limit as usize]
        } else {
            &rows[..]
        };

        let feed_rows: Vec<FeedEntryRow> = rows
            .iter()
            .map(|r| FeedEntryRow {
                feed_entry_id: r.get(0),
                created_at: r.get(1),
                post_id: r.get(2),
                author_id: r.get(3),
                caption: r.get(4),
                visibility: r.get(5),
                like_count: r.get(6),
                comment_count: r.get(7),
                share_count: r.get(8),
                post_created_at: r.get(9),
                edited_at: r.get(10),
                location_lat: r.get(11),
                location_lng: r.get(12),
                location_name: r.get(13),
            })
            .collect();

        let items = FeedServiceImpl::hydrate_feed_items(&self.pool, &feed_rows, auth.user_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let next_cursor = if has_more {
            let last = feed_rows.last().unwrap();
            Some(PaginationParams::encode_cursor(&CursorValue {
                timestamp: last.created_at,
                id: last.feed_entry_id,
            }))
        } else {
            None
        };

        tracing::info!(user_id = %auth.user_id, item_count = items.len(), has_more = has_more, "get_home_feed response");

        let response = GetHomeFeedResponse {
            items,
            pagination: Some(cove_proto::cove::common::PaginationResponse {
                next_cursor: next_cursor.unwrap_or_default(),
                has_more: has_more,
                total_count: 0,
            }),
        };

        // Caching is currently disabled.

        Ok(Response::new(response))
    }
}
