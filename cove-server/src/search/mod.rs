//! Search service gRPC implementation.

use crate::auth;
use crate::authorization::build_user_summary;
use cove_common::id::UserId;
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::common::{MediaReference, MediaType};
use cove_proto::cove::search::{
    search_service_server::SearchService, SearchPostResult, SearchPostsRequest, SearchPostsResponse,
    SearchUsersRequest, SearchUsersResponse,
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};

/// Search service implementation.
pub struct SearchServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
}

impl SearchServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String) -> Self {
        Self { pool, jwt_secret }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<cove_common::auth_context::AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }
}

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    async fn search_users(
        &self,
        request: Request<SearchUsersRequest>,
    ) -> Result<Response<SearchUsersResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let query = req.query.trim();
        if query.is_empty() {
            return Ok(Response::new(SearchUsersResponse {
                users: vec![],
                pagination: Some(cove_proto::cove::common::PaginationResponse {
                    next_cursor: String::new(),
                    has_more: false,
                    total_count: 0,
                }),
            }));
        }

        let pagination = PaginationParams::from_proto(
            req.pagination.as_ref().map(|p| p.page_size).unwrap_or(20),
            req.pagination.as_ref().map(|p| p.cursor.as_str()).unwrap_or(""),
        );
        let limit = (pagination.limit + 1) as i64;
        let pattern = format!("%{}%", query);

        type UserRow = (
            uuid::Uuid,
            String,
            String,
            Option<uuid::Uuid>,
            chrono::DateTime<chrono::Utc>,
        );
        let rows: Vec<UserRow> = if let Some(ref cursor) = pagination.cursor {
            sqlx::query_as(
                r#"
                SELECT u.id, u.username, COALESCE(u.display_name, ''), p.avatar_media_id, u.created_at
                FROM users u
                LEFT JOIN profiles p ON p.user_id = u.id
                WHERE u.account_state != 'suspended'
                  AND (u.username ILIKE $1 OR u.display_name ILIKE $1)
                  AND (u.created_at, u.id) < ($4, $5)
                ORDER BY
                  COALESCE(similarity(u.username, $2), 0) + COALESCE(similarity(u.display_name, $2), 0) DESC,
                  u.created_at DESC, u.id DESC
                LIMIT $3
                "#,
            )
            .bind(&pattern)
            .bind(query)
            .bind(limit)
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                if e.to_string().contains("function similarity") {
                    Status::invalid_argument("search requires pg_trgm extension")
                } else {
                    Status::internal("database error")
                }
            })?
        } else {
            sqlx::query_as(
                r#"
                SELECT u.id, u.username, COALESCE(u.display_name, ''), p.avatar_media_id, u.created_at
                FROM users u
                LEFT JOIN profiles p ON p.user_id = u.id
                WHERE u.account_state != 'suspended'
                  AND (u.username ILIKE $1 OR u.display_name ILIKE $1)
                ORDER BY
                  COALESCE(similarity(u.username, $2), 0) + COALESCE(similarity(u.display_name, $2), 0) DESC,
                  u.created_at DESC, u.id DESC
                LIMIT $3
                "#,
            )
            .bind(&pattern)
            .bind(query)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                if e.to_string().contains("function similarity") {
                    Status::invalid_argument("search requires pg_trgm extension")
                } else {
                    Status::internal("database error")
                }
            })?
        };

        let has_more = rows.len() as i64 > pagination.limit as i64;
        let truncated: Vec<_> = rows.iter().take(pagination.limit as usize).collect();

        let user_ids: Vec<uuid::Uuid> = truncated.iter().map(|r| r.0).collect();

        let following_ids: Vec<uuid::Uuid> = if user_ids.is_empty() {
            vec![]
        } else {
            sqlx::query_scalar(
                "SELECT followee_id FROM follows WHERE follower_id = $1 AND followee_id = ANY($2) AND state = 'accepted'",
            )
            .bind(auth.user_id.as_uuid())
            .bind(&user_ids)
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default()
        };
        let following_set: std::collections::HashSet<uuid::Uuid> =
            following_ids.into_iter().collect();

        let users: Vec<cove_proto::cove::common::UserSummary> = truncated
            .iter()
            .map(|(id, username, display_name, avatar_media_id, _)| {
                let avatar_url = avatar_media_id
                    .map(|media_id| format!("/media/{}", media_id))
                    .unwrap_or_default();
                build_user_summary(
                    &UserId::from_uuid(*id),
                    username.clone(),
                    display_name.clone(),
                    avatar_url,
                    following_set.contains(id),
                )
            })
            .collect();

        let next_cursor = if has_more && rows.len() > pagination.limit as usize {
            rows.get(pagination.limit as usize - 1)
                .map(|r| {
                    PaginationParams::encode_cursor(&CursorValue {
                        timestamp: r.4,
                        id: r.0,
                    })
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(SearchUsersResponse {
            users,
            pagination: Some(cove_proto::cove::common::PaginationResponse {
                next_cursor,
                has_more,
                total_count: -1,
            }),
        }))
    }

    async fn search_posts(
        &self,
        request: Request<SearchPostsRequest>,
    ) -> Result<Response<SearchPostsResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let query = req.query.trim();
        if query.is_empty() {
            return Ok(Response::new(SearchPostsResponse {
                results: vec![],
                pagination: Some(cove_proto::cove::common::PaginationResponse {
                    next_cursor: String::new(),
                    has_more: false,
                    total_count: 0,
                }),
            }));
        }

        let pagination = PaginationParams::from_proto(
            req.pagination.as_ref().map(|p| p.page_size).unwrap_or(20),
            req.pagination.as_ref().map(|p| p.cursor.as_str()).unwrap_or(""),
        );
        let limit = (pagination.limit + 1) as i64;
        let pattern = format!("%{}%", query);

        type PostRow = (
            uuid::Uuid,
            uuid::Uuid,
            String,
            String,
            Option<String>,
            Option<uuid::Uuid>,
            Option<uuid::Uuid>,
            Option<String>,
            Option<i32>,
            Option<i32>,
            Option<f64>,
            Option<i32>,
            chrono::DateTime<chrono::Utc>,
        );

        let rows: Vec<PostRow> = if let Some(ref cursor) = pagination.cursor {
            sqlx::query_as(
                r#"
                SELECT p.id, p.author_id, p.caption,
                       u.username, u.display_name, pr.avatar_media_id,
                       m.id as media_id, m.media_type,
                       m.width, m.height, m.aspect_ratio, m.duration_seconds,
                       p.created_at
                FROM posts p
                JOIN users u ON u.id = p.author_id
                LEFT JOIN profiles pr ON pr.user_id = u.id
                LEFT JOIN LATERAL (
                    SELECT id, media_type, width, height, aspect_ratio, duration_seconds
                    FROM media_items
                    WHERE post_id = p.id AND processing_state = 'completed'
                    ORDER BY created_at
                    LIMIT 1
                ) m ON true
                WHERE NOT p.is_deleted
                  AND p.caption ILIKE $1
                  AND (p.created_at, p.id) < ($4, $5)
                  AND (
                    p.author_id = $2
                    OR (
                      p.visibility = 'followers'
                      AND EXISTS (
                        SELECT 1 FROM follows f
                        WHERE f.followee_id = p.author_id
                          AND f.follower_id = $2
                          AND f.state = 'accepted'
                      )
                    )
                  )
                ORDER BY p.created_at DESC, p.id DESC
                LIMIT $3
                "#,
            )
            .bind(&pattern)
            .bind(auth.user_id.as_uuid())
            .bind(limit)
            .bind(cursor.timestamp)
            .bind(cursor.id)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?
        } else {
            sqlx::query_as(
                r#"
                SELECT p.id, p.author_id, p.caption,
                       u.username, u.display_name, pr.avatar_media_id,
                       m.id as media_id, m.media_type,
                       m.width, m.height, m.aspect_ratio, m.duration_seconds,
                       p.created_at
                FROM posts p
                JOIN users u ON u.id = p.author_id
                LEFT JOIN profiles pr ON pr.user_id = u.id
                LEFT JOIN LATERAL (
                    SELECT id, media_type, width, height, aspect_ratio, duration_seconds
                    FROM media_items
                    WHERE post_id = p.id AND processing_state = 'completed'
                    ORDER BY created_at
                    LIMIT 1
                ) m ON true
                WHERE NOT p.is_deleted
                  AND p.caption ILIKE $1
                  AND (
                    p.author_id = $2
                    OR (
                      p.visibility = 'followers'
                      AND EXISTS (
                        SELECT 1 FROM follows f
                        WHERE f.followee_id = p.author_id
                          AND f.follower_id = $2
                          AND f.state = 'accepted'
                      )
                    )
                  )
                ORDER BY p.created_at DESC, p.id DESC
                LIMIT $3
                "#,
            )
            .bind(&pattern)
            .bind(auth.user_id.as_uuid())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| Status::internal("database error"))?
        };

        let has_more = rows.len() as i64 > pagination.limit as i64;
        let truncated: Vec<_> = rows.iter().take(pagination.limit as usize).collect();

        let snippet_len = 120;
        let results: Vec<SearchPostResult> = truncated
            .iter()
            .cloned()
            .map(|(post_id, author_id, caption, username, display_name, avatar_media_id, media_id, media_type, width, height, aspect_ratio, duration_seconds, _)| {
                let caption_snippet = if caption.len() > snippet_len {
                    format!("{}...", &caption[..snippet_len])
                } else {
                    caption.clone()
                };
                let avatar_url = avatar_media_id
                    .map(|media_id| format!("/media/{}", media_id))
                    .unwrap_or_default();

                let author = build_user_summary(
                    &cove_common::id::UserId::from_uuid(*author_id),
                    username.clone(),
                    display_name.as_deref().unwrap_or_default().to_string(),
                    avatar_url,
                    false,
                );

                let media_type_enum = media_type
                    .as_deref()
                    .map(|t| match t {
                        "video" => MediaType::Video as i32,
                        "audio" => MediaType::Audio as i32,
                        _ => MediaType::Photo as i32,
                    })
                    .unwrap_or(MediaType::Unspecified as i32);

                let thumbnail = media_id.map(|id| MediaReference {
                    media_id: id.to_string(),
                    media_type: media_type_enum,
                    url: String::new(),
                    width: width.unwrap_or(0),
                    height: height.unwrap_or(0),
                    aspect_ratio: aspect_ratio.unwrap_or(1.0),
                    duration_seconds: duration_seconds.unwrap_or(0),
                    thumbnail_url: String::new(),
                });

                SearchPostResult {
                    post_id: post_id.to_string(),
                    author: Some(author),
                    caption_snippet,
                    thumbnail,
                }
            })
            .collect();

        let next_cursor = if has_more && rows.len() > pagination.limit as usize {
            rows.get(pagination.limit as usize - 1)
                .map(|r| {
                    PaginationParams::encode_cursor(&CursorValue {
                        timestamp: r.12,
                        id: r.0,
                    })
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(SearchPostsResponse {
            results,
            pagination: Some(cove_proto::cove::common::PaginationResponse {
                next_cursor,
                has_more,
                total_count: -1,
            }),
        }))
    }
}
