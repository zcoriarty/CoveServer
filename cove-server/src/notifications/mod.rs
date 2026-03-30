//! Notification service gRPC implementation.

use crate::auth;
use crate::authorization::build_user_summary;
use crate::push::PushService;
use cove_common::error::{CoveError, CoveResult};
use cove_common::id::{NotificationId, UserId};
use cove_common::pagination::{CursorValue, PaginationParams};
use cove_proto::cove::notification::{
    notification_service_server::NotificationService, GetUnreadCountRequest,
    GetUnreadCountResponse, ListNotificationsRequest, ListNotificationsResponse, MarkReadRequest,
    MarkReadResponse, NotificationItem, NotificationType,
};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Notification service implementation.
pub struct NotificationServiceImpl {
    pub pool: PgPool,
    pub jwt_secret: String,
    push: Arc<PushService>,
}

impl NotificationServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String, push: Arc<PushService>) -> Self {
        Self {
            pool,
            jwt_secret,
            push,
        }
    }

    fn auth(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<cove_common::auth_context::AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }

    fn db_type_to_proto(db_type: &str) -> i32 {
        match db_type {
            "follow_request" => NotificationType::FollowRequest as i32,
            "follow_accepted" => NotificationType::FollowAccepted as i32,
            "new_follower" => NotificationType::NewFollower as i32,
            "like" => NotificationType::Like as i32,
            "mention" => NotificationType::Comment as i32,
            "comment" => NotificationType::Comment as i32,
            "share" => NotificationType::Share as i32,
            "new_post" => NotificationType::NewPost as i32,
            _ => NotificationType::Unspecified as i32,
        }
    }
}

/// Public helper for other modules to create notifications.
/// Inserts a notification record directly (synchronous). For async fanout,
/// enqueue a job instead.
pub async fn create_notification(
    pool: &PgPool,
    recipient_id: UserId,
    actor_id: UserId,
    notification_type: &str,
    target_id: Option<Uuid>,
    message: &str,
) -> CoveResult<()> {
    let id = NotificationId::new();
    sqlx::query(
        r#"
        INSERT INTO notifications (id, recipient_id, actor_id, notification_type, target_type, target_id, message, is_read, created_at)
        VALUES ($1, $2, $3, $4, 'post', $5, $6, FALSE, NOW())
        "#,
    )
    .bind(id.as_uuid())
    .bind(recipient_id.as_uuid())
    .bind(actor_id.as_uuid())
    .bind(notification_type)
    .bind(target_id)
    .bind(message)
    .execute(pool)
    .await
    .map_err(|e| CoveError::Database(e.to_string()))?;

    Ok(())
}

#[tonic::async_trait]
impl NotificationService for NotificationServiceImpl {
    async fn list_notifications(
        &self,
        request: Request<ListNotificationsRequest>,
    ) -> Result<Response<ListNotificationsResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        if let Err(error) = self
            .push
            .sync_token_from_metadata(&auth, request.metadata())
            .await
        {
            tracing::warn!(error = %error, "failed syncing push token metadata");
        }
        let req = request.into_inner();

        let pagination = PaginationParams::from_proto(
            req.pagination.as_ref().map(|p| p.page_size).unwrap_or(20),
            req.pagination
                .as_ref()
                .map(|p| p.cursor.as_str())
                .unwrap_or(""),
        );

        let limit = (pagination.limit + 1) as i64;

        type NotifRow = (
            uuid::Uuid,
            String,
            Option<uuid::Uuid>,
            Option<uuid::Uuid>,
            String,
            bool,
            chrono::DateTime<chrono::Utc>,
        );

        let rows: Vec<NotifRow> = if let Some(ref cursor) = pagination.cursor {
            sqlx::query_as(
                r#"
                SELECT n.id, n.notification_type, n.actor_id, n.target_id, n.message, n.is_read, n.created_at
                FROM notifications n
                WHERE n.recipient_id = $1
                  AND (n.created_at, n.id) < ($2, $3)
                ORDER BY n.created_at DESC, n.id DESC
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
                SELECT n.id, n.notification_type, n.actor_id, n.target_id, n.message, n.is_read, n.created_at
                FROM notifications n
                WHERE n.recipient_id = $1
                ORDER BY n.created_at DESC, n.id DESC
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

        let actor_ids: Vec<uuid::Uuid> =
            truncated
                .iter()
                .filter_map(|r| r.2)
                .fold(vec![], |mut acc, id| {
                    if !acc.contains(&id) {
                        acc.push(id);
                    }
                    acc
                });

        let mut actor_map: std::collections::HashMap<
            uuid::Uuid,
            (String, String, Option<uuid::Uuid>),
        > = std::collections::HashMap::new();

        for aid in &actor_ids {
            if let Ok(row) = sqlx::query_as::<_, (String, String, Option<uuid::Uuid>)>(
                r#"
                SELECT u.username, COALESCE(u.display_name, ''), p.avatar_media_id
                FROM users u
                LEFT JOIN profiles p ON p.user_id = u.id
                WHERE u.id = $1
                "#,
            )
            .bind(aid)
            .fetch_one(&self.pool)
            .await
            {
                actor_map.insert(*aid, row);
            }
        }

        let notifications: Vec<NotificationItem> = truncated
            .iter()
            .map(
                |(id, notif_type, actor_id, target_id, message, is_read, created_at)| {
                    let actor = actor_id.map(|aid| {
                        actor_map
                            .get(&aid)
                            .map(|(username, display_name, avatar_media_id)| {
                                let avatar_url = avatar_media_id
                                    .map(|media_id| format!("/media/{}", media_id))
                                    .unwrap_or_default();
                                build_user_summary(
                                    &UserId::from_uuid(aid),
                                    username.clone(),
                                    display_name.clone(),
                                    avatar_url,
                                    false,
                                )
                            })
                            .unwrap_or_else(|| {
                                build_user_summary(
                                    &UserId::from_uuid(aid),
                                    "[deleted]".into(),
                                    String::new(),
                                    String::new(),
                                    false,
                                )
                            })
                    });

                    let target_id_str = target_id.map(|u| u.to_string()).unwrap_or_default();

                    NotificationItem {
                        notification_id: id.to_string(),
                        notification_type: Self::db_type_to_proto(notif_type),
                        actor,
                        target_id: target_id_str,
                        message: message.clone(),
                        is_read: *is_read,
                        created_at: Some(prost_types::Timestamp {
                            seconds: created_at.timestamp(),
                            nanos: created_at.timestamp_subsec_nanos() as i32,
                        }),
                    }
                },
            )
            .collect();

        let next_cursor = if has_more && rows.len() > pagination.limit as usize {
            rows.get(pagination.limit as usize - 1)
                .map(|r| {
                    PaginationParams::encode_cursor(&CursorValue {
                        timestamp: r.6,
                        id: r.0,
                    })
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(ListNotificationsResponse {
            notifications,
            pagination: Some(cove_proto::cove::common::PaginationResponse {
                next_cursor,
                has_more,
                total_count: -1,
            }),
        }))
    }

    async fn mark_read(
        &self,
        request: Request<MarkReadRequest>,
    ) -> Result<Response<MarkReadResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        if let Err(error) = self
            .push
            .sync_token_from_metadata(&auth, request.metadata())
            .await
        {
            tracing::warn!(error = %error, "failed syncing push token metadata");
        }
        let req = request.into_inner();

        if req.notification_ids.is_empty() {
            return Ok(Response::new(MarkReadResponse {}));
        }

        let ids: Vec<uuid::Uuid> = req
            .notification_ids
            .iter()
            .filter_map(|s| uuid::Uuid::parse_str(s).ok())
            .collect();

        if ids.is_empty() {
            return Ok(Response::new(MarkReadResponse {}));
        }

        sqlx::query(
            r#"
            UPDATE notifications
            SET is_read = TRUE
            WHERE id = ANY($1) AND recipient_id = $2
            "#,
        )
        .bind(&ids)
        .bind(auth.user_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        Ok(Response::new(MarkReadResponse {}))
    }

    async fn get_unread_count(
        &self,
        request: Request<GetUnreadCountRequest>,
    ) -> Result<Response<GetUnreadCountResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        if let Err(error) = self
            .push
            .sync_token_from_metadata(&auth, request.metadata())
            .await
        {
            tracing::warn!(error = %error, "failed syncing push token metadata");
        }

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND NOT is_read",
        )
        .bind(auth.user_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| Status::internal("database error"))?;

        Ok(Response::new(GetUnreadCountResponse {
            count: count as i32,
        }))
    }
}
