//! Admin service for invite management, user moderation, and system health.

use crate::auth;
use cove_common::error::CoveError;
use cove_common::id::{InviteId, UserId};
use cove_proto::cove::admin::{
    admin_service_server::AdminService, AuditEntry, CreateInviteRequest, CreateInviteResponse,
    GetAuditLogRequest, GetAuditLogResponse, GetSystemHealthRequest, GetSystemHealthResponse,
    InviteInfo, ListInvitesRequest, ListInvitesResponse, RevokeInviteRequest, RevokeInviteResponse,
    SuspendUserRequest, SuspendUserResponse, UnsuspendUserRequest, UnsuspendUserResponse,
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};

pub struct AdminServiceImpl {
    pool: PgPool,
    jwt_secret: String,
    redis_conn: redis::aio::ConnectionManager,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
}

impl AdminServiceImpl {
    pub fn new(
        pool: PgPool,
        jwt_secret: String,
        redis_conn: redis::aio::ConnectionManager,
        s3_client: aws_sdk_s3::Client,
        bucket: String,
    ) -> Self {
        Self {
            pool,
            jwt_secret,
            redis_conn,
            s3_client,
            bucket,
        }
    }

    fn auth(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<cove_common::auth_context::AuthContext, Status> {
        let ctx = auth::extract_auth(metadata, &self.jwt_secret).map_err(Status::from)?;
        if !ctx.is_admin {
            return Err(Status::permission_denied("admin access required"));
        }
        Ok(ctx)
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn create_invite(
        &self,
        request: Request<CreateInviteRequest>,
    ) -> Result<Response<CreateInviteResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let invite_id = InviteId::new();
        let code = format!(
            "cove-{}",
            uuid::Uuid::now_v7()
                .to_string()
                .chars()
                .take(12)
                .collect::<String>()
        );
        let max_uses = if req.max_uses > 0 { req.max_uses } else { 1 };

        let expires_at = req.expires_at.map(|ts| {
            chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                .unwrap_or_else(chrono::Utc::now)
        });

        sqlx::query(
            r#"
            INSERT INTO invites (id, code, created_by, max_uses, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(invite_id.as_uuid())
        .bind(&code)
        .bind(auth.user_id.as_uuid())
        .bind(max_uses)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "create invite failed");
            Status::internal("internal error")
        })?;

        crate::audit::log_action(
            &self.pool,
            &auth.user_id,
            "create_invite",
            "invite",
            &invite_id.to_string(),
            &serde_json::json!({"max_uses": max_uses}),
        )
        .await
        .ok();

        Ok(Response::new(CreateInviteResponse {
            invite_code: code,
            expires_at: req.expires_at,
        }))
    }

    async fn list_invites(
        &self,
        request: Request<ListInvitesRequest>,
    ) -> Result<Response<ListInvitesResponse>, Status> {
        let _auth = self.auth(request.metadata())?;

        let rows = sqlx::query(
            r#"
            SELECT i.code, u.username, i.max_uses, i.use_count, i.created_at, i.expires_at, i.revoked
            FROM invites i
            JOIN users u ON u.id = i.created_by
            ORDER BY i.created_at DESC
            LIMIT 100
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "list invites failed");
            Status::internal("internal error")
        })?;

        let invites = rows
            .iter()
            .map(|r| {
                let code: String = r.get(0);
                let created_by: String = r.get(1);
                let max_uses: i32 = r.get(2);
                let use_count: i32 = r.get(3);
                let created_at: chrono::DateTime<chrono::Utc> = r.get(4);
                let expires_at: Option<chrono::DateTime<chrono::Utc>> = r.get(5);
                let revoked: bool = r.get(6);

                InviteInfo {
                    invite_code: code,
                    created_by,
                    max_uses,
                    use_count,
                    created_at: Some(prost_types::Timestamp {
                        seconds: created_at.timestamp(),
                        nanos: created_at.timestamp_subsec_nanos() as i32,
                    }),
                    expires_at: expires_at.map(|t| prost_types::Timestamp {
                        seconds: t.timestamp(),
                        nanos: t.timestamp_subsec_nanos() as i32,
                    }),
                    revoked,
                }
            })
            .collect();

        Ok(Response::new(ListInvitesResponse { invites }))
    }

    async fn revoke_invite(
        &self,
        request: Request<RevokeInviteRequest>,
    ) -> Result<Response<RevokeInviteResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let result = sqlx::query(
            r#"UPDATE invites SET revoked = TRUE WHERE code = $1 AND NOT revoked"#,
        )
        .bind(&req.invite_code)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "revoke invite failed");
            Status::internal("internal error")
        })?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("invite not found or already revoked"));
        }

        crate::audit::log_action(
            &self.pool,
            &auth.user_id,
            "revoke_invite",
            "invite",
            &req.invite_code,
            &serde_json::json!({}),
        )
        .await
        .ok();

        Ok(Response::new(RevokeInviteResponse {}))
    }

    async fn suspend_user(
        &self,
        request: Request<SuspendUserRequest>,
    ) -> Result<Response<SuspendUserResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target = UserId::parse(&req.user_id)
            .map_err(|_| Status::invalid_argument("invalid user_id"))?;

        if target == auth.user_id {
            return Err(Status::invalid_argument("cannot suspend yourself"));
        }

        let result = sqlx::query(
            r#"UPDATE users SET account_state = 'suspended' WHERE id = $1 AND account_state = 'active'"#,
        )
        .bind(target.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "suspend user failed");
            Status::internal("internal error")
        })?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("user not found or already suspended"));
        }

        sqlx::query(r#"UPDATE sessions SET revoked_at = NOW() WHERE user_id = $1 AND revoked_at IS NULL"#)
            .bind(target.as_uuid())
            .execute(&self.pool)
            .await
            .ok();

        crate::audit::log_action(
            &self.pool,
            &auth.user_id,
            "suspend_user",
            "user",
            &req.user_id,
            &serde_json::json!({"reason": req.reason}),
        )
        .await
        .ok();

        tracing::warn!(admin = %auth.user_id, target = %target, "user suspended");
        Ok(Response::new(SuspendUserResponse {}))
    }

    async fn unsuspend_user(
        &self,
        request: Request<UnsuspendUserRequest>,
    ) -> Result<Response<UnsuspendUserResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target = UserId::parse(&req.user_id)
            .map_err(|_| Status::invalid_argument("invalid user_id"))?;

        let result = sqlx::query(
            r#"UPDATE users SET account_state = 'active' WHERE id = $1 AND account_state = 'suspended'"#,
        )
        .bind(target.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "unsuspend user failed");
            Status::internal("internal error")
        })?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("user not found or not suspended"));
        }

        crate::audit::log_action(
            &self.pool,
            &auth.user_id,
            "unsuspend_user",
            "user",
            &req.user_id,
            &serde_json::json!({}),
        )
        .await
        .ok();

        Ok(Response::new(UnsuspendUserResponse {}))
    }

    async fn get_system_health(
        &self,
        request: Request<GetSystemHealthRequest>,
    ) -> Result<Response<GetSystemHealthResponse>, Status> {
        let _auth = self.auth(request.metadata())?;

        let db_healthy = sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok();

        let redis_healthy = {
            let mut conn = self.redis_conn.clone();
            redis::cmd("PING")
                .query_async::<String>(&mut conn)
                .await
                .is_ok()
        };

        let storage_healthy = self
            .s3_client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .is_ok();

        let active_users: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM users WHERE account_state = 'active'",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        let total_posts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM posts WHERE NOT is_deleted",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        let storage_used: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(file_size_bytes), 0) FROM media_items",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        Ok(Response::new(GetSystemHealthResponse {
            database_healthy: db_healthy,
            redis_healthy,
            storage_healthy,
            worker_healthy: true,
            active_users,
            total_posts,
            storage_used_bytes: storage_used,
        }))
    }

    async fn get_audit_log(
        &self,
        request: Request<GetAuditLogRequest>,
    ) -> Result<Response<GetAuditLogResponse>, Status> {
        let _auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let page_size = req.page_size.clamp(1, 100);

        let (rows, has_cursor) = if req.cursor.is_empty() {
            let rows = if let Some(ref actor_id) = req.actor_id {
                let actor_uuid = uuid::Uuid::parse_str(actor_id)
                    .map_err(|_| Status::invalid_argument("invalid actor_id"))?;
                sqlx::query(
                    r#"
                    SELECT id, actor_id, action, target_type, target_id, details, created_at
                    FROM audit_log
                    WHERE actor_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    "#,
                )
                .bind(actor_uuid)
                .bind(page_size + 1)
                .fetch_all(&self.pool)
                .await
            } else {
                sqlx::query(
                    r#"
                    SELECT id, actor_id, action, target_type, target_id, details, created_at
                    FROM audit_log
                    ORDER BY created_at DESC
                    LIMIT $1
                    "#,
                )
                .bind(page_size + 1)
                .fetch_all(&self.pool)
                .await
            };
            (
                rows.map_err(|e| {
                    tracing::error!(error = %e, "audit log query failed");
                    Status::internal("internal error")
                })?,
                false,
            )
        } else {
            let cursor_ts = chrono::DateTime::parse_from_rfc3339(&req.cursor)
                .map_err(|_| Status::invalid_argument("invalid cursor"))?
                .with_timezone(&chrono::Utc);

            let rows = sqlx::query(
                r#"
                SELECT id, actor_id, action, target_type, target_id, details, created_at
                FROM audit_log
                WHERE created_at < $1
                ORDER BY created_at DESC
                LIMIT $2
                "#,
            )
            .bind(cursor_ts)
            .bind(page_size + 1)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "audit log query failed");
                Status::internal("internal error")
            })?;

            (rows, true)
        };

        let has_more = rows.len() as i32 > page_size;
        let entries: Vec<AuditEntry> = rows
            .iter()
            .take(page_size as usize)
            .map(|r| {
                let id: uuid::Uuid = r.get(0);
                let actor_id: uuid::Uuid = r.get(1);
                let action: String = r.get(2);
                let target_type: String = r.get(3);
                let target_id: String = r.get(4);
                let details: serde_json::Value = r.get(5);
                let created_at: chrono::DateTime<chrono::Utc> = r.get(6);

                AuditEntry {
                    entry_id: id.to_string(),
                    actor_id: actor_id.to_string(),
                    action,
                    target_type,
                    target_id,
                    details: serde_json::to_string(&details).unwrap_or_default(),
                    timestamp: Some(prost_types::Timestamp {
                        seconds: created_at.timestamp(),
                        nanos: created_at.timestamp_subsec_nanos() as i32,
                    }),
                }
            })
            .collect();

        let next_cursor = if has_more {
            entries
                .last()
                .and_then(|e| e.timestamp.as_ref())
                .map(|ts| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default()
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(GetAuditLogResponse {
            entries,
            next_cursor,
            has_more,
        }))
    }
}
