//! Authentication service and gRPC handler.

use crate::config::CoveConfig;
use crate::crypto::{PasswordHasher, TokenService};
use cove_common::auth_context::AuthContext;
use cove_common::error::{CoveError, CoveResult};
use cove_common::id::{SessionId, UserId};
use cove_proto::cove::auth::auth_service_server::AuthService;
use cove_proto::cove::auth::{
    ListSessionsRequest, ListSessionsResponse, LoginRequest, LoginResponse, LogoutRequest,
    LogoutResponse, RefreshTokenRequest, RefreshTokenResponse, RegisterRequest, RegisterResponse,
    RevokeSessionRequest, RevokeSessionResponse, SessionInfo, ValidateInviteRequest,
    ValidateInviteResponse,
};
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tonic::metadata::MetadataMap;

/// JWT claims for access token validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    pub user_id: String,
    pub session_id: String,
    pub is_admin: bool,
    pub exp: i64,
    pub iat: i64,
}

/// Extracts AuthContext from the `authorization` metadata (Bearer token).
/// Verifies the JWT and returns the claims as AuthContext.
pub fn extract_auth(
    request_metadata: &tonic::metadata::MetadataMap,
    jwt_secret: &str,
) -> CoveResult<AuthContext> {
    let auth_header = request_metadata
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| CoveError::Unauthorized("missing authorization header".into()))?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .or_else(|| auth_header.strip_prefix("bearer "))
        .ok_or_else(|| CoveError::Unauthorized("invalid authorization format".into()))?;

    let mut validation = Validation::default();
    validation.validate_exp = true;

    let token_data = decode::<JwtClaims>(
        token,
        &DecodingKey::from_secret(jwt_secret.as_bytes()),
        &validation,
    )
    .map_err(|e| CoveError::Unauthorized(format!("invalid token: {}", e)))?;

    let user_id = UserId::parse(&token_data.claims.user_id)
        .map_err(|e| CoveError::Unauthorized(format!("invalid user_id in token: {}", e)))?;
    let session_id = SessionId::parse(&token_data.claims.session_id)
        .map_err(|e| CoveError::Unauthorized(format!("invalid session_id in token: {}", e)))?;

    Ok(AuthContext {
        user_id,
        session_id,
        is_admin: token_data.claims.is_admin,
    })
}

/// Authentication service implementation.
pub struct AuthServiceImpl {
    pool: PgPool,
    token_service: TokenService,
    password_hasher: Arc<PasswordHasher>,
    config: Arc<CoveConfig>,
}

impl AuthServiceImpl {
    pub fn new(
        pool: PgPool,
        token_service: TokenService,
        password_hasher: Arc<PasswordHasher>,
        config: Arc<CoveConfig>,
    ) -> Self {
        Self {
            pool,
            token_service,
            password_hasher,
            config,
        }
    }

    fn auth(&self, request_metadata: &MetadataMap) -> Result<AuthContext, Status> {
        extract_auth(request_metadata, &self.config.auth.jwt_secret).map_err(Into::into)
    }
}

#[tonic::async_trait]
impl AuthService for AuthServiceImpl {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();

        tracing::info!(username = %req.username, "register attempt");

        if req.invite_code.is_empty() {
            return Err(Status::invalid_argument("invite code required"));
        }
        if req.username.is_empty() {
            return Err(Status::invalid_argument("username required"));
        }
        if req.email.is_empty() {
            return Err(Status::invalid_argument("email required"));
        }
        if req.password.is_empty() {
            return Err(Status::invalid_argument("password required"));
        }

        // Validate invite code and get inviter
        let invite_row = sqlx::query(
            r#"
            SELECT i.id, i.created_by, i.max_uses, i.use_count, i.expires_at, i.revoked, u.username
            FROM invites i
            JOIN users u ON u.id = i.created_by
            WHERE i.code = $1
            "#,
        )
        .bind(&req.invite_code)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        let invite = invite_row.ok_or_else(|| Status::invalid_argument("invalid invite code"))?;
        let invite_id: uuid::Uuid = invite.get(0);
        let created_by: uuid::Uuid = invite.get(1);
        let max_uses: i32 = invite.get(2);
        let use_count: i32 = invite.get(3);
        let expires_at: Option<chrono::DateTime<chrono::Utc>> = invite.get(4);
        let revoked: bool = invite.get(5);
        let invited_by_username: String = invite.get(6);

        if revoked {
            return Err(Status::invalid_argument("invite code has been revoked"));
        }
        if use_count >= max_uses {
            return Err(Status::invalid_argument("invite code has reached max uses"));
        }
        if let Some(exp) = expires_at {
            if exp < chrono::Utc::now() {
                return Err(Status::invalid_argument("invite code has expired"));
            }
        }

        // Check username uniqueness
        let username_taken = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)"#,
        )
        .bind(&req.username)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        if username_taken {
            return Err(Status::already_exists("username already taken"));
        }

        // Check email uniqueness
        let email_taken = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)"#,
        )
        .bind(&req.email)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        if email_taken {
            return Err(Status::already_exists("email already taken"));
        }

        let password_hash = self
            .password_hasher
            .hash(&req.password)
            .map_err(|e| {
                tracing::error!(error = %e, "password hash failed");
                Status::internal("internal error")
            })?;

        let user_id = UserId::new();
        let session_id = SessionId::new();
        let refresh_token = self.token_service.generate_refresh_token();
        let refresh_hash = self.token_service.hash_refresh_token(&refresh_token);

        let display_name = req
            .display_name
            .trim()
            .to_string()
            .chars()
            .take(100)
            .collect::<String>();

        let mut tx = self.pool.begin().await.map_err(|e| {
            tracing::error!(error = %e, "begin transaction failed");
            Status::internal("internal error")
        })?;

        sqlx::query(
            r#"
            INSERT INTO users (id, username, email, password_hash, display_name, invited_by)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(user_id.as_uuid())
        .bind(&req.username)
        .bind(&req.email)
        .bind(&password_hash)
        .bind(&display_name)
        .bind(created_by)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "insert user failed");
            Status::internal("internal error")
        })?;

        sqlx::query(
            r#"
            INSERT INTO profiles (user_id)
            VALUES ($1)
            "#,
        )
        .bind(user_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "insert profile failed");
            Status::internal("internal error")
        })?;

        sqlx::query(
            r#"
            INSERT INTO sessions (id, user_id, refresh_token_hash, device_id, device_name)
            VALUES ($1, $2, $3, '', '')
            "#,
        )
        .bind(session_id.as_uuid())
        .bind(user_id.as_uuid())
        .bind(&refresh_hash)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "insert session failed");
            Status::internal("internal error")
        })?;

        sqlx::query(
            r#"
            UPDATE invites SET use_count = use_count + 1 WHERE id = $1
            "#,
        )
        .bind(invite_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "update invite count failed");
            Status::internal("internal error")
        })?;

        tx.commit().await.map_err(|e| {
            tracing::error!(error = %e, "commit failed");
            Status::internal("internal error")
        })?;

        let is_admin = false;
        let access_token = self
            .token_service
            .create_access_token(&user_id.to_string(), &session_id.to_string(), is_admin)
            .map_err(|e| {
                tracing::error!(error = %e, "create access token failed");
                Status::internal("internal error")
            })?;

        let ttl = self.token_service.refresh_token_ttl_secs();
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(ttl as i64);

        tracing::info!(user_id = %user_id, "user registered successfully");

        Ok(Response::new(RegisterResponse {
            user_id: user_id.to_string(),
            access_token,
            refresh_token,
            expires_at: Some(prost_types::Timestamp {
                seconds: expires_at.timestamp(),
                nanos: expires_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let req = request.into_inner();

        tracing::info!(username_or_email = %req.username_or_email, "login attempt");

        if req.username_or_email.is_empty() || req.password.is_empty() {
            return Err(Status::invalid_argument("username/email and password required"));
        }

        let user_row = sqlx::query(
            r#"
            SELECT id, username, email, password_hash, is_admin
            FROM users
            WHERE (username = $1 OR email = $1) AND account_state = 'active'
            "#,
        )
        .bind(&req.username_or_email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        let user = user_row.ok_or_else(|| Status::unauthenticated("invalid credentials"))?;
        let user_id: uuid::Uuid = user.get(0);
        let password_hash: String = user.get(3);
        let is_admin: bool = user.get(4);

        let valid = self
            .password_hasher
            .verify(&req.password, &password_hash)
            .map_err(|e| {
                tracing::error!(error = %e, "password verify failed");
                Status::internal("internal error")
            })?;

        if !valid {
            return Err(Status::unauthenticated("invalid credentials"));
        }

        let user_id = UserId::from_uuid(user_id);
        let session_id = SessionId::new();
        let refresh_token = self.token_service.generate_refresh_token();
        let refresh_hash = self.token_service.hash_refresh_token(&refresh_token);

        let device_id = req.device_id.trim().chars().take(128).collect::<String>();
        let device_name = req.device_name.trim().chars().take(128).collect::<String>();

        sqlx::query(
            r#"
            INSERT INTO sessions (id, user_id, refresh_token_hash, device_id, device_name)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(session_id.as_uuid())
        .bind(user_id.as_uuid())
        .bind(&refresh_hash)
        .bind(&device_id)
        .bind(&device_name)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "insert session failed");
            Status::internal("internal error")
        })?;

        let access_token = self
            .token_service
            .create_access_token(&user_id.to_string(), &session_id.to_string(), is_admin)
            .map_err(|e| {
                tracing::error!(error = %e, "create access token failed");
                Status::internal("internal error")
            })?;

        let ttl = self.token_service.refresh_token_ttl_secs();
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(ttl as i64);

        tracing::info!(user_id = %user_id, "login successful");

        Ok(Response::new(LoginResponse {
            user_id: user_id.to_string(),
            access_token,
            refresh_token,
            expires_at: Some(prost_types::Timestamp {
                seconds: expires_at.timestamp(),
                nanos: expires_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn refresh_token(
        &self,
        request: Request<RefreshTokenRequest>,
    ) -> Result<Response<RefreshTokenResponse>, Status> {
        let req = request.into_inner();

        if req.refresh_token.is_empty() {
            return Err(Status::invalid_argument("refresh token required"));
        }

        let refresh_hash = self.token_service.hash_refresh_token(&req.refresh_token);

        let session_row = sqlx::query(
            r#"
            SELECT s.id, s.user_id, u.is_admin
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.refresh_token_hash = $1 AND s.revoked_at IS NULL
            "#,
        )
        .bind(&refresh_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        let session = session_row
            .ok_or_else(|| Status::unauthenticated("invalid or revoked refresh token"))?;

        let session_id: uuid::Uuid = session.get(0);
        let user_id: uuid::Uuid = session.get(1);
        let is_admin: bool = session.get(2);

        let session_id = SessionId::from_uuid(session_id);
        let user_id = UserId::from_uuid(user_id);

        // Rotate refresh token
        let new_refresh_token = self.token_service.generate_refresh_token();
        let new_refresh_hash = self.token_service.hash_refresh_token(&new_refresh_token);

        sqlx::query(
            r#"
            UPDATE sessions
            SET refresh_token_hash = $1, last_used_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(&new_refresh_hash)
        .bind(session_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "update session failed");
            Status::internal("internal error")
        })?;

        let access_token = self
            .token_service
            .create_access_token(&user_id.to_string(), &session_id.to_string(), is_admin)
            .map_err(|e| {
                tracing::error!(error = %e, "create access token failed");
                Status::internal("internal error")
            })?;

        let ttl = self.token_service.refresh_token_ttl_secs();
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(ttl as i64);

        Ok(Response::new(RefreshTokenResponse {
            access_token,
            refresh_token: new_refresh_token,
            expires_at: Some(prost_types::Timestamp {
                seconds: expires_at.timestamp(),
                nanos: expires_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> Result<Response<LogoutResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let session_id = SessionId::parse(&req.session_id)
            .map_err(|_| Status::invalid_argument("invalid session_id"))?;

        // Must own the session
        let rows = sqlx::query(
            r#"
            UPDATE sessions SET revoked_at = NOW()
            WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL
            "#,
        )
        .bind(session_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        if rows.rows_affected() == 0 {
            return Err(Status::permission_denied("session not found or already revoked"));
        }

        tracing::info!(user_id = %auth.user_id, session_id = %session_id, "logout");

        Ok(Response::new(LogoutResponse {}))
    }

    async fn revoke_session(
        &self,
        request: Request<RevokeSessionRequest>,
    ) -> Result<Response<RevokeSessionResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let session_id = SessionId::parse(&req.session_id)
            .map_err(|_| Status::invalid_argument("invalid session_id"))?;

        let rows = sqlx::query(
            r#"
            UPDATE sessions SET revoked_at = NOW()
            WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL
            "#,
        )
        .bind(session_id.as_uuid())
        .bind(auth.user_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        if rows.rows_affected() == 0 {
            return Err(Status::permission_denied("session not found or already revoked"));
        }

        tracing::info!(user_id = %auth.user_id, session_id = %session_id, "session revoked");

        Ok(Response::new(RevokeSessionResponse {}))
    }

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let auth = self.auth(request.metadata())?;

        let rows = sqlx::query_as::<_, (uuid::Uuid, String, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"
            SELECT id, device_id, device_name, created_at, last_used_at
            FROM sessions
            WHERE user_id = $1 AND revoked_at IS NULL
            ORDER BY last_used_at DESC
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        let sessions: Vec<SessionInfo> = rows
            .into_iter()
            .map(|(id, device_id, device_name, created_at, last_used_at)| {
                let is_current = id == auth.session_id.into_uuid();
                SessionInfo {
                    session_id: id.to_string(),
                    device_id,
                    device_name,
                    created_at: Some(prost_types::Timestamp {
                        seconds: created_at.timestamp(),
                        nanos: created_at.timestamp_subsec_nanos() as i32,
                    }),
                    last_used_at: Some(prost_types::Timestamp {
                        seconds: last_used_at.timestamp(),
                        nanos: last_used_at.timestamp_subsec_nanos() as i32,
                    }),
                    is_current,
                }
            })
            .collect();

        Ok(Response::new(ListSessionsResponse { sessions }))
    }

    async fn validate_invite(
        &self,
        request: Request<ValidateInviteRequest>,
    ) -> Result<Response<ValidateInviteResponse>, Status> {
        let req = request.into_inner();

        if req.invite_code.is_empty() {
            return Ok(Response::new(ValidateInviteResponse {
                valid: false,
                invited_by_username: String::new(),
            }));
        }

        let invite_row = sqlx::query(
            r#"
            SELECT i.id, i.max_uses, i.use_count, i.expires_at, i.revoked, u.username
            FROM invites i
            JOIN users u ON u.id = i.created_by
            WHERE i.code = $1
            "#,
        )
        .bind(&req.invite_code)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "database error");
            Status::internal("internal error")
        })?;

        let invite = match invite_row {
            Some(row) => row,
            None => {
                return Ok(Response::new(ValidateInviteResponse {
                    valid: false,
                    invited_by_username: String::new(),
                }))
            }
        };

        let max_uses: i32 = invite.get(1);
        let use_count: i32 = invite.get(2);
        let expires_at: Option<chrono::DateTime<chrono::Utc>> = invite.get(3);
        let revoked: bool = invite.get(4);
        let invited_by_username: String = invite.get(5);

        let valid = !revoked && use_count < max_uses;
        let valid = valid
            && expires_at
                .map(|exp| exp >= chrono::Utc::now())
                .unwrap_or(true);

        Ok(Response::new(ValidateInviteResponse {
            valid,
            invited_by_username: if valid {
                invited_by_username
            } else {
                String::new()
            },
        }))
    }
}
