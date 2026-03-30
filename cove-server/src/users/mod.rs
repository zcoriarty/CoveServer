//! User service and gRPC handler.

use crate::auth;
use crate::crypto::PasswordHasher;
use cove_common::auth_context::AuthContext;
use cove_common::id::UserId;
use cove_proto::cove::user::{
    user_service_server::UserService, AccountState, ChangePasswordRequest, ChangePasswordResponse,
    DeactivateAccountRequest, DeactivateAccountResponse, GetUserRequest, GetUserResponse,
    UpdateUserRequest, UpdateUserResponse,
};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// User service implementation.
pub struct UserServiceImpl {
    pool: PgPool,
    jwt_secret: String,
    password_hasher: Arc<PasswordHasher>,
}

impl UserServiceImpl {
    pub fn new(pool: PgPool, jwt_secret: String, password_hasher: Arc<PasswordHasher>) -> Self {
        Self {
            pool,
            jwt_secret,
            password_hasher,
        }
    }

    fn auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<AuthContext, Status> {
        auth::extract_auth(metadata, &self.jwt_secret).map_err(Into::into)
    }
}

#[tonic::async_trait]
impl UserService for UserServiceImpl {
    async fn get_user(
        &self,
        request: Request<GetUserRequest>,
    ) -> Result<Response<GetUserResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        let target_id =
            UserId::parse(&req.user_id).map_err(|_| Status::invalid_argument("invalid user_id"))?;

        if auth.user_id != target_id && !auth.is_admin {
            return Err(Status::permission_denied("must be self or admin"));
        }

        let row = sqlx::query(
            r#"
            SELECT id, username, email, account_state, created_at
            FROM users
            WHERE id = $1
            "#,
        )
        .bind(target_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        let row = row.ok_or_else(|| Status::not_found("user not found"))?;

        let user_id: uuid::Uuid = row.get(0);
        let username: String = row.get(1);
        let email: String = row.get(2);
        let state_str: String = row.get(3);
        let created_at: chrono::DateTime<chrono::Utc> = row.get(4);

        let state = match state_str.as_str() {
            "active" => AccountState::Active as i32,
            "deactivated" => AccountState::Deactivated as i32,
            "suspended" => AccountState::Suspended as i32,
            _ => AccountState::Unspecified as i32,
        };

        Ok(Response::new(GetUserResponse {
            user_id: user_id.to_string(),
            username,
            email,
            state,
            created_at: Some(prost_types::Timestamp {
                seconds: created_at.timestamp(),
                nanos: created_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn update_user(
        &self,
        request: Request<UpdateUserRequest>,
    ) -> Result<Response<UpdateUserResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        // UpdateUser: authorized - must be self (we always update current user's email)
        if req.email.is_empty() {
            return Err(Status::invalid_argument("email required"));
        }

        let row = sqlx::query(
            r#"
            UPDATE users
            SET email = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING id, email
            "#,
        )
        .bind(&req.email)
        .bind(auth.user_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        let row = row.ok_or_else(|| Status::not_found("user not found"))?;

        let user_id: uuid::Uuid = row.get(0);
        let email: String = row.get(1);

        Ok(Response::new(UpdateUserResponse {
            user_id: user_id.to_string(),
            email,
        }))
    }

    async fn change_password(
        &self,
        request: Request<ChangePasswordRequest>,
    ) -> Result<Response<ChangePasswordResponse>, Status> {
        let auth = self.auth(request.metadata())?;
        let req = request.into_inner();

        if req.current_password.is_empty() || req.new_password.is_empty() {
            return Err(Status::invalid_argument(
                "current and new password required",
            ));
        }

        let row = sqlx::query(
            r#"
            SELECT password_hash FROM users WHERE id = $1
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        let row = row.ok_or_else(|| Status::not_found("user not found"))?;
        let password_hash: String = row.get(0);

        let valid = self
            .password_hasher
            .verify(&req.current_password, &password_hash)
            .map_err(|e| Status::internal("crypto error"))?;

        if !valid {
            return Err(Status::unauthenticated("invalid current password"));
        }

        let new_hash = self
            .password_hasher
            .hash(&req.new_password)
            .map_err(|e| Status::internal("crypto error"))?;

        sqlx::query(
            r#"
            UPDATE users SET password_hash = $1, updated_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(&new_hash)
        .bind(auth.user_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        Ok(Response::new(ChangePasswordResponse {}))
    }

    async fn deactivate_account(
        &self,
        request: Request<DeactivateAccountRequest>,
    ) -> Result<Response<DeactivateAccountResponse>, Status> {
        let auth = self.auth(request.metadata())?;

        sqlx::query(
            r#"
            UPDATE users
            SET account_state = 'deactivated', updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(auth.user_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal("database error"))?;

        Ok(Response::new(DeactivateAccountResponse {}))
    }
}
