//! Audit logging for privileged and security-sensitive actions.

use cove_common::error::{CoveError, CoveResult};
use cove_common::id::UserId;
use sqlx::PgPool;

pub async fn log_action(
    pool: &PgPool,
    actor_id: &UserId,
    action: &str,
    target_type: &str,
    target_id: &str,
    details: &serde_json::Value,
) -> CoveResult<()> {
    sqlx::query(
        r#"
        INSERT INTO audit_log (id, actor_id, action, target_type, target_id, details)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(uuid::Uuid::now_v7())
    .bind(actor_id.as_uuid())
    .bind(action)
    .bind(target_type)
    .bind(target_id)
    .bind(details)
    .execute(pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, action = action, "audit log write failed");
        CoveError::Database(e.to_string())
    })?;
    Ok(())
}
