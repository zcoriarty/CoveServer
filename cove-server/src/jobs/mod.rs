//! Background job queue interface used by the API server to enqueue jobs.

use cove_common::error::{CoveError, CoveResult};
use sqlx::PgPool;

pub async fn enqueue(
    pool: &PgPool,
    job_type: &str,
    payload: &serde_json::Value,
) -> CoveResult<uuid::Uuid> {
    let job_id = uuid::Uuid::now_v7();
    sqlx::query(
        r#"
        INSERT INTO jobs (id, job_type, payload, state)
        VALUES ($1, $2, $3, 'pending')
        "#,
    )
    .bind(job_id)
    .bind(job_type)
    .bind(payload)
    .execute(pool)
    .await
    .map_err(|e| CoveError::Database(e.to_string()))?;
    Ok(job_id)
}

pub async fn get_pending_count(pool: &PgPool) -> CoveResult<i64> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE state = 'pending'")
        .fetch_one(pool)
        .await
        .map_err(|e| CoveError::Database(e.to_string()))?;
    Ok(count)
}
