//! CoveServer Background Worker
//! Polls the jobs table and processes async tasks: feed fanout, media processing,
//! notification delivery, EXIF stripping, thumbnail generation, and search indexing.

use cove_server::config::CoveConfig;
use cove_server::push::PushService;
use cove_server::storage::object_store::SupabaseStorageService;
use sqlx::{postgres::PgPoolOptions, Row};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod handlers;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "cove_worker=info".into()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    tracing::info!("starting CoveServer worker");

    let config = CoveConfig::load().expect("failed to load configuration");

    let pool = PgPoolOptions::new()
        .max_connections(config.database.max_connections)
        .min_connections(config.database.min_connections)
        .connect(&config.database.url)
        .await
        .expect("failed to connect to PostgreSQL");
    let push = Arc::new(PushService::new(pool.clone(), &config.push)?);

    let storage = SupabaseStorageService::new(
        config.storage.endpoint.clone(),
        config.storage.bucket.clone(),
        config.storage.api_key.clone(),
    )
    .expect("failed to configure Supabase storage");

    storage
        .ensure_bucket_exists()
        .await
        .expect("failed to ensure Supabase storage bucket exists");

    storage
        .health_check()
        .await
        .expect("failed to connect to Supabase storage");

    tracing::info!(
        endpoint = %storage.endpoint(),
        bucket = %storage.bucket(),
        "supabase storage initialized"
    );

    let worker = Worker {
        pool,
        storage,
        push,
    };

    tracing::info!("worker polling started");
    worker.run().await;

    Ok(())
}

struct Worker {
    pool: sqlx::PgPool,
    storage: SupabaseStorageService,
    push: Arc<PushService>,
}

impl Worker {
    async fn run(&self) {
        loop {
            match self.poll_and_process().await {
                Ok(processed) => {
                    if processed == 0 {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "worker poll error");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn poll_and_process(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let rows = sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'running', started_at = NOW(), attempts = attempts + 1
            WHERE id IN (
                SELECT id FROM jobs
                WHERE state = 'pending' AND run_at <= NOW()
                ORDER BY run_at ASC
                LIMIT 10
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, job_type, payload
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let count = rows.len();

        for row in rows {
            let job_id: uuid::Uuid = row.get(0);
            let job_type: String = row.get(1);
            let payload: serde_json::Value = row.get(2);

            tracing::info!(job_id = %job_id, job_type = %job_type, "processing job");

            let result = match job_type.as_str() {
                "feed_fanout" => handlers::handle_feed_fanout(&self.pool, &payload).await,
                "media_processing" => {
                    handlers::handle_media_processing(&self.pool, &self.storage, &payload).await
                }
                "notification" => {
                    handlers::handle_notification(&self.pool, self.push.as_ref(), &payload).await
                }
                other => {
                    tracing::warn!(job_type = other, "unknown job type");
                    Ok(())
                }
            };

            match result {
                Ok(()) => {
                    sqlx::query(
                        "UPDATE jobs SET state = 'completed', completed_at = NOW() WHERE id = $1",
                    )
                    .bind(job_id)
                    .execute(&self.pool)
                    .await?;
                    tracing::info!(job_id = %job_id, "job completed");
                }
                Err(e) => {
                    tracing::error!(job_id = %job_id, error = %e, "job failed");

                    let max_attempts: i32 =
                        sqlx::query_scalar("SELECT max_attempts FROM jobs WHERE id = $1")
                            .bind(job_id)
                            .fetch_one(&self.pool)
                            .await
                            .unwrap_or(3);

                    let attempts: i32 =
                        sqlx::query_scalar("SELECT attempts FROM jobs WHERE id = $1")
                            .bind(job_id)
                            .fetch_one(&self.pool)
                            .await
                            .unwrap_or(1);

                    let new_state = if attempts >= max_attempts {
                        "dead"
                    } else {
                        "pending"
                    };

                    let backoff_secs = (2i64).pow(attempts as u32).min(300);

                    sqlx::query(
                        r#"
                        UPDATE jobs
                        SET state = $1, last_error = $2, run_at = NOW() + ($3 || ' seconds')::interval
                        WHERE id = $4
                        "#,
                    )
                    .bind(new_state)
                    .bind(e.to_string())
                    .bind(backoff_secs.to_string())
                    .bind(job_id)
                    .execute(&self.pool)
                    .await?;
                }
            }
        }

        Ok(count)
    }
}
