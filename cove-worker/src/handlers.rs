//! Job handler implementations for the background worker.

use sqlx::{PgPool, Row};
use std::path::Path;
use uuid::Uuid;

type JobResult = Result<(), Box<dyn std::error::Error>>;

/// Feed fanout: insert feed_entries for all followers of the post author.
pub async fn handle_feed_fanout(
    pool: &PgPool,
    redis_conn: &redis::aio::ConnectionManager,
    payload: &serde_json::Value,
) -> JobResult {
    let post_id = payload["post_id"]
        .as_str()
        .ok_or("missing post_id")?;
    let author_id = payload["author_id"]
        .as_str()
        .ok_or("missing author_id")?;

    let post_uuid = Uuid::parse_str(post_id)?;
    let author_uuid = Uuid::parse_str(author_id)?;

    let follower_ids: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT follower_id FROM follows
        WHERE followee_id = $1 AND state = 'accepted'
        "#,
    )
    .bind(author_uuid)
    .fetch_all(pool)
    .await?;

    if follower_ids.is_empty() {
        tracing::info!(post_id = post_id, "no followers to fan out to");
        return Ok(());
    }

    let post_created_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT created_at FROM posts WHERE id = $1",
    )
    .bind(post_uuid)
    .fetch_one(pool)
    .await?;

    for follower_id in &follower_ids {
        let entry_id = Uuid::now_v7();

        sqlx::query(
            r#"
            INSERT INTO feed_entries (id, user_id, post_id, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, post_id) DO NOTHING
            "#,
        )
        .bind(entry_id)
        .bind(follower_id)
        .bind(post_uuid)
        .bind(post_created_at)
        .execute(pool)
        .await?;
    }

    sqlx::query(
        r#"
        INSERT INTO feed_entries (id, user_id, post_id, created_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id, post_id) DO NOTHING
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(author_uuid)
    .bind(post_uuid)
    .bind(post_created_at)
    .execute(pool)
    .await?;

    sqlx::query("UPDATE profiles SET post_count = post_count + 1 WHERE user_id = $1")
        .bind(author_uuid)
        .execute(pool)
        .await?;

    let mut conn = redis_conn.clone();
    for follower_id in &follower_ids {
        let key = format!("feed:{}:0", follower_id);
        let _: Result<(), _> = redis::AsyncCommands::del(&mut conn, &key).await;
    }
    let author_key = format!("feed:{}:0", author_uuid);
    let _: Result<(), _> = redis::AsyncCommands::del(&mut conn, &author_key).await;

    tracing::info!(
        post_id = post_id,
        follower_count = follower_ids.len(),
        "feed fanout complete"
    );

    Ok(())
}

/// Media processing: validate, strip EXIF, generate thumbnails and display variants.
pub async fn handle_media_processing(
    pool: &PgPool,
    storage_base: &Path,
    payload: &serde_json::Value,
) -> JobResult {
    let media_id = payload["media_id"]
        .as_str()
        .ok_or("missing media_id")?;

    let media_uuid = Uuid::parse_str(media_id)?;

    let row = sqlx::query(
        r#"
        SELECT original_key, media_type, content_type
        FROM media_items
        WHERE id = $1 AND processing_state = 'processing'
        "#,
    )
    .bind(media_uuid)
    .fetch_optional(pool)
    .await?;

    let row = row.ok_or("media not found or not in processing state")?;
    let original_key: String = row.get(0);
    let media_type: String = row.get(1);
    let _content_type: String = row.get(2);

    tracing::info!(media_id = media_id, media_type = %media_type, "processing media");

    let original_path = storage_base.join(&original_key);
    let body = tokio::fs::read(&original_path)
        .await
        .map_err(|e| format!("read {}: {}", original_path.display(), e))?;

    if media_type == "photo" {
        process_image(pool, storage_base, media_uuid, &original_key, &body).await?;
    } else if media_type == "video" {
        process_video(pool, media_uuid, &body).await?;
    }

    sqlx::query(
        "UPDATE media_items SET processing_state = 'completed' WHERE id = $1",
    )
    .bind(media_uuid)
    .execute(pool)
    .await?;

    tracing::info!(media_id = media_id, "media processing complete");
    Ok(())
}

async fn process_image(
    pool: &PgPool,
    storage_base: &Path,
    media_uuid: Uuid,
    original_key: &str,
    image_data: &[u8],
) -> JobResult {
    let img = image::load_from_memory(image_data)
        .map_err(|e| format!("invalid image: {}", e))?;

    let (width, height) = (img.width(), img.height());
    let aspect_ratio = width as f64 / height.max(1) as f64;

    let thumbnail = img.thumbnail(200, 200);
    let mut thumb_bytes = Vec::new();
    thumbnail.write_to(
        &mut std::io::Cursor::new(&mut thumb_bytes),
        image::ImageFormat::Jpeg,
    )?;

    let feed_img = if width > 800 {
        img.resize(800, (800.0 / aspect_ratio) as u32, image::imageops::FilterType::Lanczos3)
    } else {
        img.clone()
    };
    let mut feed_bytes = Vec::new();
    feed_img.write_to(
        &mut std::io::Cursor::new(&mut feed_bytes),
        image::ImageFormat::Jpeg,
    )?;

    let display_img = if width > 1600 {
        img.resize(1600, (1600.0 / aspect_ratio) as u32, image::imageops::FilterType::Lanczos3)
    } else {
        img.clone()
    };
    let mut display_bytes = Vec::new();
    display_img.write_to(
        &mut std::io::Cursor::new(&mut display_bytes),
        image::ImageFormat::Jpeg,
    )?;

    let base_key = original_key
        .rsplit_once('/')
        .map(|(base, _)| base)
        .unwrap_or("media");

    let thumb_key = format!("{}/thumbnail.jpg", base_key);
    let feed_key = format!("{}/feed.jpg", base_key);
    let display_key = format!("{}/display.jpg", base_key);

    write_file(storage_base, &thumb_key, &thumb_bytes).await?;
    write_file(storage_base, &feed_key, &feed_bytes).await?;
    write_file(storage_base, &display_key, &display_bytes).await?;

    sqlx::query(
        r#"
        UPDATE media_items
        SET width = $1, height = $2, aspect_ratio = $3,
            thumbnail_key = $4, feed_key = $5, display_key = $6,
            file_size_bytes = $7
        WHERE id = $8
        "#,
    )
    .bind(width as i32)
    .bind(height as i32)
    .bind(aspect_ratio)
    .bind(&thumb_key)
    .bind(&feed_key)
    .bind(&display_key)
    .bind(image_data.len() as i64)
    .bind(media_uuid)
    .execute(pool)
    .await?;

    Ok(())
}

async fn process_video(
    pool: &PgPool,
    media_uuid: Uuid,
    video_data: &[u8],
) -> JobResult {
    sqlx::query(
        r#"
        UPDATE media_items
        SET file_size_bytes = $1, width = 1920, height = 1080, aspect_ratio = 1.78
        WHERE id = $2
        "#,
    )
    .bind(video_data.len() as i64)
    .bind(media_uuid)
    .execute(pool)
    .await?;

    tracing::info!(media_id = %media_uuid, "video processed (metadata only for v1)");
    Ok(())
}

async fn write_file(
    storage_base: &Path,
    key: &str,
    data: &[u8],
) -> JobResult {
    let path = storage_base.join(key);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("mkdir failed for {}: {}", parent.display(), e))?;
    }
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, data)
        .await
        .map_err(|e| format!("write failed for {}: {}", path.display(), e))?;
    tokio::fs::rename(&tmp_path, &path)
        .await
        .map_err(|e| format!("rename failed for {}: {}", path.display(), e))?;
    Ok(())
}

/// Notification job handler: creates a notification record from the job payload.
pub async fn handle_notification(
    pool: &PgPool,
    payload: &serde_json::Value,
) -> JobResult {
    let recipient_id = payload["recipient_id"]
        .as_str()
        .ok_or("missing recipient_id")?;
    let actor_id = payload["actor_id"]
        .as_str()
        .ok_or("missing actor_id")?;
    let notification_type = payload["notification_type"]
        .as_str()
        .ok_or("missing notification_type")?;
    let target_id = payload["target_id"].as_str().unwrap_or("");
    let message = payload["message"].as_str().unwrap_or("");

    let recipient_uuid = Uuid::parse_str(recipient_id)?;
    let actor_uuid = Uuid::parse_str(actor_id)?;
    let target_uuid = if target_id.is_empty() {
        None
    } else {
        Some(Uuid::parse_str(target_id)?)
    };

    sqlx::query(
        r#"
        INSERT INTO notifications (id, recipient_id, actor_id, notification_type, target_type, target_id, message)
        VALUES ($1, $2, $3, $4, 'post', $5, $6)
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(recipient_uuid)
    .bind(actor_uuid)
    .bind(notification_type)
    .bind(target_uuid)
    .bind(message)
    .execute(pool)
    .await?;

    tracing::info!(
        recipient = recipient_id,
        notification_type = notification_type,
        "notification created"
    );

    Ok(())
}
