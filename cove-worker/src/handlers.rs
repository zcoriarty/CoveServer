//! Job handler implementations for the background worker.

use cove_server::storage::object_store::SupabaseStorageService;
use exif::{In, Reader as ExifReader, Tag};
use image::{codecs::jpeg::JpegEncoder, DynamicImage, ImageReader};
use sqlx::{PgPool, Row};
use uuid::Uuid;

type JobResult = Result<(), Box<dyn std::error::Error>>;

/// Feed fanout: insert feed_entries for all followers of the post author.
pub async fn handle_feed_fanout(
    pool: &PgPool,
    payload: &serde_json::Value,
) -> JobResult {
    let post_id = payload["post_id"].as_str().ok_or("missing post_id")?;
    let author_id = payload["author_id"].as_str().ok_or("missing author_id")?;

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

    // Author's feed entry and post_count are handled synchronously in CreatePost.
    // The worker only fans out to followers.
    if follower_ids.is_empty() {
        tracing::info!(post_id = post_id, "no followers to fan out to");
        return Ok(());
    }

    let post_created_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT created_at FROM posts WHERE id = $1")
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

    for follower_id in &follower_ids {
        let notif_payload = serde_json::json!({
            "recipient_id": follower_id.to_string(),
            "actor_id": author_id,
            "notification_type": "new_post",
            "target_id": post_id,
            "message": ""
        });

        let _ = sqlx::query(
            r#"
            INSERT INTO jobs (id, job_type, payload, state, run_at)
            VALUES (gen_random_uuid(), 'notification', $1, 'pending', NOW())
            "#,
        )
        .bind(sqlx::types::Json(&notif_payload))
        .execute(pool)
        .await;
    }

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
    storage: &SupabaseStorageService,
    payload: &serde_json::Value,
) -> JobResult {
    let media_id = payload["media_id"].as_str().ok_or("missing media_id")?;

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

    let body = storage
        .download_object(&original_key)
        .await
        .map_err(|e| format!("download {} failed: {}", original_key, e))?;

    if media_type == "photo" {
        process_image(pool, storage, media_uuid, &original_key, &body).await?;
    } else if media_type == "video" {
        process_video(pool, media_uuid, &body).await?;
    }

    sqlx::query("UPDATE media_items SET processing_state = 'completed' WHERE id = $1")
        .bind(media_uuid)
        .execute(pool)
        .await?;

    tracing::info!(media_id = media_id, "media processing complete");
    Ok(())
}

async fn process_image(
    pool: &PgPool,
    storage: &SupabaseStorageService,
    media_uuid: Uuid,
    original_key: &str,
    image_data: &[u8],
) -> JobResult {
    let orientation = read_exif_orientation(image_data);
    tracing::debug!(media_id = %media_uuid, orientation = ?orientation, "EXIF orientation");

    let mut img = ImageReader::new(std::io::Cursor::new(image_data))
        .with_guessed_format()
        .map_err(|e| format!("invalid image format: {}", e))?
        .into_decoder()
        .and_then(DynamicImage::from_decoder)
        .map_err(|e| format!("invalid image: {}", e))?;
    img.apply_orientation(orientation);

    let (width, height) = (img.width(), img.height());
    let aspect_ratio = width as f64 / height.max(1) as f64;

    let thumbnail = img.thumbnail(480, 480);
    let thumb_bytes = encode_jpeg(&thumbnail, 88)?;

    let feed_img = if width > 800 {
        let target_height = ((800.0 / aspect_ratio).round() as u32).max(1);
        img.resize(800, target_height, image::imageops::FilterType::Lanczos3)
    } else {
        img.clone()
    };
    let feed_bytes = encode_jpeg(&feed_img, 90)?;

    let display_img = if width > 1600 {
        let target_height = ((1600.0 / aspect_ratio).round() as u32).max(1);
        img.resize(1600, target_height, image::imageops::FilterType::Lanczos3)
    } else {
        img.clone()
    };
    let display_bytes = encode_jpeg(&display_img, 92)?;

    let base_key = original_key
        .rsplit_once('/')
        .map(|(base, _)| base)
        .unwrap_or("media");

    let thumb_key = format!("{}/thumbnail.jpg", base_key);
    let feed_key = format!("{}/feed.jpg", base_key);
    let display_key = format!("{}/display.jpg", base_key);

    storage
        .upload_object(&thumb_key, &thumb_bytes, "image/jpeg")
        .await
        .map_err(|e| format!("upload thumbnail failed: {}", e))?;
    storage
        .upload_object(&feed_key, &feed_bytes, "image/jpeg")
        .await
        .map_err(|e| format!("upload feed image failed: {}", e))?;
    storage
        .upload_object(&display_key, &display_bytes, "image/jpeg")
        .await
        .map_err(|e| format!("upload display image failed: {}", e))?;

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

fn read_exif_orientation(data: &[u8]) -> image::metadata::Orientation {
    let reader = ExifReader::new();
    match reader.read_from_container(&mut std::io::Cursor::new(data)) {
        Ok(exif) => match exif.get_field(Tag::Orientation, In::PRIMARY) {
            Some(field) => {
                let val = field.value.get_uint(0).unwrap_or(1) as u8;
                image::metadata::Orientation::from_exif(val)
                    .unwrap_or(image::metadata::Orientation::NoTransforms)
            }
            None => image::metadata::Orientation::NoTransforms,
        },
        Err(_) => image::metadata::Orientation::NoTransforms,
    }
}

fn encode_jpeg(image: &DynamicImage, quality: u8) -> Result<Vec<u8>, image::ImageError> {
    let mut output = Vec::new();
    let mut encoder = JpegEncoder::new_with_quality(&mut output, quality);
    encoder.encode_image(image)?;
    Ok(output)
}

async fn process_video(pool: &PgPool, media_uuid: Uuid, video_data: &[u8]) -> JobResult {
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

/// Notification job handler: creates a notification record from the job payload.
pub async fn handle_notification(pool: &PgPool, payload: &serde_json::Value) -> JobResult {
    let recipient_id = payload["recipient_id"]
        .as_str()
        .ok_or("missing recipient_id")?;
    let actor_id = payload["actor_id"].as_str().ok_or("missing actor_id")?;
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
