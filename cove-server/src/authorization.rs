//! Shared authorization helpers for post visibility and user summaries.

use cove_common::error::{CoveError, CoveResult};
use cove_common::id::{PostId, UserId};
use cove_proto::cove::common::UserSummary;
use sqlx::PgPool;

/// Returns true if the viewer can see the post: viewer is author, or post is
/// visible to followers and viewer has accepted follow, and post is not deleted.
pub async fn can_view_post(
    pool: &PgPool,
    viewer_id: &UserId,
    post_id: &PostId,
) -> CoveResult<bool> {
    let row = sqlx::query(
        r#"
        SELECT p.author_id, p.visibility, p.is_deleted
        FROM posts p
        WHERE p.id = $1
        "#,
    )
    .bind(post_id.as_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|e| CoveError::Database(e.to_string()))?;

    let row = match row {
        Some(r) => r,
        None => return Ok(false),
    };

    let author_id: uuid::Uuid = row.get(0);
    let visibility: String = row.get(1);
    let is_deleted: bool = row.get(2);

    if is_deleted {
        return Ok(false);
    }

    if author_id == *viewer_id.as_uuid() {
        return Ok(true);
    }

    if visibility == "private" {
        return Ok(false);
    }

    if visibility == "followers" {
        let followed: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM follows
                WHERE follower_id = $1 AND followee_id = $2 AND state = 'accepted'
            )
            "#,
        )
        .bind(viewer_id.as_uuid())
        .bind(author_id)
        .fetch_one(pool)
        .await
        .map_err(|e| CoveError::Database(e.to_string()))?;

        return Ok(followed);
    }

    Ok(false)
}

/// Build a UserSummary proto from user and profile data.
pub fn build_user_summary(
    user_id: &UserId,
    username: String,
    display_name: String,
    avatar_url: String,
    is_following: bool,
) -> UserSummary {
    UserSummary {
        user_id: user_id.to_string(),
        username,
        display_name,
        avatar_url,
        is_following,
    }
}
