use cove_common::id::UserId;
use sqlx::PgPool;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotificationPreferences {
    pub likes_enabled: bool,
    pub comments_enabled: bool,
    pub mentions_enabled: bool,
    pub shares_enabled: bool,
    pub follow_requests_enabled: bool,
    pub follow_activity_enabled: bool,
    pub new_posts_enabled: bool,
}

impl Default for NotificationPreferences {
    fn default() -> Self {
        Self {
            likes_enabled: true,
            comments_enabled: true,
            mentions_enabled: true,
            shares_enabled: true,
            follow_requests_enabled: true,
            follow_activity_enabled: true,
            new_posts_enabled: true,
        }
    }
}

impl NotificationPreferences {
    pub fn allows_notification_type(&self, notification_type: &str) -> bool {
        match notification_type {
            "like" => self.likes_enabled,
            "comment" => self.comments_enabled,
            "mention" => self.mentions_enabled,
            "share" => self.shares_enabled,
            "follow_request" => self.follow_requests_enabled,
            "follow_accepted" | "new_follower" => self.follow_activity_enabled,
            "new_post" => self.new_posts_enabled,
            _ => true,
        }
    }
}

type PreferenceRow = (bool, bool, bool, bool, bool, bool, bool);

pub async fn load(
    pool: &PgPool,
    user_id: UserId,
) -> Result<NotificationPreferences, sqlx::Error> {
    let row: Option<PreferenceRow> = sqlx::query_as(
        r#"
        SELECT
            likes_enabled,
            comments_enabled,
            mentions_enabled,
            shares_enabled,
            follow_requests_enabled,
            follow_activity_enabled,
            new_posts_enabled
        FROM notification_preferences
        WHERE user_id = $1
        "#,
    )
    .bind(user_id.as_uuid())
    .fetch_optional(pool)
    .await?;

    Ok(row
        .map(
            |(
                likes_enabled,
                comments_enabled,
                mentions_enabled,
                shares_enabled,
                follow_requests_enabled,
                follow_activity_enabled,
                new_posts_enabled,
            )| NotificationPreferences {
                likes_enabled,
                comments_enabled,
                mentions_enabled,
                shares_enabled,
                follow_requests_enabled,
                follow_activity_enabled,
                new_posts_enabled,
            },
        )
        .unwrap_or_default())
}

pub async fn update(
    pool: &PgPool,
    user_id: UserId,
    preferences: NotificationPreferences,
) -> Result<NotificationPreferences, sqlx::Error> {
    let row: PreferenceRow = sqlx::query_as(
        r#"
        INSERT INTO notification_preferences (
            user_id,
            likes_enabled,
            comments_enabled,
            mentions_enabled,
            shares_enabled,
            follow_requests_enabled,
            follow_activity_enabled,
            new_posts_enabled,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET
            likes_enabled = EXCLUDED.likes_enabled,
            comments_enabled = EXCLUDED.comments_enabled,
            mentions_enabled = EXCLUDED.mentions_enabled,
            shares_enabled = EXCLUDED.shares_enabled,
            follow_requests_enabled = EXCLUDED.follow_requests_enabled,
            follow_activity_enabled = EXCLUDED.follow_activity_enabled,
            new_posts_enabled = EXCLUDED.new_posts_enabled,
            updated_at = NOW()
        RETURNING
            likes_enabled,
            comments_enabled,
            mentions_enabled,
            shares_enabled,
            follow_requests_enabled,
            follow_activity_enabled,
            new_posts_enabled
        "#,
    )
    .bind(user_id.as_uuid())
    .bind(preferences.likes_enabled)
    .bind(preferences.comments_enabled)
    .bind(preferences.mentions_enabled)
    .bind(preferences.shares_enabled)
    .bind(preferences.follow_requests_enabled)
    .bind(preferences.follow_activity_enabled)
    .bind(preferences.new_posts_enabled)
    .fetch_one(pool)
    .await?;

    Ok(NotificationPreferences {
        likes_enabled: row.0,
        comments_enabled: row.1,
        mentions_enabled: row.2,
        shares_enabled: row.3,
        follow_requests_enabled: row.4,
        follow_activity_enabled: row.5,
        new_posts_enabled: row.6,
    })
}

pub async fn is_enabled_for_notification_type(
    pool: &PgPool,
    user_id: UserId,
    notification_type: &str,
) -> Result<bool, sqlx::Error> {
    let preferences = load(pool, user_id).await?;
    Ok(preferences.allows_notification_type(notification_type))
}
