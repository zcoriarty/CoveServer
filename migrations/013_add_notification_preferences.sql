CREATE TABLE IF NOT EXISTS notification_preferences (
    user_id                  UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    likes_enabled            BOOLEAN NOT NULL DEFAULT TRUE,
    comments_enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    mentions_enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    shares_enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    follow_requests_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
    follow_activity_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
    new_posts_enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
