ALTER TABLE comments
    ADD COLUMN IF NOT EXISTS like_count INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS comment_likes (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    comment_id UUID NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, comment_id)
);

CREATE INDEX IF NOT EXISTS idx_comment_likes_comment
    ON comment_likes(comment_id);
