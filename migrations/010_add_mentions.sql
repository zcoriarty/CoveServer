-- Mention tracking for captions and comments.

CREATE TABLE post_mentions (
    post_id              UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    mentioned_user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    mentioned_by_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, mentioned_user_id)
);

CREATE INDEX idx_post_mentions_user_timeline
    ON post_mentions(mentioned_user_id, created_at DESC);

CREATE TABLE comment_mentions (
    comment_id           UUID NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
    post_id              UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    mentioned_user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    mentioned_by_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (comment_id, mentioned_user_id)
);

CREATE INDEX idx_comment_mentions_user_timeline
    ON comment_mentions(mentioned_user_id, created_at DESC);

CREATE INDEX idx_comment_mentions_post_timeline
    ON comment_mentions(post_id, created_at DESC);
