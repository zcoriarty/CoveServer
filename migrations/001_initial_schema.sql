-- CoveServer Initial Schema
-- All tables use UUID primary keys (v7 for time-ordering)
-- Timestamps are stored as TIMESTAMPTZ

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ============================================================
-- Users & Auth
-- ============================================================

CREATE TABLE users (
    id              UUID PRIMARY KEY,
    username        TEXT NOT NULL UNIQUE,
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    display_name    TEXT NOT NULL DEFAULT '',
    is_admin        BOOLEAN NOT NULL DEFAULT FALSE,
    account_state   TEXT NOT NULL DEFAULT 'active'
                    CHECK (account_state IN ('active', 'deactivated', 'suspended')),
    invited_by      UUID REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_username_trgm ON users USING gin (username gin_trgm_ops);
CREATE INDEX idx_users_display_name_trgm ON users USING gin (display_name gin_trgm_ops);

CREATE TABLE sessions (
    id              UUID PRIMARY KEY,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    refresh_token_hash TEXT NOT NULL,
    device_id       TEXT NOT NULL DEFAULT '',
    device_name     TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at      TIMESTAMPTZ
);

CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_refresh_hash ON sessions(refresh_token_hash);

CREATE TABLE invites (
    id              UUID PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    created_by      UUID NOT NULL REFERENCES users(id),
    max_uses        INT NOT NULL DEFAULT 1,
    use_count       INT NOT NULL DEFAULT 0,
    expires_at      TIMESTAMPTZ,
    revoked         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_invites_code ON invites(code);

-- ============================================================
-- Profiles
-- ============================================================

CREATE TABLE profiles (
    user_id         UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    bio             TEXT NOT NULL DEFAULT '',
    avatar_media_id UUID,
    is_private      BOOLEAN NOT NULL DEFAULT TRUE,
    follower_count  INT NOT NULL DEFAULT 0,
    following_count INT NOT NULL DEFAULT 0,
    post_count      INT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Social Graph
-- ============================================================

CREATE TABLE follows (
    follower_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    followee_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'accepted', 'blocked')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    accepted_at     TIMESTAMPTZ,
    PRIMARY KEY (follower_id, followee_id)
);

CREATE INDEX idx_follows_followee ON follows(followee_id, state);
CREATE INDEX idx_follows_follower ON follows(follower_id, state);

-- ============================================================
-- Posts & Media
-- ============================================================

CREATE TABLE posts (
    id              UUID PRIMARY KEY,
    author_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    caption         TEXT NOT NULL DEFAULT '',
    visibility      TEXT NOT NULL DEFAULT 'followers'
                    CHECK (visibility IN ('followers', 'private')),
    post_type       TEXT NOT NULL DEFAULT 'photo'
                    CHECK (post_type IN ('photo', 'video', 'carousel')),
    like_count      INT NOT NULL DEFAULT 0,
    comment_count   INT NOT NULL DEFAULT 0,
    share_count     INT NOT NULL DEFAULT 0,
    is_deleted      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    edited_at       TIMESTAMPTZ
);

CREATE INDEX idx_posts_author ON posts(author_id, created_at DESC) WHERE NOT is_deleted;
CREATE INDEX idx_posts_created ON posts(created_at DESC) WHERE NOT is_deleted;

CREATE TABLE media_items (
    id                  UUID PRIMARY KEY,
    post_id             UUID REFERENCES posts(id) ON DELETE SET NULL,
    owner_id            UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    media_type          TEXT NOT NULL CHECK (media_type IN ('photo', 'video')),
    original_key        TEXT NOT NULL,
    thumbnail_key       TEXT,
    feed_key            TEXT,
    display_key         TEXT,
    width               INT NOT NULL DEFAULT 0,
    height              INT NOT NULL DEFAULT 0,
    aspect_ratio        DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    duration_seconds    INT NOT NULL DEFAULT 0,
    file_size_bytes     BIGINT NOT NULL DEFAULT 0,
    content_type        TEXT NOT NULL DEFAULT '',
    checksum            TEXT NOT NULL DEFAULT '',
    processing_state    TEXT NOT NULL DEFAULT 'pending'
                        CHECK (processing_state IN ('pending', 'processing', 'completed', 'failed')),
    encrypted_dek       BYTEA,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_media_post ON media_items(post_id);
CREATE INDEX idx_media_owner ON media_items(owner_id);
CREATE INDEX idx_media_processing ON media_items(processing_state) WHERE processing_state != 'completed';

-- ============================================================
-- Feed
-- ============================================================

CREATE TABLE feed_entries (
    id              UUID PRIMARY KEY,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    post_id         UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_feed_user_post ON feed_entries(user_id, post_id);
CREATE INDEX idx_feed_user_timeline ON feed_entries(user_id, created_at DESC);

-- ============================================================
-- Comments
-- ============================================================

CREATE TABLE comments (
    id              UUID PRIMARY KEY,
    post_id         UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_id       UUID REFERENCES comments(id) ON DELETE CASCADE,
    body            TEXT NOT NULL,
    reply_count     INT NOT NULL DEFAULT 0,
    is_deleted      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_comments_post ON comments(post_id, created_at) WHERE NOT is_deleted;
CREATE INDEX idx_comments_parent ON comments(parent_id) WHERE parent_id IS NOT NULL;

-- ============================================================
-- Likes
-- ============================================================

CREATE TABLE likes (
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    post_id         UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, post_id)
);

CREATE INDEX idx_likes_post ON likes(post_id);

-- ============================================================
-- Shares
-- ============================================================

CREATE TABLE shares (
    id              UUID PRIMARY KEY,
    sender_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    recipient_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    post_id         UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_shares_recipient ON shares(recipient_id, created_at DESC);
CREATE INDEX idx_shares_sender ON shares(sender_id, created_at DESC);

-- ============================================================
-- Notifications
-- ============================================================

CREATE TABLE notifications (
    id              UUID PRIMARY KEY,
    recipient_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    actor_id        UUID REFERENCES users(id) ON DELETE SET NULL,
    notification_type TEXT NOT NULL
                    CHECK (notification_type IN (
                        'follow_request', 'follow_accepted', 'new_follower',
                        'like', 'comment', 'share', 'new_post'
                    )),
    target_type     TEXT NOT NULL DEFAULT '',
    target_id       UUID,
    message         TEXT NOT NULL DEFAULT '',
    is_read         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notifications_recipient ON notifications(recipient_id, created_at DESC);
CREATE INDEX idx_notifications_unread ON notifications(recipient_id, is_read) WHERE NOT is_read;

-- ============================================================
-- Audit Log
-- ============================================================

CREATE TABLE audit_log (
    id              UUID PRIMARY KEY,
    actor_id        UUID NOT NULL REFERENCES users(id),
    action          TEXT NOT NULL,
    target_type     TEXT NOT NULL DEFAULT '',
    target_id       TEXT NOT NULL DEFAULT '',
    details         JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_actor ON audit_log(actor_id, created_at DESC);
CREATE INDEX idx_audit_action ON audit_log(action, created_at DESC);

-- ============================================================
-- Background Jobs
-- ============================================================

CREATE TABLE jobs (
    id              UUID PRIMARY KEY,
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'running', 'completed', 'failed', 'dead')),
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    last_error      TEXT,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_pending ON jobs(run_at) WHERE state = 'pending';
CREATE INDEX idx_jobs_type ON jobs(job_type, state);
