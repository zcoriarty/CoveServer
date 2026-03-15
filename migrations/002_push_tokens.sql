-- Push tokens for APNs delivery

CREATE TABLE IF NOT EXISTS push_tokens (
    token           TEXT PRIMARY KEY,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id      UUID REFERENCES sessions(id) ON DELETE CASCADE,
    platform        TEXT NOT NULL CHECK (platform IN ('ios')),
    environment     TEXT NOT NULL CHECK (environment IN ('development', 'production')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_push_tokens_user_active
    ON push_tokens(user_id)
    WHERE revoked_at IS NULL;
