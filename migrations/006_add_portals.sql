-- Add profile portals (highlight-style collections) and post membership

CREATE TABLE portals (
    id              UUID PRIMARY KEY,
    owner_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_portals_owner_name ON portals(owner_id, lower(name));
CREATE INDEX idx_portals_owner_timeline ON portals(owner_id, updated_at DESC, id DESC);

CREATE TABLE portal_posts (
    portal_id       UUID NOT NULL REFERENCES portals(id) ON DELETE CASCADE,
    post_id         UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (portal_id, post_id)
);

CREATE INDEX idx_portal_posts_post ON portal_posts(post_id);
CREATE INDEX idx_portal_posts_portal_timeline ON portal_posts(portal_id, added_at DESC, post_id DESC);
