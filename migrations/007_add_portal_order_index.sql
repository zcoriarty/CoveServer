-- Add stable manual ordering for profile portals.

ALTER TABLE portals
ADD COLUMN IF NOT EXISTS order_index INTEGER;

WITH ranked AS (
    SELECT
        id,
        (ROW_NUMBER() OVER (
            PARTITION BY owner_id
            ORDER BY updated_at DESC, id DESC
        ) - 1)::INTEGER AS position
    FROM portals
)
UPDATE portals po
SET order_index = ranked.position
FROM ranked
WHERE po.id = ranked.id
  AND po.order_index IS NULL;

UPDATE portals
SET order_index = 0
WHERE order_index IS NULL;

ALTER TABLE portals
ALTER COLUMN order_index SET NOT NULL;

ALTER TABLE portals
ALTER COLUMN order_index SET DEFAULT 0;

DROP INDEX IF EXISTS idx_portals_owner_timeline;

CREATE INDEX idx_portals_owner_order
ON portals(owner_id, order_index ASC, updated_at DESC, id DESC);
