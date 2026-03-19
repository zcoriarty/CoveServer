-- Add order_index to media_items for carousel post ordering
ALTER TABLE media_items ADD COLUMN order_index INT NOT NULL DEFAULT 0;
