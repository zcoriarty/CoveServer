-- Video metadata placeholders were previously stamped as 1920x1080 (1.78),
-- which forces landscape layout in feed/detail for portrait clips.
-- Normalize existing placeholder rows to a neutral square ratio until full
-- video probing is implemented in the worker.

UPDATE media_items
SET width = 1080,
    height = 1080,
    aspect_ratio = 1.0
WHERE media_type = 'video'
  AND width = 1920
  AND height = 1080
  AND aspect_ratio BETWEEN 1.77 AND 1.79;
