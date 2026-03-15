-- Add optional location metadata to posts
ALTER TABLE posts ADD COLUMN location_lat DOUBLE PRECISION;
ALTER TABLE posts ADD COLUMN location_lng DOUBLE PRECISION;
ALTER TABLE posts ADD COLUMN location_name TEXT;
