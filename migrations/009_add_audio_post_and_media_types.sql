-- Allow audio posts/media alongside existing photo/video types.

ALTER TABLE posts
    DROP CONSTRAINT IF EXISTS posts_post_type_check;

ALTER TABLE posts
    ADD CONSTRAINT posts_post_type_check
    CHECK (post_type IN ('photo', 'video', 'audio', 'carousel'));

ALTER TABLE media_items
    DROP CONSTRAINT IF EXISTS media_items_media_type_check;

ALTER TABLE media_items
    ADD CONSTRAINT media_items_media_type_check
    CHECK (media_type IN ('photo', 'video', 'audio'));
