-- Backfill home feed entries for already-accepted follows.
-- This repairs users who followed accounts before follow-time fanout existed.
INSERT INTO feed_entries (id, user_id, post_id, created_at)
SELECT gen_random_uuid(), f.follower_id, p.id, p.created_at
FROM follows f
JOIN posts p ON p.author_id = f.followee_id
WHERE f.state = 'accepted'
  AND p.visibility = 'followers'
  AND NOT p.is_deleted
ON CONFLICT (user_id, post_id) DO NOTHING;
