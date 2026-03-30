ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_notification_type_check;

ALTER TABLE notifications
    ADD CONSTRAINT notifications_notification_type_check
    CHECK (
        notification_type IN (
            'follow_request',
            'follow_accepted',
            'new_follower',
            'like',
            'comment',
            'mention',
            'share',
            'new_post'
        )
    );
