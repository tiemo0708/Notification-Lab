ALTER TABLE outbox_event
    DROP FOREIGN KEY fk_outbox_notification,
    DROP FOREIGN KEY fk_outbox_traffic;

ALTER TABLE outbox_event
    MODIFY COLUMN notification_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD CONSTRAINT fk_outbox_notification FOREIGN KEY (notification_id)
        REFERENCES notification(notification_id),
    ADD CONSTRAINT fk_outbox_traffic FOREIGN KEY (notification_id, traffic_class)
        REFERENCES notification(notification_id, traffic_class),
    ADD CONSTRAINT chk_outbox_event_owner CHECK (
        (event_type = 'CAMPAIGN_CHUNK_REQUESTED' AND traffic_class = 'CAMPAIGN' AND notification_id IS NULL)
        OR (event_type <> 'CAMPAIGN_CHUNK_REQUESTED' AND notification_id IS NOT NULL)
    );

ALTER TABLE campaign_target ADD INDEX idx_campaign_target_event (event_id);
ALTER TABLE campaign_target DROP INDEX uk_campaign_target_event;
