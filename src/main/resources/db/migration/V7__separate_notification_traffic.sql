-- Existing rows and legacy events stay REALTIME. Stop old binaries before enabling campaign roles.
ALTER TABLE notification
    ADD COLUMN traffic_class VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'REALTIME',
    ADD CONSTRAINT chk_notification_traffic CHECK (traffic_class IN ('REALTIME', 'CAMPAIGN')),
    ADD CONSTRAINT uk_notification_traffic UNIQUE (notification_id, traffic_class);

ALTER TABLE outbox_event
    ADD COLUMN traffic_class VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'REALTIME',
    ADD CONSTRAINT chk_outbox_traffic CHECK (traffic_class IN ('REALTIME', 'CAMPAIGN')),
    ADD CONSTRAINT fk_outbox_traffic FOREIGN KEY (notification_id, traffic_class)
        REFERENCES notification (notification_id, traffic_class),
    ADD INDEX idx_outbox_traffic_status_available (traffic_class, status, available_at, event_id);
