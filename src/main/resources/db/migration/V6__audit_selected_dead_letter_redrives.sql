CREATE TABLE dead_letter_redrive (
    source_event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    source_notification_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    root_notification_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target_notification_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    redrive_no INT NOT NULL,
    reason VARCHAR(32) NOT NULL,
    acknowledged_possible_duplicate BOOLEAN NOT NULL,
    created_at DATETIME(6) NOT NULL,
    CONSTRAINT uk_redrive_source UNIQUE (source_notification_id),
    CONSTRAINT uk_redrive_target UNIQUE (target_notification_id),
    CONSTRAINT uk_redrive_root_number UNIQUE (root_notification_id, redrive_no),
    CONSTRAINT fk_redrive_event FOREIGN KEY (source_event_id) REFERENCES outbox_event(event_id),
    CONSTRAINT fk_redrive_source FOREIGN KEY (source_notification_id) REFERENCES notification(notification_id),
    CONSTRAINT fk_redrive_root FOREIGN KEY (root_notification_id) REFERENCES notification(notification_id),
    CONSTRAINT fk_redrive_target FOREIGN KEY (target_notification_id) REFERENCES notification(notification_id),
    CONSTRAINT chk_redrive_number CHECK (redrive_no BETWEEN 1 AND 3),
    CONSTRAINT chk_redrive_reason CHECK (reason IN ('PROVIDER_RECOVERED', 'CONFIGURATION_FIXED', 'MANUAL_REVIEW')),
    CONSTRAINT chk_redrive_acknowledged CHECK (acknowledged_possible_duplicate = TRUE),
    CONSTRAINT chk_redrive_distinct CHECK (source_notification_id <> target_notification_id
        AND root_notification_id <> target_notification_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
