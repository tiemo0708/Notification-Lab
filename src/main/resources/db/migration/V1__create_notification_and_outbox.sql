CREATE TABLE notification (
    notification_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    recipient_id VARCHAR(100) NOT NULL,
    title VARCHAR(100) NOT NULL,
    body VARCHAR(500) NOT NULL,
    status VARCHAR(32) NOT NULL,
    provider_message_id VARCHAR(255),
    error_code VARCHAR(64),
    created_at DATETIME(6) NOT NULL,
    completed_at DATETIME(6),
    PRIMARY KEY (notification_id),
    CONSTRAINT chk_notification_status CHECK (status IN (
        'PROCESSING', 'PROVIDER_ACCEPTED', 'RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'INTERNAL_FAILURE'
    )),
    CONSTRAINT chk_notification_completion CHECK (
        (status = 'PROCESSING' AND completed_at IS NULL)
        OR (status <> 'PROCESSING' AND completed_at IS NOT NULL)
    ),
    CONSTRAINT chk_notification_result CHECK (
        (status = 'PROCESSING' AND provider_message_id IS NULL AND error_code IS NULL)
        OR (status = 'PROVIDER_ACCEPTED' AND provider_message_id IS NOT NULL AND error_code IS NULL)
        OR (status IN ('RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'INTERNAL_FAILURE')
            AND provider_message_id IS NULL AND error_code IS NOT NULL)
    ),
    INDEX idx_notification_status_created (status, created_at, notification_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- S2 prepares the schema only. No application writes or publishes outbox events yet.
CREATE TABLE outbox_event (
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    notification_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    created_at DATETIME(6) NOT NULL,
    available_at DATETIME(6) NOT NULL,
    published_at DATETIME(6),
    PRIMARY KEY (event_id),
    CONSTRAINT uk_outbox_notification_type UNIQUE (notification_id, event_type),
    CONSTRAINT fk_outbox_notification FOREIGN KEY (notification_id)
        REFERENCES notification (notification_id),
    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED')),
    CONSTRAINT chk_outbox_attempts CHECK (attempt_count >= 0),
    CONSTRAINT chk_outbox_published CHECK (
        (status = 'PENDING' AND published_at IS NULL)
        OR (status = 'PUBLISHED' AND published_at IS NOT NULL)
    ),
    INDEX idx_outbox_status_available (status, available_at, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
