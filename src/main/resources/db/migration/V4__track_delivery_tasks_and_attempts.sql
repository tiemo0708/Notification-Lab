CREATE TABLE delivery_task (
    task_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    notification_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target_id VARBINARY(400) NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    CONSTRAINT uk_delivery_notification UNIQUE (notification_id),
    CONSTRAINT fk_delivery_notification FOREIGN KEY (notification_id) REFERENCES notification(notification_id),
    CONSTRAINT chk_delivery_attempts CHECK (max_attempts BETWEEN 1 AND 10
        AND attempt_count BETWEEN 0 AND max_attempts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE delivery_attempt (
    task_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    attempt_no INT NOT NULL,
    lease_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    error_code VARCHAR(64),
    started_at DATETIME(6),
    completed_at DATETIME(6),
    PRIMARY KEY (task_id, attempt_no),
    CONSTRAINT uk_delivery_attempt_token UNIQUE (lease_token),
    CONSTRAINT fk_attempt_task FOREIGN KEY (task_id) REFERENCES delivery_task(task_id),
    CONSTRAINT chk_attempt_number CHECK (attempt_no BETWEEN 1 AND 10),
    CONSTRAINT chk_attempt_outcome CHECK (outcome IN ('STARTED', 'UNKNOWN', 'PROVIDER_ACCEPTED',
        'RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'INTERNAL_FAILURE')),
    CONSTRAINT chk_attempt_completion CHECK ((outcome = 'STARTED' AND completed_at IS NULL)
        OR (outcome <> 'STARTED' AND completed_at IS NOT NULL)),
    CONSTRAINT chk_attempt_error CHECK ((outcome IN ('STARTED', 'PROVIDER_ACCEPTED') AND error_code IS NULL)
        OR (outcome NOT IN ('STARTED', 'PROVIDER_ACCEPTED') AND error_code IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Only asynchronous notifications have an outbox. Never manufacture work for S1/S2 rows.
INSERT INTO delivery_task (task_id, notification_id, target_id, attempt_count, max_attempts, created_at)
SELECT UUID(), n.notification_id, CONVERT(n.recipient_id USING binary),
    IF(n.status = 'PROCESSING' AND n.worker_token IS NOT NULL, 1, 0), 3, UTC_TIMESTAMP(6)
FROM notification n JOIN outbox_event e ON e.notification_id = n.notification_id
WHERE e.event_type = 'NOTIFICATION_REQUESTED';

-- Preserve active ownership. The actual old start time is unknown, so do not invent it.
INSERT INTO delivery_attempt (task_id, attempt_no, lease_token, outcome, started_at)
SELECT t.task_id, 1, n.worker_token, 'STARTED', NULL
FROM delivery_task t JOIN notification n ON n.notification_id = t.notification_id
WHERE t.attempt_count = 1;
