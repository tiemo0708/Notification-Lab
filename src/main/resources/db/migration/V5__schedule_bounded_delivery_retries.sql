ALTER TABLE notification
    DROP CHECK chk_notification_status,
    DROP CHECK chk_notification_completion,
    DROP CHECK chk_notification_result,
    ADD CONSTRAINT chk_notification_status CHECK (status IN (
        'QUEUED', 'PROCESSING', 'WAITING_RETRY', 'PROVIDER_ACCEPTED',
        'RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'INTERNAL_FAILURE')),
    ADD CONSTRAINT chk_notification_completion CHECK (
        (status IN ('QUEUED', 'PROCESSING', 'WAITING_RETRY') AND completed_at IS NULL)
        OR (status NOT IN ('QUEUED', 'PROCESSING', 'WAITING_RETRY') AND completed_at IS NOT NULL)),
    ADD CONSTRAINT chk_notification_result CHECK (
        (status IN ('QUEUED', 'PROCESSING') AND provider_message_id IS NULL AND error_code IS NULL)
        OR (status = 'WAITING_RETRY' AND provider_message_id IS NULL AND error_code IS NOT NULL)
        OR (status = 'PROVIDER_ACCEPTED' AND provider_message_id IS NOT NULL AND error_code IS NULL)
        OR (status IN ('RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'INTERNAL_FAILURE')
            AND provider_message_id IS NULL AND error_code IS NOT NULL));

ALTER TABLE delivery_task
    ADD COLUMN retry_generation INT NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at DATETIME(6),
    ADD CONSTRAINT chk_delivery_generation CHECK (retry_generation BETWEEN 0 AND 9
        AND retry_generation <= attempt_count AND retry_generation < max_attempts);

-- Existing events stay generation zero. Do not reopen historical terminal failures.
ALTER TABLE outbox_event
    DROP INDEX uk_outbox_notification_type,
    ADD COLUMN retry_generation INT NOT NULL DEFAULT 0,
    ADD CONSTRAINT uk_outbox_notification_type UNIQUE (notification_id, event_type, retry_generation),
    ADD CONSTRAINT chk_outbox_generation CHECK (
        (event_type = 'NOTIFICATION_RETRY' AND retry_generation BETWEEN 1 AND 9)
        OR (event_type <> 'NOTIFICATION_RETRY' AND retry_generation = 0));
