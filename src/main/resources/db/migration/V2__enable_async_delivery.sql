ALTER TABLE notification
    DROP CHECK chk_notification_status,
    DROP CHECK chk_notification_completion,
    DROP CHECK chk_notification_result,
    ADD COLUMN worker_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    ADD COLUMN worker_lease_until DATETIME(6),
    ADD CONSTRAINT chk_notification_status CHECK (status IN (
        'QUEUED', 'PROCESSING', 'PROVIDER_ACCEPTED', 'RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'INTERNAL_FAILURE')),
    ADD CONSTRAINT chk_notification_completion CHECK (
        (status IN ('QUEUED', 'PROCESSING') AND completed_at IS NULL)
        OR (status NOT IN ('QUEUED', 'PROCESSING') AND completed_at IS NOT NULL)),
    ADD CONSTRAINT chk_notification_result CHECK (
        (status IN ('QUEUED', 'PROCESSING') AND provider_message_id IS NULL AND error_code IS NULL)
        OR (status = 'PROVIDER_ACCEPTED' AND provider_message_id IS NOT NULL AND error_code IS NULL)
        OR (status IN ('RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'INTERNAL_FAILURE')
            AND provider_message_id IS NULL AND error_code IS NOT NULL)),
    ADD CONSTRAINT chk_notification_worker CHECK (
        (worker_token IS NULL AND worker_lease_until IS NULL)
        OR (status = 'PROCESSING' AND worker_token IS NOT NULL AND worker_lease_until IS NOT NULL));

ALTER TABLE outbox_event
    DROP CHECK chk_outbox_status,
    DROP CHECK chk_outbox_published,
    ADD COLUMN lease_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    ADD CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED')),
    ADD CONSTRAINT chk_outbox_published CHECK (
        (status <> 'PUBLISHED' AND published_at IS NULL)
        OR (status = 'PUBLISHED' AND published_at IS NOT NULL)),
    ADD CONSTRAINT chk_outbox_lease CHECK (
        (status = 'PROCESSING' AND lease_token IS NOT NULL)
        OR (status <> 'PROCESSING' AND lease_token IS NULL));
