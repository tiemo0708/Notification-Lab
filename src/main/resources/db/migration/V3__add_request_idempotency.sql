-- NULL keeps keyless requests independent. Binary equality makes keys case-sensitive.
ALTER TABLE notification
    ADD COLUMN idempotency_key VARBINARY(128),
    ADD CONSTRAINT uk_notification_idempotency_key UNIQUE (idempotency_key);
