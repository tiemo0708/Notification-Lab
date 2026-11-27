ALTER TABLE campaign
    ADD COLUMN scheduled_at DATETIME(6),
    ADD COLUMN prepared_at DATETIME(6),
    DROP CHECK chk_campaign_status,
    DROP CHECK chk_campaign_start,
    ADD CONSTRAINT chk_campaign_status CHECK (
        status = 'DRAFT' OR (status = 'START_REQUESTED' AND scheduled_at IS NULL)
        OR (status = 'PREPARATION_REQUESTED' AND scheduled_at IS NOT NULL)),
    ADD CONSTRAINT chk_campaign_start CHECK (
        (status = 'DRAFT' AND start_requested_at IS NULL)
        OR (status <> 'DRAFT' AND start_requested_at IS NOT NULL AND start_requested_at >= created_at)),
    ADD CONSTRAINT chk_campaign_prepared CHECK (
        prepared_at IS NULL OR (status = 'PREPARATION_REQUESTED' AND prepared_at >= start_requested_at));

ALTER TABLE outbox_event
    ADD COLUMN scheduled_campaign_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
    ADD INDEX idx_outbox_scheduled_campaign (scheduled_campaign_id),
    ADD CONSTRAINT fk_outbox_scheduled_campaign FOREIGN KEY (scheduled_campaign_id) REFERENCES campaign(campaign_id),
    DROP CHECK chk_outbox_status,
    ADD CONSTRAINT chk_outbox_status CHECK (status IN ('HELD', 'PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED')),
    ADD CONSTRAINT chk_outbox_held CHECK (status <> 'HELD' OR
        (scheduled_campaign_id IS NOT NULL AND event_type = 'CAMPAIGN_CHUNK_REQUESTED'));
