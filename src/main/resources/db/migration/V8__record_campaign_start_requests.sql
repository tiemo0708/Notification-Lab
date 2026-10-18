-- Control-plane metadata only. START_REQUESTED is durable intent, not an executing Batch job.
CREATE TABLE campaign (
    campaign_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    idempotency_key VARBINARY(128) NOT NULL,
    name VARCHAR(100) NOT NULL,
    title VARCHAR(100) NOT NULL,
    body VARCHAR(500) NOT NULL,
    audience VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target_count INT NOT NULL,
    status VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    start_requested_at DATETIME(6),
    CONSTRAINT uk_campaign_idempotency UNIQUE (idempotency_key),
    CONSTRAINT chk_campaign_key CHECK (OCTET_LENGTH(idempotency_key) BETWEEN 1 AND 128),
    CONSTRAINT chk_campaign_audience CHECK (audience = 'SYNTHETIC_V1'),
    CONSTRAINT chk_campaign_target_count CHECK (target_count BETWEEN 1 AND 1000000),
    CONSTRAINT chk_campaign_status CHECK (status IN ('DRAFT', 'START_REQUESTED')),
    CONSTRAINT chk_campaign_start CHECK (
        (status = 'DRAFT' AND start_requested_at IS NULL)
        OR (status = 'START_REQUESTED' AND start_requested_at IS NOT NULL AND start_requested_at >= created_at)
    ),
    INDEX idx_campaign_status_requested (status, start_requested_at, campaign_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
