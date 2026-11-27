package dev.jinhyeon.notificationlab.campaign;

import java.time.Instant;
import java.util.UUID;

public record Campaign(UUID campaignId, String name, String title, String body,
                       String audience, int targetCount, Status status,
                       Instant createdAt, Instant startRequestedAt, Instant scheduledAt, Instant preparedAt) {
    public Campaign(UUID campaignId, String name, String title, String body, String audience, int targetCount,
                    Status status, Instant createdAt, Instant startRequestedAt) {
        this(campaignId, name, title, body, audience, targetCount, status, createdAt, startRequestedAt, null, null);
    }

    public enum Status { DRAFT, START_REQUESTED, PREPARATION_REQUESTED }
}
