package dev.jinhyeon.notificationlab.campaign;

import java.time.Instant;
import java.util.UUID;

public record Campaign(UUID campaignId, String name, String title, String body,
                       String audience, int targetCount, Status status,
                       Instant createdAt, Instant startRequestedAt) {
    public enum Status { DRAFT, START_REQUESTED }
}
