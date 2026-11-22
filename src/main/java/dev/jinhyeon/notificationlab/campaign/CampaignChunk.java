package dev.jinhyeon.notificationlab.campaign;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public record CampaignChunk(UUID campaignId, long firstOrdinal, int targetCount) {
    public static final String EVENT_TYPE = "CAMPAIGN_CHUNK_REQUESTED";
    public static final int SIZE = 100;

    public CampaignChunk {
        if (campaignId == null || firstOrdinal < 1 || firstOrdinal > 1_000_000
                || (firstOrdinal - 1) % SIZE != 0 || targetCount < 1 || targetCount > SIZE
                || firstOrdinal + targetCount - 1 > 1_000_000) {
            throw new IllegalArgumentException("Invalid campaign chunk");
        }
    }

    public UUID eventId() {
        String key = "notification-lab:campaign:chunk:v1:" + campaignId + ":" + firstOrdinal;
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
    }
}
