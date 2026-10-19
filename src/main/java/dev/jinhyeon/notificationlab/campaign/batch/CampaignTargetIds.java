package dev.jinhyeon.notificationlab.campaign.batch;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Versioned namespace: do not change these identities for an existing campaign. */
public record CampaignTargetIds(UUID notificationId, UUID taskId, UUID eventId) {
    public static CampaignTargetIds of(UUID campaignId, long ordinal) {
        if (ordinal < 1 || ordinal > 1_000_000) throw new IllegalArgumentException("Invalid target ordinal");
        String key = "notification-lab:campaign:v1:" + campaignId + ":" + ordinal + ":";
        return new CampaignTargetIds(id(key + "notification"), id(key + "task"), id(key + "event"));
    }

    private static UUID id(String key) { return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)); }
}
