package dev.jinhyeon.notificationlab.campaign;

import java.util.List;
import java.util.UUID;

public record CampaignAudiencePage(UUID campaignId, String audience, int targetCount,
                                   List<Target> items, Long nextAfter) {
    public CampaignAudiencePage {
        items = List.copyOf(items);
    }

    public record Target(long ordinal, String recipientId) { }
}
