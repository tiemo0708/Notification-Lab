package dev.jinhyeon.notificationlab.campaign;

import java.util.ArrayList;

/**
 * Immutable SYNTHETIC_V1 membership: ordinal 1..targetCount, ordered by ordinal.
 * This is a virtual fixture, not a SQL reader or an acknowledged Batch checkpoint.
 * Keep this mapping stable; a different membership rule needs a new audience version.
 */
public final class SyntheticCampaignAudience {
    public static final int MAX_PAGE_SIZE = 1000;

    private SyntheticCampaignAudience() { }

    public static CampaignAudiencePage read(Campaign campaign, long after, int limit) {
        if (!"SYNTHETIC_V1".equals(campaign.audience())
                || campaign.targetCount() < 1 || campaign.targetCount() > 1_000_000) {
            throw new CampaignAudienceUnavailableException();
        }
        if (after < 0 || after > campaign.targetCount() || limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new InvalidCampaignPageException();
        }
        // Jump directly to the key after the last returned ordinal; never allocate/scan the full set.
        long end = Math.min(after + limit, campaign.targetCount());
        var items = new ArrayList<CampaignAudiencePage.Target>((int) (end - after));
        for (long ordinal = after + 1; ordinal <= end; ordinal++) {
            items.add(new CampaignAudiencePage.Target(ordinal, "synthetic-v1-" + ordinal));
        }
        return new CampaignAudiencePage(campaign.campaignId(), campaign.audience(), campaign.targetCount(),
                items, end < campaign.targetCount() ? end : null);
    }
}
