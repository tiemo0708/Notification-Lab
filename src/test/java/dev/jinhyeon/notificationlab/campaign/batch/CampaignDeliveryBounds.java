package dev.jinhyeon.notificationlab.campaign.batch;

/** Resource limits for opt-in experiments, never application delivery settings. */
record CampaignDeliveryBounds(int targets, int maxRealtimeProbes, int drainLimitSeconds) {
    static CampaignDeliveryBounds forRun(boolean load, int targets) {
        if (!load && targets >= 1000 && targets <= 10_000) {
            return new CampaignDeliveryBounds(targets, 5000, 900);
        }
        if (load && targets == 100_000) {
            return new CampaignDeliveryBounds(targets, 20_000, 3600);
        }
        if (load && targets == 1_000_000) {
            return new CampaignDeliveryBounds(targets, 150_000, 8 * 3600);
        }
        throw new IllegalArgumentException("Pilot targets must be 1000..10000; load targets must be 100000 or 1000000");
    }

    int maxResourceSamples() { return drainLimitSeconds / 2 + 2; }

    static CampaignDeliveryBounds forOffsetRun(boolean load, int targets) {
        if (load && targets != 100_000) throw new IllegalArgumentException("Offset load is limited to exactly 100000 targets");
        return forRun(load, targets);
    }

    long campaignAuditBytes() { return ((targets + 64L) / 64) * Long.BYTES; }

    long latencyArrayBytes() { return (long) Math.max(targets, maxRealtimeProbes) * Double.BYTES; }
}
