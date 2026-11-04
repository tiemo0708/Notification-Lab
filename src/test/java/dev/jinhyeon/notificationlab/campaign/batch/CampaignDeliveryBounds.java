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

    static CampaignDeliveryBounds forConsumerRun(boolean load, int targets, int consumers, boolean conflictingMode) {
        if (load || targets != 10_000 || consumers < 1 || consumers > 3 || conflictingMode) {
            throw new IllegalArgumentException("Consumer comparison requires isolated 10k pilot and 1..3 campaign consumers");
        }
        return forRun(false, targets);
    }

    static CampaignDeliveryBounds forPartitionRun(boolean load, int targets, int partitions, boolean conflictingMode) {
        if (load || targets != 10_000 || (partitions != 3 && partitions != 6) || conflictingMode) {
            throw new IllegalArgumentException("Partition comparison requires isolated 10k pilot and 3 or 6 campaign partitions");
        }
        return forRun(false, targets);
    }

    int maxResourceSamples() { return drainLimitSeconds / 2 + 2; }

    static CampaignDeliveryBounds forOffsetRun(boolean load, int targets) {
        if (load && targets != 100_000) throw new IllegalArgumentException("Offset load is limited to exactly 100000 targets");
        return forRun(load, targets);
    }

    long campaignAuditBytes() { return ((targets + 64L) / 64) * Long.BYTES; }

    long latencyArrayBytes() { return (long) Math.max(targets, maxRealtimeProbes) * Double.BYTES; }
}
