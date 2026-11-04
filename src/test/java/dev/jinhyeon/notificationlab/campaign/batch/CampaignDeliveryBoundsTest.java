package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CampaignDeliveryBoundsTest {
    private final UUID campaignId = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void partitionComparisonKeepsBoundedTenThousandTargetRuns() {
        for (int partitions : new int[]{3, 6}) {
            var bounds = CampaignDeliveryBounds.forPartitionRun(false, 10_000, partitions, false);
            assertThat(bounds.maxRealtimeProbes()).isEqualTo(5000);
            assertThat(bounds.maxResourceSamples()).isEqualTo(452);
        }
    }

    @Test
    void partitionComparisonRejectsLargeRunsUnsupportedTopologyAndMixedFactors() {
        for (int partitions : new int[]{0, 1, 4, 12, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> CampaignDeliveryBounds.forPartitionRun(false, 10_000, partitions, false)).isInstanceOf(IllegalArgumentException.class);
        }
        for (int targets : new int[]{1000, 100_000, 1_000_000}) {
            assertThatThrownBy(() -> CampaignDeliveryBounds.forPartitionRun(false, targets, 3, false)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> CampaignDeliveryBounds.forPartitionRun(true, 10_000, 3, false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CampaignDeliveryBounds.forPartitionRun(false, 10_000, 3, true)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void consumerComparisonKeepsSmallRunBoundsForEachSupportedConcurrency() {
        for (int consumers : new int[]{1, 2, 3}) {
            var bounds = CampaignDeliveryBounds.forConsumerRun(false, 10_000, consumers, false);
            assertThat(bounds.maxRealtimeProbes()).isEqualTo(5000);
            assertThat(bounds.maxResourceSamples()).isEqualTo(452);
        }
    }

    @Test
    void consumerComparisonRejectsLargeRunsExtraConsumersAndMixedFactors() {
        for (int consumers : new int[]{0, 4, 100}) {
            assertThatThrownBy(() -> CampaignDeliveryBounds.forConsumerRun(false, 10_000, consumers, false)).isInstanceOf(IllegalArgumentException.class);
        }
        for (int targets : new int[]{1000, 100_000, 1_000_000}) {
            assertThatThrownBy(() -> CampaignDeliveryBounds.forConsumerRun(false, targets, 3, false)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> CampaignDeliveryBounds.forConsumerRun(true, 10_000, 3, false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CampaignDeliveryBounds.forConsumerRun(false, 10_000, 3, true)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void offsetComparisonAllowsOnlySmallPilotsOrTheExplicitHundredThousandRun() {
        assertThat(CampaignDeliveryBounds.forOffsetRun(false, 1000).targets()).isEqualTo(1000);
        assertThat(CampaignDeliveryBounds.forOffsetRun(false, 10_000).targets()).isEqualTo(10_000);
        var load = CampaignDeliveryBounds.forOffsetRun(true, 100_000);
        assertThat(load.drainLimitSeconds()).isEqualTo(3600);
        assertThat(load.maxRealtimeProbes()).isEqualTo(20_000);
        assertThat(load.maxResourceSamples()).isEqualTo(1802);
        for (int targets : new int[] {0, 10_000, 100_001, 1_000_000, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> CampaignDeliveryBounds.forOffsetRun(true, targets)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> CampaignDeliveryBounds.forOffsetRun(false, 100_000)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pilotCannotAccidentallyStartLargeOrOutOfRangeRuns() {
        for (int targets : new int[] {0, 999, 10_001, 100_000, 1_000_000, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> CampaignDeliveryBounds.forRun(false, targets))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void loadAcceptsOnlyExplicitValidatedSizes() {
        for (int targets : new int[] {0, 10_000, 100_001, 999_999, 1_000_001, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> CampaignDeliveryBounds.forRun(true, targets))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(CampaignDeliveryBounds.forRun(true, 100_000).maxRealtimeProbes()).isEqualTo(20_000);
        assertThat(CampaignDeliveryBounds.forRun(true, 1_000_000).maxRealtimeProbes()).isEqualTo(150_000);
    }

    @Test
    void probeAndMemoryBoundsCoverTheWholeSequentialDrainWindow() {
        for (var bounds : new CampaignDeliveryBounds[] {
                CampaignDeliveryBounds.forRun(false, 1000), CampaignDeliveryBounds.forRun(false, 10_000),
                CampaignDeliveryBounds.forRun(true, 100_000), CampaignDeliveryBounds.forRun(true, 1_000_000)}) {
            // 50 baseline requests, then at least 200ms sleep per mixed request, plus a boundary iteration.
            assertThat(bounds.maxRealtimeProbes()).isGreaterThanOrEqualTo(50 + bounds.drainLimitSeconds() * 5 + 1);
            assertThat(bounds.latencyArrayBytes()).isLessThanOrEqualTo(8_000_000);
            assertThat(bounds.campaignAuditBytes()).isLessThanOrEqualTo(125_008);
            assertThat(bounds.maxResourceSamples()).isLessThanOrEqualTo(14_402);
        }
    }

    @Test
    void millionTargetAuditValidatesFirstAndLastIdsAndDetectsDuplicates() throws Exception {
        var audit = new CampaignDeliveryPilotTest.Audit(campaignId, 1_000_000, 150_000);
        audit.observe(campaignPayload(1), 202);
        audit.observe(campaignPayload(1_000_000), 202);
        audit.observe(campaignPayload(1), 202);
        assertThat(audit.summary()).containsEntry("uniqueCampaign", 2).containsEntry("duplicates", 1).containsEntry("invalid", 0);
        assertThat(audit.count()).isEqualTo(3);
    }

    @Test
    void auditRejectsOversizedOrdinalsWrongIdsAndUnacceptedHttpWithoutRecordingThem() throws Exception {
        var audit = new CampaignDeliveryPilotTest.Audit(campaignId, 1_000_000, 150_000);
        audit.observe(payload("synthetic-v1-1000001", campaignId.toString()), 202);
        audit.observe(payload("synthetic-v1-" + Integer.MAX_VALUE, campaignId.toString()), 202);
        audit.observe(payload("synthetic-v1-1", campaignId.toString()), 202);
        audit.observe(campaignPayload(1), 500);
        assertThat(audit.summary()).containsEntry("uniqueCampaign", 0).containsEntry("invalid", 4);
    }

    @Test
    void realtimeAuditUsesItsSeparateUpperBound() throws Exception {
        var audit = new CampaignDeliveryPilotTest.Audit(campaignId, 1_000_000, 150_000);
        audit.observe(payload("pilot-realtime-150000", campaignId.toString()), 202);
        audit.observe(payload("pilot-realtime-150001", campaignId.toString()), 202);
        audit.observe(payload("pilot-realtime-" + Integer.MAX_VALUE, campaignId.toString()), 202);
        assertThat(audit.summary()).containsEntry("uniqueRealtime", 1).containsEntry("invalid", 2);
    }

    private byte[] campaignPayload(int ordinal) throws Exception {
        return payload("synthetic-v1-" + ordinal, CampaignTargetIds.of(campaignId, ordinal).notificationId().toString());
    }

    private byte[] payload(String recipient, String id) throws Exception {
        return mapper.writeValueAsBytes(Map.of("recipientId", recipient, "notificationId", id));
    }
}
