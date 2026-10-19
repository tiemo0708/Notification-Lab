package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.campaign.Campaign;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CampaignCheckpointTest {
    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");

    @Test
    void readerRestartsAtSavedKeyAndDoesNotTreatUncommittedReadsAsProgress() {
        var context = new ExecutionContext();
        var reader = new CampaignTargetReader(campaign(), 3);
        reader.open(context);
        for (int i = 1; i <= 3; i++) assertThat(reader.read().ordinal()).isEqualTo(i);
        reader.update(context);
        assertThat(reader.read().ordinal()).isEqualTo(4);
        assertThat(reader.read().ordinal()).isEqualTo(5);
        reader.close(); // Failed chunk: no committed update of this saved context.
        var resumed = new CampaignTargetReader(campaign(), 2);
        resumed.open(context);
        for (int i = 4; i <= 7; i++) assertThat(resumed.read().ordinal()).isEqualTo(i);
        assertThat(resumed.read()).isNull();
        resumed.update(context);
        assertThat(context.getLong(CampaignTargetReader.CHECKPOINT)).isEqualTo(7);
        resumed.close();
    }

    @Test
    void corruptedCheckpointCannotSkipBeyondTheAudience() {
        for (long value : new long[]{-1, 8, Long.MAX_VALUE}) {
            var context = new ExecutionContext();
            context.putLong(CampaignTargetReader.CHECKPOINT, value);
            assertThatThrownBy(() -> new CampaignTargetReader(campaign(), 3).open(context))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void identityNamespaceIsFrozenAndSeparatesTargetsCampaignsAndEntityTypes() {
        var ids = CampaignTargetIds.of(ID, 1);
        assertThat(ids.notificationId()).isEqualTo(UUID.fromString("f18443f1-653a-32c1-b2a3-d856bdd2a8f3"));
        assertThat(ids.taskId()).isEqualTo(UUID.fromString("3e55c73d-13ed-336d-8fc8-a17fc7e6b44b"));
        assertThat(ids.eventId()).isEqualTo(UUID.fromString("772354ed-8306-3fc0-a55d-5f2e2e0a4ecb"));
        assertThat(CampaignTargetIds.of(ID, 1)).isEqualTo(ids);
        assertThat(CampaignTargetIds.of(ID, 2)).isNotEqualTo(ids);
        assertThat(CampaignTargetIds.of(UUID.fromString("00000000-0000-0000-0000-000000000002"), 1)).isNotEqualTo(ids);
    }

    @Test
    void writerCannotCreatePartialWorkWithoutATransaction() {
        var writer = new CampaignTargetWriter(campaign(), null, 3);
        assertThatThrownBy(() -> writer.write(new org.springframework.batch.item.Chunk<>()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("chunk transaction");
    }

    @Test
    void batchConfigurationRejectsWrongLaneAndNonUtcMetadata() {
        var environment = new org.springframework.mock.env.MockEnvironment().withProperty("app.traffic.lane", "REALTIME");
        assertThatThrownBy(() -> new CampaignBatchConfiguration(null, null, environment))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("CAMPAIGN");
        var original = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Asia/Seoul"));
            environment.withProperty("app.traffic.lane", "CAMPAIGN");
            assertThatThrownBy(() -> new CampaignBatchConfiguration(null, null, environment))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("UTC");
        } finally { java.util.TimeZone.setDefault(original); }
    }

    private Campaign campaign() {
        return new Campaign(ID, "test", "title", "synthetic", "SYNTHETIC_V1", 7,
                Campaign.Status.START_REQUESTED, Instant.EPOCH, Instant.EPOCH);
    }
}
