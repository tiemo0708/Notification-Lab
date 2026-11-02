package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlabtest.OffsetCommitPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ContainerProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OffsetCommitPolicyTest {
    @Test
    void changesOnlyAcknowledgmentPolicyAndPreservesShutdownAndPolling() {
        for (var mode : OffsetCommitPolicy.Mode.values()) {
            var properties = new ContainerProperties("synthetic");
            properties.setPollTimeout(500);
            properties.setStopImmediate(true);
            OffsetCommitPolicy.apply(properties, mode);
            assertThat(properties.getAckMode().name()).isEqualTo(mode.name());
            assertThat(properties.getAckCount()).isEqualTo(10);
            assertThat(properties.getAckTime()).isEqualTo(100);
            assertThat(properties.getPollTimeout()).isEqualTo(500);
            assertThat(properties.isStopImmediate()).isTrue();
            assertThat(properties.isSyncCommits()).isTrue();
            assertThat(properties.isAsyncAcks()).isFalse();
        }
    }

    @Test
    void rejectsUnknownModesAndUnvalidatedAsyncSemantics() {
        for (String value : new String[] {null, "", "BATCH", "record", "../COUNT_TIME"}) {
            assertThatThrownBy(() -> OffsetCommitPolicy.mode(value)).isInstanceOf(IllegalArgumentException.class);
        }
        var properties = new ContainerProperties("synthetic");
        properties.setSyncCommits(false);
        assertThatThrownBy(() -> OffsetCommitPolicy.apply(properties, OffsetCommitPolicy.Mode.COUNT_TIME)).isInstanceOf(IllegalArgumentException.class);
        properties.setSyncCommits(true); properties.setAsyncAcks(true);
        assertThatThrownBy(() -> OffsetCommitPolicy.apply(properties, OffsetCommitPolicy.Mode.COUNT_TIME)).isInstanceOf(IllegalArgumentException.class);
    }
}
