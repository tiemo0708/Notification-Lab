package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.config.RelayProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryRelayObservationTest {
    @Test
    void requiresTheEffectiveWindowAndAnUnchangedCompletionBatch() {
        assertThat(DeliveryRelayObservation.configuration(new RelayProperties(1, 10), 10))
                .containsEntry("submissionWindowSize", 10).containsEntry("publishBatchSize", 1);
        assertThatThrownBy(() -> DeliveryRelayObservation.configuration(new RelayProperties(1, 1), 10)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeliveryRelayObservation.configuration(new RelayProperties(10, 10), 10)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeliveryRelayObservation.configuration(new RelayProperties(1, 2), 2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresEveryAckAndPublishedRowWithoutAssumingMissingSuccessMetricsMeanZero() {
        var registry = new SimpleMeterRegistry();
        try {
            assertThatThrownBy(() -> DeliveryRelayObservation.completion(registry, 1)).isInstanceOf(IllegalStateException.class);
            registry.counter("outbox.published").increment();
            assertThatThrownBy(() -> DeliveryRelayObservation.completion(registry, 1)).isInstanceOf(IllegalStateException.class);
            registry.timer("notification.pipeline.stage", "stage", "relay.ack").record(1, TimeUnit.MILLISECONDS);
            assertThat(DeliveryRelayObservation.completion(registry, 1)).containsEntry("published", 1.0).containsEntry("unconfirmedAcks", 0.0);
            assertThatThrownBy(() -> DeliveryRelayObservation.completion(registry, 2)).isInstanceOf(IllegalStateException.class);
        } finally { registry.close(); }
    }

    @Test
    void rejectsAnyPublishFailureOrUnconfirmedAckEvenWhenAllRowsComplete() {
        for (String error : new String[]{"outbox.publish.failures", "outbox.publish.unconfirmed"}) {
            var registry = new SimpleMeterRegistry();
            try {
                registry.counter("outbox.published").increment();
                registry.timer("notification.pipeline.stage", "stage", "relay.ack").record(1, TimeUnit.MILLISECONDS);
                registry.counter(error).increment();
                assertThatThrownBy(() -> DeliveryRelayObservation.completion(registry, 1)).isInstanceOf(IllegalStateException.class);
            } finally { registry.close(); }
        }
    }
}
