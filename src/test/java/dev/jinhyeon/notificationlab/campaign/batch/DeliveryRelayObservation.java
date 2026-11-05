package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.config.RelayProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;

/** Effective settings and ACK completion guards for the test-only one-factor window experiment. */
final class DeliveryRelayObservation {
    private DeliveryRelayObservation() { }

    static Map<String, Integer> configuration(RelayProperties actual, int expectedWindow) {
        if ((expectedWindow != 1 && expectedWindow != 10) || actual.submissionWindowSize() != expectedWindow
                || actual.publishBatchSize() != 1) {
            throw new IllegalArgumentException("Relay window comparison settings differ from the protocol");
        }
        return Map.of("submissionWindowSize", actual.submissionWindowSize(), "publishBatchSize", actual.publishBatchSize());
    }

    static Map<String, Double> completion(MeterRegistry registry, int expectedRecords) {
        var published = registry.find("outbox.published").counter();
        var acknowledged = registry.find("notification.pipeline.stage").tag("stage", "relay.ack").timer();
        double failures = counter(registry, "outbox.publish.failures");
        double unconfirmed = counter(registry, "outbox.publish.unconfirmed");
        if (expectedRecords < 1 || published == null || acknowledged == null
                || published.count() != expectedRecords || acknowledged.count() != expectedRecords
                || failures != 0 || unconfirmed != 0) {
            throw new IllegalStateException("Relay ACK completion differs from the normal-path comparison contract");
        }
        return Map.of("published", published.count(), "ackObservations", (double) acknowledged.count(),
                "publishFailures", failures, "unconfirmedAcks", unconfirmed);
    }

    private static double counter(MeterRegistry registry, String name) {
        var counter = registry.find(name).counter();
        return counter == null ? 0 : counter.count(); // These error counters are lazily created on the error path.
    }
}
