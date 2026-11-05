package dev.jinhyeon.notificationlab.campaign.batch;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryProducerObservationTest {
    @Test
    void readsTypedBootSettingsWithoutExportingAddressesOrArbitraryProperties() {
        var raw = settings(); raw.put("bootstrap.servers", "private-address");
        var selected = DeliveryProducerObservation.configuration(raw, "lz4");
        assertThat(selected).hasSize(11).containsEntry("compression.type", "lz4").containsEntry("acks", "all");
        assertThat(selected).doesNotContainKey("bootstrap.servers");
        raw.put("acks", "-1");
        assertThat(DeliveryProducerObservation.configuration(raw, "lz4")).containsEntry("acks", "all");
    }

    @Test
    void rejectsChangedDurabilityBoundsBatchingAndUnexpectedCodec() {
        for (var change : Map.of("acks", "1", "enable.idempotence", "false", "linger.ms", "5",
                "max.in.flight.requests.per.connection", "5", "buffer.memory", "33554432", "batch.size", "32768",
                "compression.type", "gzip").entrySet()) {
            var raw = settings(); raw.put(change.getKey(), change.getValue());
            assertThatThrownBy(() -> DeliveryProducerObservation.configuration(raw, "lz4")).isInstanceOf(IllegalArgumentException.class);
        }
        var missing = settings(); missing.remove("compression.type");
        assertThatThrownBy(() -> DeliveryProducerObservation.configuration(missing, "lz4")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void projectsOnlyFixedMetricsUsesNullForStaleAveragesAndRejectsBadCounters() {
        var raw = metrics(); raw.put(name("compression-rate-avg"), metric("compression-rate-avg", Double.NaN));
        raw.put(name("unexpected-secret-label"), metric("unexpected-secret-label", 999));
        var selected = DeliveryProducerObservation.metrics(raw);
        assertThat(selected).hasSize(11).containsEntry("compression-rate-avg", null).doesNotContainKey("unexpected-secret-label");
        raw.put(name("record-send-total"), metric("record-send-total", Double.POSITIVE_INFINITY));
        assertThatThrownBy(() -> DeliveryProducerObservation.metrics(raw)).isInstanceOf(IllegalArgumentException.class);
        var missing = metrics(); missing.remove(name("record-send-total"));
        assertThatThrownBy(() -> DeliveryProducerObservation.metrics(missing)).isInstanceOf(IllegalArgumentException.class);
    }

    private static Map<String, Object> settings() {
        return new HashMap<>(Map.ofEntries(Map.entry("acks", "all"), Map.entry("enable.idempotence", true),
                Map.entry("max.in.flight.requests.per.connection", "1"), Map.entry("retries", 3),
                Map.entry("buffer.memory", 1048576L), Map.entry("batch.size", 16384), Map.entry("linger.ms", "0"),
                Map.entry("max.block.ms", "3000"), Map.entry("request.timeout.ms", "3000"),
                Map.entry("delivery.timeout.ms", "10000"), Map.entry("compression.type", "lz4")));
    }

    private static Map<MetricName, Metric> metrics() {
        var values = new HashMap<MetricName, Metric>();
        DeliveryProducerObservation.METRICS.forEach(name -> values.put(name(name), metric(name, 1.0)));
        return values;
    }

    private static MetricName name(String name) { return new MetricName(name, "producer-metrics", "", Map.of("client-id", "do-not-export")); }

    private static Metric metric(String name, Object value) {
        return new Metric() {
            public MetricName metricName() { return name(name); }
            public Object metricValue() { return value; }
        };
    }
}
