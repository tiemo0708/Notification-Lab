package dev.jinhyeon.notificationlab.campaign.batch;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/** Test-only, fixed-size producer observations. No client IDs, addresses or arbitrary labels are exported. */
final class DeliveryProducerObservation {
    static final List<String> METRICS = List.of("record-send-total", "record-error-total", "record-retry-total",
            "batch-size-avg", "records-per-request-avg", "compression-rate-avg", "outgoing-byte-total",
            "request-total", "record-size-avg", "record-queue-time-avg", "request-latency-avg");

    private DeliveryProducerObservation() { }

    static Map<String, String> configuration(Map<String, Object> raw, String codec) {
        if (!("none".equals(codec) || "lz4".equals(codec))) throw new IllegalArgumentException("Unsupported comparison codec");
        var expected = Map.ofEntries(Map.entry("acks", "all"), Map.entry("enable.idempotence", "true"),
                Map.entry("max.in.flight.requests.per.connection", "1"), Map.entry("retries", "3"),
                Map.entry("buffer.memory", "1048576"), Map.entry("batch.size", "16384"), Map.entry("linger.ms", "0"),
                Map.entry("max.block.ms", "3000"), Map.entry("request.timeout.ms", "3000"),
                Map.entry("delivery.timeout.ms", "10000"), Map.entry("compression.type", codec));
        var selected = new LinkedHashMap<String, String>();
        expected.forEach((name, value) -> {
            String actual = String.valueOf(raw.get(name));
            if (name.equals("acks") && actual.equals("-1")) actual = "all";
            if (!value.equals(actual)) throw new IllegalArgumentException("Unexpected producer comparison setting: " + name);
            selected.put(name, actual);
        });
        return selected;
    }

    static Map<String, Double> metrics(Map<MetricName, ? extends Metric> raw) {
        var selected = new LinkedHashMap<String, Double>();
        raw.forEach((name, metric) -> {
            if (!name.group().equals("producer-metrics") || !METRICS.contains(name.name())) return;
            if (selected.containsKey(name.name())) throw new IllegalArgumentException("Duplicate producer metric");
            double value = ((Number) metric.metricValue()).doubleValue();
            if (name.name().endsWith("-total") && (!Double.isFinite(value) || value < 0)) {
                throw new IllegalArgumentException("Invalid cumulative producer counter");
            }
            selected.put(name.name(), Double.isFinite(value) ? value : null);
        });
        if (selected.size() != METRICS.size()) throw new IllegalArgumentException("Missing producer observations");
        return selected;
    }
}
