package dev.jinhyeon.notificationlab.outbox;

import dev.jinhyeon.notificationlab.config.AsyncProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("!memory & !sync")
public class PipelineMetrics {
    private final JdbcTemplate jdbc;
    private final Admin admin;
    private final AsyncProperties properties;
    private volatile double unpublished = Double.NaN;
    private volatile double failed = Double.NaN;
    private volatile double oldestAge = Double.NaN;
    private volatile double lag = Double.NaN;
    private volatile long dbSample;
    private volatile long kafkaSample;

    public PipelineMetrics(JdbcTemplate jdbc, KafkaAdmin kafkaAdmin, AsyncProperties properties,
                           KafkaListenerEndpointRegistry listeners, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.admin = Admin.create(kafkaAdmin.getConfigurationProperties());
        this.properties = properties;
        Gauge.builder("outbox.unpublished", this, m -> m.unpublished).register(registry);
        Gauge.builder("outbox.failed", this, m -> m.failed).register(registry);
        Gauge.builder("outbox.oldest.age.seconds", this, m -> m.oldestAge).register(registry);
        Gauge.builder("notification.consumer.lag", this, m -> m.lag).register(registry);
        Gauge.builder("outbox.sample.age.seconds", this, m -> age(m.dbSample)).register(registry);
        Gauge.builder("notification.consumer.sample.age.seconds", this, m -> age(m.kafkaSample)).register(registry);
        Gauge.builder("notification.worker.running", listeners, l -> {
            var container = l.getListenerContainer("notification-worker");
            return container != null && container.isRunning() ? 1 : 0;
        }).register(registry);
    }

    @Scheduled(fixedDelay = 5000)
    public void refresh() {
        try {
            jdbc.query("""
                    SELECT COUNT(*), COALESCE(SUM(status = 'FAILED'), 0),
                        COALESCE(TIMESTAMPDIFF(MICROSECOND, MIN(created_at), UTC_TIMESTAMP(6)) / 1000000.0, 0)
                    FROM outbox_event WHERE status <> 'PUBLISHED'
                    """, rs -> {
                unpublished = rs.getDouble(1);
                failed = rs.getDouble(2);
                oldestAge = Math.max(0, rs.getDouble(3));
            });
            dbSample = System.nanoTime();
        } catch (RuntimeException unavailable) {
            unpublished = failed = oldestAge = Double.NaN;
        }
        try {
            var topic = admin.describeTopics(List.of(properties.topic())).allTopicNames()
                    .get(2, TimeUnit.SECONDS).get(properties.topic());
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            topic.partitions().forEach(p -> latest.put(new TopicPartition(properties.topic(), p.partition()), OffsetSpec.latest()));
            var ends = admin.listOffsets(latest).all().get(2, TimeUnit.SECONDS);
            var committed = admin.listConsumerGroupOffsets(properties.group()).partitionsToOffsetAndMetadata()
                    .get(2, TimeUnit.SECONDS);
            lag = ends.entrySet().stream().mapToLong(e -> Math.max(0, e.getValue().offset()
                    - (committed.get(e.getKey()) == null ? 0 : committed.get(e.getKey()).offset()))).sum();
            kafkaSample = System.nanoTime();
        } catch (Exception unavailable) {
            lag = Double.NaN;
            if (unavailable instanceof InterruptedException) Thread.currentThread().interrupt();
        }
    }

    private static double age(long sample) {
        return sample == 0 ? Double.NaN : (System.nanoTime() - sample) / 1_000_000_000.0;
    }

    @PreDestroy
    void close() {
        admin.close(java.time.Duration.ofSeconds(2));
    }
}
