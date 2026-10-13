package dev.jinhyeon.notificationlab.outbox;

import dev.jinhyeon.notificationlab.config.AsyncProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("!memory & !sync")
@ConditionalOnProperty(name = "app.async.relay-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxStore store;
    private final KafkaTemplate<String, String> kafka;
    private final AsyncProperties properties;
    private final MeterRegistry registry;

    public OutboxRelay(OutboxStore store, KafkaTemplate<String, String> kafka,
                       AsyncProperties properties, MeterRegistry registry) {
        this.store = store;
        this.kafka = kafka;
        this.properties = properties;
        this.registry = registry;
    }

    @Scheduled(fixedDelayString = "${app.async.relay-delay-ms:100}",
            initialDelayString = "${app.async.relay-initial-delay-ms:0}")
    public void relay() {
        try {
            for (int i = 0; i < 50; i++) {
                var next = store.claim(properties.maxPublishAttempts());
                if (next.isEmpty()) return;
                var claim = next.get();
                try {
                    // No surrounding DB transaction. Only confirmed ACKs can become PUBLISHED.
                    kafka.send(properties.topic(), claim.notificationId(), claim.payload()).get(12, TimeUnit.SECONDS);
                } catch (Exception failure) {
                    store.failed(claim, properties.maxPublishAttempts());
                    registry.counter("outbox.publish.failures").increment();
                    if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                    return;
                }
                if (store.published(claim)) registry.counter("outbox.published").increment();
            }
        } catch (RuntimeException failure) {
            // An ACK followed by a DB failure leaves the lease for recovery (duplicates are possible).
            log.warn("Outbox relay paused: exceptionType={}", failure.getClass().getSimpleName());
        }
    }
}
