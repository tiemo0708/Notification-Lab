package dev.jinhyeon.notificationlab.outbox;

import dev.jinhyeon.notificationlab.config.TrafficRouting;
import dev.jinhyeon.notificationlab.config.AsyncProperties;
import dev.jinhyeon.notificationlab.config.RelayProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
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
    private final TrafficRouting routing;
    private final MeterRegistry registry;
    private final Timer claimTime;
    private final Timer ackTime;
    private final Timer publishedTime;
    private final Timer runTime;
    private final int publishBatchSize;
    private volatile boolean stopping;

    public OutboxRelay(OutboxStore store, KafkaTemplate<String, String> kafka,
                       AsyncProperties properties, MeterRegistry registry, TrafficRouting routing) {
        this(store, kafka, properties, registry, routing, new RelayProperties(1));
    }

    @Autowired
    public OutboxRelay(OutboxStore store, KafkaTemplate<String, String> kafka,
                       AsyncProperties properties, MeterRegistry registry, TrafficRouting routing, RelayProperties relay) {
        if (relay.publishBatchSize() < 1 || relay.publishBatchSize() > 10) throw new IllegalArgumentException("Publish batch must contain 1 to 10 claims");
        this.publishBatchSize = relay.publishBatchSize();
        this.store = store;
        this.kafka = kafka;
        this.properties = properties;
        this.registry = registry;
        this.routing = routing;
        this.claimTime = registry.timer("notification.pipeline.stage", "stage", "relay.claim");
        this.ackTime = registry.timer("notification.pipeline.stage", "stage", "relay.ack");
        this.publishedTime = registry.timer("notification.pipeline.stage", "stage", "relay.mark.published");
        this.runTime = registry.timer("notification.pipeline.stage", "stage", "relay.run");
    }

    @EventListener(ContextClosedEvent.class)
    public void stopClaiming() {
        // Finish the already claimed publish; leave the remaining durable queue for the next process.
        stopping = true;
    }

    @Scheduled(fixedDelayString = "${app.async.relay-delay-ms:100}",
            initialDelayString = "${app.async.relay-initial-delay-ms:0}")
    public void relay() {
        var run = Timer.start(registry);
        var acknowledged = new ArrayList<OutboxStore.Claim>(publishBatchSize);
        long batchStart = System.nanoTime();
        try {
            try {
                for (int i = 0; i < 50 && !stopping; i++) {
                    // Soft age threshold, checked between blocking calls; never wait for a full batch.
                    if (!acknowledged.isEmpty() && System.nanoTime() - batchStart >= TimeUnit.MILLISECONDS.toNanos(100)) {
                        flush(acknowledged);
                    }
                    var next = claimTime.record(() -> store.claim(properties.maxPublishAttempts()));
                    if (next.isEmpty()) return;
                    var claim = next.get();
                    try {
                        var ack = Timer.start(registry);
                        try {
                            // No surrounding DB transaction. Only confirmed ACKs can become PUBLISHED.
                            kafka.send(routing.destination(claim.eventType()), claim.notificationId(), claim.payload()).get(12, TimeUnit.SECONDS);
                        } finally {
                            ack.stop(ackTime);
                        }
                    } catch (Exception failure) {
                        store.failed(claim, properties.maxPublishAttempts());
                        registry.counter("outbox.publish.failures").increment();
                        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                        return;
                    }
                    if (acknowledged.isEmpty()) batchStart = System.nanoTime();
                    acknowledged.add(claim);
                    if (acknowledged.size() >= publishBatchSize) flush(acknowledged);
                }
            } finally {
                // Also flush a partial batch on idle, stop, send failure or claim failure.
                flush(acknowledged);
            }
        } catch (RuntimeException failure) {
            // An ACK followed by a DB failure leaves the lease for recovery (duplicates are possible).
            log.warn("Outbox relay paused: exceptionType={}", failure.getClass().getSimpleName());
        } finally {
            run.stop(runTime);
        }
    }

    private void flush(List<OutboxStore.Claim> acknowledged) {
        if (acknowledged.isEmpty()) return;
        var batch = List.copyOf(acknowledged);
        // On DB failure, recover from durable leases instead of retrying this ambiguous commit in finally.
        acknowledged.clear();
        int count = publishedTime.record(() -> publishBatchSize == 1
                ? (store.published(batch.getFirst()) ? 1 : 0) : store.published(batch));
        if (count > 0) registry.counter("outbox.published").increment(count);
    }
}
