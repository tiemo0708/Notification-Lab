package dev.jinhyeon.notificationlab.outbox;

import dev.jinhyeon.notificationlab.config.TrafficRouting;
import dev.jinhyeon.notificationlab.config.AsyncProperties;
import dev.jinhyeon.notificationlab.config.RelayProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.springframework.kafka.support.SendResult;
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
    private final int submissionWindowSize;
    private final long ackWaitNanos;
    private final AtomicBoolean running = new AtomicBoolean();
    // A timed-out/interrupted send can still be alive in the producer between scheduled runs.
    // Retain at most one window until settled; observing its late completion never writes DB state.
    private final List<CompletableFuture<SendResult<String, String>>> unresolved = new ArrayList<>(10);
    private volatile boolean stopping;

    public OutboxRelay(OutboxStore store, KafkaTemplate<String, String> kafka,
                       AsyncProperties properties, MeterRegistry registry, TrafficRouting routing) {
        this(store, kafka, properties, registry, routing, new RelayProperties(1));
    }

    @Autowired
    public OutboxRelay(OutboxStore store, KafkaTemplate<String, String> kafka,
                       AsyncProperties properties, MeterRegistry registry, TrafficRouting routing, RelayProperties relay) {
        this(store, kafka, properties, registry, routing, relay, Duration.ofSeconds(12));
    }

    // A shorter deadline is available only to same-package fault tests, not application configuration.
    OutboxRelay(OutboxStore store, KafkaTemplate<String, String> kafka,
                AsyncProperties properties, MeterRegistry registry, TrafficRouting routing, RelayProperties relay,
                Duration ackWait) {
        if (relay.publishBatchSize() < 1 || relay.publishBatchSize() > 10) throw new IllegalArgumentException("Publish batch must contain 1 to 10 claims");
        if (relay.submissionWindowSize() < 1 || relay.submissionWindowSize() > 10) throw new IllegalArgumentException("Submission window must contain 1 to 10 claims");
        if (ackWait.isNegative() || ackWait.isZero() || ackWait.compareTo(Duration.ofSeconds(12)) > 0) throw new IllegalArgumentException("ACK wait must be positive and at most 12 seconds");
        this.publishBatchSize = relay.publishBatchSize();
        this.submissionWindowSize = relay.submissionWindowSize();
        this.ackWaitNanos = ackWait.toNanos();
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
        // Drain the submitted window; leave unclaimed work and unresolved leases durable.
        stopping = true;
    }

    @Scheduled(fixedDelayString = "${app.async.relay-delay-ms:100}",
            initialDelayString = "${app.async.relay-initial-delay-ms:0}")
    public void relay() {
        // Scheduled and manual invocations share one bounded window, without a waiting task queue.
        if (!running.compareAndSet(false, true)) return;
        var run = Timer.start(registry);
        var acknowledged = new ArrayList<OutboxStore.Claim>(publishBatchSize);
        long batchStart = System.nanoTime();
        try {
            try {
                if (submissionWindowSize > 1) {
                    unresolved.removeIf(CompletableFuture::isDone);
                    if (unresolved.isEmpty()) relayWindows(acknowledged);
                    return;
                }
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
                            kafka.send(routing.destination(claim.eventType()), claim.notificationId(), claim.payload()).get(ackWaitNanos, TimeUnit.NANOSECONDS);
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
            running.set(false);
        }
    }

    private record Pending(OutboxStore.Claim claim, CompletableFuture<SendResult<String, String>> future,
                           long startedNanos) { }

    private void relayWindows(List<OutboxStore.Claim> acknowledged) {
        int claimed = 0;
        while (claimed < 50 && !stopping && !Thread.currentThread().isInterrupted()) {
            var pending = new ArrayList<Pending>(submissionWindowSize);
            // One deadline includes filling and draining, rather than 12 seconds for each record.
            // Blocking DB/send calls still have their own client limits; this is not a hard run deadline.
            long deadline = System.nanoTime() + ackWaitNanos;
            boolean halt = false;
            try {
                while (pending.size() < submissionWindowSize && claimed < 50 && !stopping) {
                    if (System.nanoTime() >= deadline || Thread.currentThread().isInterrupted()) {
                        halt = true;
                        break;
                    }
                    var next = claimTime.record(() -> store.claim(properties.maxPublishAttempts()));
                    if (next.isEmpty()) {
                        halt = true;
                        break;
                    }
                    var claim = next.get();
                    claimed++;
                    if (stopping || System.nanoTime() >= deadline || Thread.currentThread().isInterrupted()) {
                        // Not submitted. Do not invent an ACK or undo an ambiguous claim transaction.
                        halt = true;
                        break;
                    }
                    long started = System.nanoTime();
                    try {
                        var future = kafka.send(routing.destination(claim.eventType()), claim.notificationId(), claim.payload());
                        pending.add(new Pending(claim, future, started));
                        if (future.isCompletedExceptionally()) {
                            halt = true;
                            break;
                        }
                    } catch (RuntimeException failure) {
                        ackTime.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
                        registry.counter("outbox.publish.failures").increment();
                        store.failed(claim, properties.maxPublishAttempts());
                        halt = true;
                        break;
                    }
                }
            } finally {
                // A claim/send failure must not discard earlier submitted ACKs.
                try {
                    if (!drainWindow(pending, deadline, acknowledged)) halt = true;
                    flush(acknowledged);
                } finally {
                    // Also retain the unobserved tail after a DB failure while draining.
                    for (var entry : pending) {
                        if (!entry.future().isDone()) unresolved.add(entry.future());
                    }
                }
            }
            if (halt) return;
        }
    }

    private boolean drainWindow(List<Pending> pending, long deadline, List<OutboxStore.Claim> acknowledged) {
        boolean successful = true;
        for (var entry : pending) {
            try {
                entry.future().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                acknowledged.add(entry.claim());
                if (acknowledged.size() >= publishBatchSize) flush(acknowledged);
            } catch (ExecutionException | CancellationException failure) {
                successful = false;
                registry.counter("outbox.publish.failures").increment();
                store.failed(entry.claim(), properties.maxPublishAttempts());
            } catch (TimeoutException unknown) {
                successful = false;
                // Timeout does not cancel a broker send. No late callback writes DB state.
                // Leave the lease for bounded durable recovery; re-publication may duplicate records.
                registry.counter("outbox.publish.unconfirmed").increment();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                // Observed submission-to-drain time, including other claims/ACKs in this window.
                ackTime.record(System.nanoTime() - entry.startedNanos(), TimeUnit.NANOSECONDS);
            }
        }
        return successful;
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
