package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.provider.ProviderCircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/** Retain failed records in Spring's bounded poll buffer; quota shortage and open provider circuits are resumable. */
@Component
@Profile("!memory & !sync")
public class WorkerPauseErrorHandler extends CommonContainerStoppingErrorHandler {
    private final WorkerAdmission admission;
    private final MeterRegistry registry;
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
        var thread = new Thread(runnable, "worker-quota-resume");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<MessageListenerContainer, Pause> pending = new HashMap<>();
    private volatile boolean stopping;
    private final Map<RetryKey, RetryBudget> transientRetries = new HashMap<>();
    private record RetryKey(MessageListenerContainer container, String topic, int partition) { }
    private record RetryBudget(long offset, int failures) { }
    private static final int MAX_TRANSIENT_RECORDS = 6; // Main + retry topics, three partitions each.
    public static final int MAX_TRANSIENT_PAUSES = 3;

    private static final class Pause {
        private ScheduledFuture<?> future;
        private String reason;
    }

    public WorkerPauseErrorHandler(WorkerAdmission admission, MeterRegistry registry) {
        this.admission = admission;
        this.registry = registry;
        scheduler.setRemoveOnCancelPolicy(true);
        registry.gauge("notification.worker.quota.pauses.pending", this, handler -> handler.pendingCount("quota"));
        registry.gauge("notification.worker.circuit.pauses.pending", this, handler -> handler.pendingCount("circuit"));
        registry.gauge("notification.worker.quota_transient.pauses.pending", this, handler -> handler.pendingCount("quota_transient"));
        registry.gauge("notification.worker.quota.transient.records", this, WorkerPauseErrorHandler::transientRecordCount);
        registry.counter("notification.worker.quota.transient.exhausted");
    }

    @Override
    public boolean seeksAfterHandling() { return false; }

    @Override
    public boolean isAckAfterHandle() { return false; }

    @Override
    public boolean handleOne(Exception failure, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
                             MessageListenerContainer container) {
        ensureRunning();
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 8; depth++, cause = cause.getCause()) {
            if (cause instanceof WorkerQuota.QuotaUnavailableDeferred) {
                long delay;
                try {
                    delay = transientDelay(container, record);
                    if (delay >= 0) pause(container, delay, "quota_transient");
                } catch (RuntimeException unavailable) {
                    handleRemaining(new IllegalStateException("Transient quota recovery unavailable"), List.of(record), consumer, container);
                    return false;
                }
                if (delay < 0) handleRemaining(failure, List.of(record), consumer, container);
                return false;
            }
            String reason = cause instanceof WorkerQuota.QuotaDeferred ? "quota" : "circuit";
            long delay = cause instanceof WorkerQuota.QuotaDeferred quota ? quota.delayMs()
                    : cause instanceof ProviderCircuitBreaker.CircuitDeferred circuit ? circuit.delayMs() : -1;
            if (delay >= 0) {
                try {
                    pause(container, delay, reason);
                } catch (RuntimeException unavailable) {
                    handleRemaining(new IllegalStateException("Worker pause unavailable"), List.of(record), consumer, container);
                }
                return false; // Never recover/skip: Spring retains this record and keeps polling while paused.
            }
        }
        handleRemaining(failure, List.of(record), consumer, container);
        return false;
    }

    private synchronized long transientDelay(MessageListenerContainer container, ConsumerRecord<?, ?> record) {
        // Spring passes the shared parent container; keep independent budgets for its partitions.
        var key = new RetryKey(container, record.topic(), record.partition());
        var previous = transientRetries.get(key);
        int failures = previous != null && previous.offset() == record.offset() ? previous.failures() + 1 : 1;
        if (previous == null && transientRetries.size() >= MAX_TRANSIENT_RECORDS) throw new IllegalStateException("Transient quota capacity exceeded");
        transientRetries.put(key, new RetryBudget(record.offset(), failures));
        if (failures > MAX_TRANSIENT_PAUSES) {
            registry.counter("notification.worker.quota.transient.exhausted").increment();
            return -1;
        }
        return 250L << (failures - 1); // Three pauses: 250, 500, 1000ms; fourth failure stops.
    }

    private synchronized void pause(MessageListenerContainer container, long delayMs, String reason) {
        ensureRunning();
        if (pending.containsKey(container)) return;
        // One parent container in this application; keep a hard bound if more listeners are added later.
        if (pending.size() >= 3) throw new IllegalStateException("Worker pause capacity exceeded");
        container.pause();
        var ticket = new Pause();
        ticket.reason = reason;
        pending.put(container, ticket);
        try {
            ticket.future = scheduler.schedule(() -> resume(container, ticket), delayMs, TimeUnit.MILLISECONDS);
        } catch (RuntimeException failure) {
            pending.remove(container);
            container.resume();
            throw failure;
        }
        registry.counter("notification.worker." + reason + ".pauses").increment();
    }

    private synchronized void resume(MessageListenerContainer container, Pause ticket) {
        if (pending.get(container) != ticket) return; // A cancelled task cannot release a newer pause.
        pending.remove(container);
        if (!stopping && !admission.isStopping()) {
            container.resume(); // Container API only; never access KafkaConsumer from this scheduler.
            registry.counter("notification.worker." + ticket.reason + ".resumes").increment();
        }
    }

    public synchronized int pendingCount() { return pending.size(); }

    public synchronized int transientRecordCount() { return transientRetries.size(); }

    private synchronized int pendingCount(String reason) {
        return (int) pending.values().stream().filter(ticket -> ticket.reason.equals(reason)).count();
    }

    private void ensureRunning() {
        if (admission.isStopping() || stopping) {
            throw new IllegalStateException("Worker is shutting down; record remains uncommitted");
        }
    }

    @Override
    public void handleRemaining(Exception failure, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
                                MessageListenerContainer container) {
        ensureRunning();
        super.handleRemaining(failure, records, consumer, container);
    }

    @Override
    public void handleOtherException(Exception failure, Consumer<?, ?> consumer,
                                     MessageListenerContainer container, boolean batchListener) {
        ensureRunning();
        super.handleOtherException(failure, consumer, container, batchListener);
    }

    @EventListener
    public synchronized void consumerStopped(ConsumerStoppedEvent event) {
        var container = event.getContainer(MessageListenerContainer.class);
        transientRetries.keySet().removeIf(key -> key.container() == container);
        var ticket = pending.remove(container);
        if (ticket != null) {
            ticket.future.cancel(false);
            container.resume(); // Clear the pause flag for an explicit restart, without starting the container.
        }
    }

    @EventListener(ContextClosedEvent.class)
    @Order(2)
    @PreDestroy
    public synchronized void close() {
        stopping = true;
        pending.forEach((container, ticket) -> ticket.future.cancel(false));
        pending.clear();
        transientRetries.clear();
        scheduler.shutdownNow();
    }
}
