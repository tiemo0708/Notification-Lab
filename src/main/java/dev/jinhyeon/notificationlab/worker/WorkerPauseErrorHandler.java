package dev.jinhyeon.notificationlab.worker;

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

/** Retain failed records in Spring's bounded poll buffer; only normal quota shortage is resumable. */
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

    private static final class Pause {
        private ScheduledFuture<?> future;
    }

    public WorkerPauseErrorHandler(WorkerAdmission admission, MeterRegistry registry) {
        this.admission = admission;
        this.registry = registry;
        scheduler.setRemoveOnCancelPolicy(true);
        registry.gauge("notification.worker.quota.pauses.pending", this, WorkerPauseErrorHandler::pendingCount);
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
            if (cause instanceof WorkerQuota.QuotaDeferred deferred) {
                try {
                    pause(container, deferred.delayMs());
                } catch (RuntimeException unavailable) {
                    handleRemaining(new IllegalStateException("Quota pause unavailable"), List.of(record), consumer, container);
                }
                return false; // Never recover/skip: Spring retains this record and keeps polling while paused.
            }
        }
        handleRemaining(failure, List.of(record), consumer, container);
        return false;
    }

    private synchronized void pause(MessageListenerContainer container, long delayMs) {
        ensureRunning();
        if (pending.containsKey(container)) return;
        // One parent container in this application; keep a hard bound if more listeners are added later.
        if (pending.size() >= 3) throw new IllegalStateException("Quota pause capacity exceeded");
        container.pause();
        var ticket = new Pause();
        pending.put(container, ticket);
        try {
            ticket.future = scheduler.schedule(() -> resume(container, ticket), delayMs, TimeUnit.MILLISECONDS);
        } catch (RuntimeException failure) {
            pending.remove(container);
            container.resume();
            throw failure;
        }
        registry.counter("notification.worker.quota.pauses").increment();
    }

    private synchronized void resume(MessageListenerContainer container, Pause ticket) {
        if (pending.get(container) != ticket) return; // A cancelled task cannot release a newer pause.
        pending.remove(container);
        if (!stopping && !admission.isStopping()) {
            container.resume(); // Container API only; never access KafkaConsumer from this scheduler.
            registry.counter("notification.worker.quota.resumes").increment();
        }
    }

    public synchronized int pendingCount() { return pending.size(); }

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
        scheduler.shutdownNow();
    }
}
