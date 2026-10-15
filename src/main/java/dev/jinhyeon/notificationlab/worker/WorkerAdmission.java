package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.config.WorkerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/** Bounded FIFO admission on the calling consumer thread, before any DB claim. */
@Component
@Profile("!memory & !sync")
public class WorkerAdmission {
    private final WorkerProperties settings;
    private final MeterRegistry registry;
    private final ArrayDeque<Object> queue = new ArrayDeque<>();
    private int active;
    private boolean stopping;

    public WorkerAdmission(WorkerProperties settings, MeterRegistry registry) {
        this.settings = settings;
        this.registry = registry;
        registry.gauge("notification.worker.active", this, WorkerAdmission::active);
        registry.gauge("notification.worker.waiting", this, WorkerAdmission::waiting);
    }

    public void execute(Runnable work) {
        enter();
        try {
            work.run(); // Synchronous: the listener returns only after durable processing completes.
        } finally {
            synchronized (this) {
                active--;
                notifyAll();
            }
        }
    }

    private synchronized void enter() {
        if (stopping) throw rejected("stopping");
        if (Thread.currentThread().isInterrupted()) throw rejected("interrupted");
        if (active < settings.maxActive() && queue.isEmpty()) {
            active++;
            return;
        }
        if (queue.size() >= settings.queueCapacity()) throw rejected("full");
        Object ticket = new Object();
        queue.addLast(ticket);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.queueWaitMs());
        try {
            while (true) {
                if (stopping) throw rejected("stopping");
                if (Thread.currentThread().isInterrupted()) throw rejected("interrupted");
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw rejected("timeout");
                if (queue.peekFirst() == ticket && active < settings.maxActive()) {
                    queue.removeFirst();
                    active++;
                    return;
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw rejected("interrupted");
        } finally {
            queue.remove(ticket);
            notifyAll();
        }
    }

    private IllegalStateException rejected(String reason) {
        registry.counter("notification.worker.admission.rejected", "reason", reason).increment();
        return new IllegalStateException("Worker admission rejected: " + reason);
    }

    public synchronized boolean isStopping() { return stopping; }

    public synchronized int active() { return active; }
    public synchronized int waiting() { return queue.size(); }

    @EventListener(ContextClosedEvent.class)
    public synchronized void stopAdmission() {
        stopping = true;
        notifyAll(); // Wake queued callers without consuming a DB attempt or acknowledging their records.
    }
}
