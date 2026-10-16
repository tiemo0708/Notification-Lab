package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** Bounded admission on the consumer thread, with no DB transaction or worker lease held. */
@Component
@Profile("!memory & !sync")
public class WorkerQuota {
    private final Supplier<RedisRateLimiter.Decision> acquire;
    private final int waitTimeoutMs;
    private final MeterRegistry registry;
    private final AtomicInteger waiting = new AtomicInteger();
    private volatile boolean stopping;

    @Autowired
    public WorkerQuota(ObjectProvider<RedisRateLimiter> limiter,
                       @Value("${app.rate-limit.wait-timeout-ms:2000}") int waitTimeoutMs, MeterRegistry registry) {
        this(supplier(limiter.getIfAvailable()), waitTimeoutMs, registry);
    }

    WorkerQuota(Supplier<RedisRateLimiter.Decision> acquire, int waitTimeoutMs, MeterRegistry registry) {
        if (waitTimeoutMs < 1 || waitTimeoutMs > 5000) throw new IllegalArgumentException("Invalid quota wait bound");
        this.acquire = acquire;
        this.waitTimeoutMs = waitTimeoutMs;
        this.registry = registry;
        registry.gauge("notification.worker.quota.waiting", waiting);
    }

    private static Supplier<RedisRateLimiter.Decision> supplier(RedisRateLimiter limiter) {
        return limiter == null ? null : limiter::tryAcquire;
    }

    public boolean enabled() { return acquire != null; }

    public void awaitPermit() {
        if (!enabled()) return;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitTimeoutMs);
        waiting.incrementAndGet();
        try {
            while (true) {
                check(deadline);
                var decision = acquire.get(); // Outside DB transactions and the shutdown monitor.
                check(deadline); // A late/uncertain permit is discarded, never refunded or sent.
                if (decision.outcome() == RedisRateLimiter.Outcome.ALLOWED) return;
                if (decision.outcome() == RedisRateLimiter.Outcome.UNAVAILABLE) throw rejected("unavailable");
                long delay = TimeUnit.MILLISECONDS.toNanos(Math.max(25, Math.min(waitTimeoutMs, decision.retryAfterMs())));
                synchronized (this) {
                    check(deadline);
                    TimeUnit.NANOSECONDS.timedWait(this, Math.min(delay, deadline - System.nanoTime()));
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw rejected("interrupted");
        } finally { waiting.decrementAndGet(); }
    }

    private void check(long deadline) {
        if (stopping) throw rejected("stopping");
        if (Thread.currentThread().isInterrupted()) throw rejected("interrupted");
        if (System.nanoTime() >= deadline) throw rejected("timeout");
    }

    private IllegalStateException rejected(String reason) {
        registry.counter("notification.worker.quota.rejected", "reason", reason).increment();
        return new IllegalStateException("Worker quota rejected: " + reason);
    }

    @EventListener(ContextClosedEvent.class)
    @Order(1) // WorkerAdmission must mark shutdown before quota waiters can fail the listener.
    public synchronized void stopAdmission() {
        stopping = true;
        notifyAll();
    }
}
