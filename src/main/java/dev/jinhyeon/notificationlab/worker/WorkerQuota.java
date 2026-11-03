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

/** Quota admission before the DB claim; LIMITED can defer to container pause or bounded comparison wait. */
@Component
@Profile("!memory & !sync")
public class WorkerQuota {
    private final Supplier<RedisRateLimiter.Decision> acquire;
    private final int waitTimeoutMs;
    private final boolean pauseEnabled;
    private final boolean transientRecoveryEnabled;
    private final MeterRegistry registry;
    private final AtomicInteger waiting = new AtomicInteger();
    private volatile boolean stopping;

    @Autowired
    public WorkerQuota(ObjectProvider<RedisRateLimiter> limiter,
                       @Value("${app.rate-limit.wait-timeout-ms:2000}") int waitTimeoutMs,
                       @Value("${app.rate-limit.pause-enabled:true}") boolean pauseEnabled,
                       @Value("${app.rate-limit.transient-recovery-enabled:false}") boolean transientRecoveryEnabled,
                       MeterRegistry registry) {
        this(supplier(limiter.getIfAvailable()), waitTimeoutMs, pauseEnabled, transientRecoveryEnabled, registry);
    }

    WorkerQuota(Supplier<RedisRateLimiter.Decision> acquire, int waitTimeoutMs, MeterRegistry registry) {
        this(acquire, waitTimeoutMs, false, registry);
    }

    WorkerQuota(Supplier<RedisRateLimiter.Decision> acquire, int waitTimeoutMs, boolean pauseEnabled, MeterRegistry registry) {
        this(acquire, waitTimeoutMs, pauseEnabled, false, registry);
    }

    WorkerQuota(Supplier<RedisRateLimiter.Decision> acquire, int waitTimeoutMs, boolean pauseEnabled,
                boolean transientRecoveryEnabled, MeterRegistry registry) {
        if (waitTimeoutMs < 1 || waitTimeoutMs > 5000) throw new IllegalArgumentException("Invalid quota wait bound");
        this.acquire = acquire;
        this.pauseEnabled = pauseEnabled;
        this.transientRecoveryEnabled = transientRecoveryEnabled;
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
        if (pauseEnabled) {
            checkAvailable();
            var decision = acquire.get();
            checkAvailable();
            if (decision.outcome() == RedisRateLimiter.Outcome.UNAVAILABLE) {
                if (transientRecoveryEnabled && decision.failureReason().retryable()) {
                    throw new QuotaUnavailableDeferred(decision.failureReason());
                }
                throw rejected("unavailable");
            }
            if (decision.outcome() == RedisRateLimiter.Outcome.LIMITED) throw new QuotaDeferred(decision.retryAfterMs());
            return;
        }
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

    private void checkAvailable() {
        if (stopping) throw rejected("stopping");
        if (Thread.currentThread().isInterrupted()) throw rejected("interrupted");
    }

    private void check(long deadline) {
        checkAvailable();
        if (System.nanoTime() >= deadline) throw rejected("timeout");
    }

    /** Retain the record for a fresh, bounded permit acquisition; never reuse an uncertain permit. */
    public static final class QuotaUnavailableDeferred extends RuntimeException {
        private final RedisRateLimiter.FailureReason reason;
        QuotaUnavailableDeferred(RedisRateLimiter.FailureReason reason) {
            super("Transient provider quota unavailable"); this.reason = reason;
        }
        public RedisRateLimiter.FailureReason reason() { return reason; }
    }

    /** Contains only a bounded delay, never record content or a low-level exception. */
    public static final class QuotaDeferred extends RuntimeException {
        private final long delayMs;

        QuotaDeferred(long delayMs) {
            super("Provider quota limited; defer unclaimed record");
            this.delayMs = Math.max(100, Math.min(60_000, delayMs));
        }

        public long delayMs() { return delayMs; }
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
