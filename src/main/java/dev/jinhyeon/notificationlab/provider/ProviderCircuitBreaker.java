package dev.jinhyeon.notificationlab.provider;

import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Local, bounded state machine. Permits are acquired before DB claim; no I/O runs under its monitor. */
@Component
@Profile("!memory & !sync")
public class ProviderCircuitBreaker {
    public enum State { CLOSED, OPEN, HALF_OPEN }
    private enum Result { SUCCESS, FAILURE, IGNORED }
    private static final Set<String> TRANSPORT_FAILURES = Set.of("HTTP_UNKNOWN_IO", "HTTP_UNKNOWN_TIMEOUT",
            "HTTP_UNKNOWN_DEADLINE", "HTTP_UNKNOWN_RESPONSE", "HTTP_UNKNOWN_STATUS");
    private final boolean enabled;
    private final int threshold;
    private final long openNanos;
    private final LongSupplier nanoTime;
    private final MeterRegistry registry;
    private State state = State.CLOSED;
    private int failures;
    private long epoch;
    private long openedAt;
    private boolean probeActive;

    @Autowired
    public ProviderCircuitBreaker(@Value("${app.circuit-breaker.enabled:false}") boolean enabled,
                                  @Value("${app.circuit-breaker.failure-threshold:3}") int threshold,
                                  @Value("${app.circuit-breaker.open-ms:5000}") int openMs,
                                  @Value("${app.provider.mode:MOCK}") String mode, MeterRegistry registry) {
        this(enabled, threshold, openMs, System::nanoTime, registry);
        if (enabled && !"HTTP".equalsIgnoreCase(mode)) throw new IllegalArgumentException("Circuit breaker requires HTTP provider");
    }

    ProviderCircuitBreaker(boolean enabled, int threshold, int openMs, LongSupplier nanoTime, MeterRegistry registry) {
        if (threshold < 1 || threshold > 20 || openMs < 100 || openMs > 60000) {
            throw new IllegalArgumentException("Invalid circuit breaker bounds");
        }
        this.enabled = enabled;
        this.threshold = threshold;
        this.openNanos = TimeUnit.MILLISECONDS.toNanos(openMs);
        this.nanoTime = nanoTime;
        this.registry = registry;
        registry.gauge("notification.provider.circuit.state", this, breaker -> breaker.state().ordinal());
        registry.gauge("notification.provider.circuit.failures", this, ProviderCircuitBreaker::failures);
        registry.gauge("notification.provider.circuit.probe.active", this, breaker -> breaker.probeActive() ? 1 : 0);
    }

    public boolean enabled() { return enabled; }
    public synchronized State state() { return state; }
    public synchronized int failures() { return failures; }
    public synchronized boolean probeActive() { return probeActive; }

    public synchronized Permit acquire() {
        if (!enabled) return new Permit(-1, false);
        if (state == State.OPEN) {
            long remaining = openNanos - (nanoTime.getAsLong() - openedAt);
            if (remaining > 0) throw deferred(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining) + 1));
            transition(State.HALF_OPEN);
        }
        if (state == State.HALF_OPEN && probeActive) throw deferred(100);
        boolean probe = state == State.HALF_OPEN;
        if (probe) probeActive = true;
        registry.counter("notification.provider.circuit.decisions", "outcome", "allowed").increment();
        return new Permit(epoch, probe);
    }

    private CircuitDeferred deferred(long delayMs) {
        registry.counter("notification.provider.circuit.decisions", "outcome", "deferred").increment();
        return new CircuitDeferred(delayMs);
    }

    private void transition(State target) {
        state = target;
        epoch++; // Results from calls admitted before a transition cannot change the new generation.
        probeActive = false;
        if (target == State.OPEN) openedAt = nanoTime.getAsLong();
        if (target == State.CLOSED) failures = 0;
        registry.counter("notification.provider.circuit.transitions", "state", target.name()).increment();
    }

    private static Result classify(Notification result) {
        if (result.status() == NotificationStatus.PROVIDER_ACCEPTED) return Result.SUCCESS;
        String code = result.errorCode();
        if (result.status() == NotificationStatus.RETRYABLE_FAILURE && code != null
                && (code.matches("HTTP_STATUS_5[0-9]{2}") || TRANSPORT_FAILURES.contains(code))) return Result.FAILURE;
        // 429, per-request 4xx, local pool/admission/interruption, and internal application errors are not backend health.
        return Result.IGNORED;
    }

    public final class Permit implements AutoCloseable {
        private final long generation;
        private final boolean probe;
        private boolean finished;

        private Permit(long generation, boolean probe) { this.generation = generation; this.probe = probe; }

        public void complete(Notification notification) {
            synchronized (ProviderCircuitBreaker.this) {
                if (finished) return;
                finished = true;
                if (!enabled || generation != epoch) return;
                Result result = classify(notification);
                registry.counter("notification.provider.circuit.results", "outcome", result.name()).increment();
                if (probe) probeActive = false;
                if (result == Result.SUCCESS) {
                    if (probe) transition(State.CLOSED);
                    else failures = 0;
                } else if (result == Result.FAILURE) {
                    failures = Math.min(threshold, failures + 1);
                    if (probe || failures >= threshold) transition(State.OPEN);
                }
            }
        }

        @Override
        public void close() {
            synchronized (ProviderCircuitBreaker.this) {
                if (!finished && probe && generation == epoch) probeActive = false;
                finished = true; // No HTTP result: quota deferral/DB race/failure must not count as provider failure.
            }
        }
    }

    /** Only a safe bounded duration crosses the listener/error-handler boundary. */
    public static final class CircuitDeferred extends RuntimeException {
        private final long delayMs;
        CircuitDeferred(long delayMs) {
            super("Provider circuit does not permit a send; defer unclaimed record");
            this.delayMs = Math.max(100, Math.min(60000, delayMs));
        }
        public long delayMs() { return delayMs; }
    }
}
