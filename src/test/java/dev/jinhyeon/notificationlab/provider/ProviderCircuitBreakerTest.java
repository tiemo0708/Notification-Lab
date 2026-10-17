package dev.jinhyeon.notificationlab.provider;

import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static dev.jinhyeon.notificationlab.provider.ProviderCircuitBreaker.State.CLOSED;
import static dev.jinhyeon.notificationlab.provider.ProviderCircuitBreaker.State.OPEN;
import static dev.jinhyeon.notificationlab.provider.ProviderCircuitBreaker.State.HALF_OPEN;

class ProviderCircuitBreakerTest {
    private final AtomicLong clock = new AtomicLong();
    private final ProviderCircuitBreaker breaker = new ProviderCircuitBreaker(true, 2, 1000, clock::get, new SimpleMeterRegistry());

    @Test
    void acceptedResultResetsConsecutiveObservedFailures() {
        complete("HTTP_STATUS_503");
        complete(null);
        complete("HTTP_STATUS_500");
        assertThat(breaker.state()).isEqualTo(CLOSED);
        assertThat(breaker.failures()).isEqualTo(1);
        complete("HTTP_UNKNOWN_TIMEOUT");
        assertThat(breaker.state()).isEqualTo(OPEN);
        assertThatThrownBy(breaker::acquire).isInstanceOf(ProviderCircuitBreaker.CircuitDeferred.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HTTP_STATUS_429", "HTTP_STATUS_400", "HTTP_POOL_TIMEOUT", "HTTP_CLIENT_BUSY",
            "HTTP_CLIENT_CLOSED", "HTTP_INTERRUPTED", "INTERNAL_PROVIDER_ERROR"})
    void localOrPerRequestFailuresDoNotCountAsProviderOutage(String code) {
        complete("HTTP_STATUS_503");
        complete(code);
        assertThat(breaker.failures()).isEqualTo(1);
        assertThat(breaker.state()).isEqualTo(CLOSED);
    }

    @ParameterizedTest
    @ValueSource(strings = {"HTTP_STATUS_500", "HTTP_STATUS_599", "HTTP_UNKNOWN_IO", "HTTP_UNKNOWN_TIMEOUT",
            "HTTP_UNKNOWN_DEADLINE", "HTTP_UNKNOWN_RESPONSE", "HTTP_UNKNOWN_STATUS"})
    void backendFailuresOpenCircuit(String code) {
        complete(code); complete(code);
        assertThat(breaker.state()).isEqualTo(OPEN);
    }

    @Test
    void cooldownAllowsExactlyOneConcurrentProbeAndSuccessCloses() throws Exception {
        open();
        clock.set(TimeUnit.MILLISECONDS.toNanos(999));
        assertThatThrownBy(breaker::acquire).isInstanceOf(ProviderCircuitBreaker.CircuitDeferred.class);
        clock.set(TimeUnit.SECONDS.toNanos(1));
        try (var probe = breaker.acquire(); var executor = Executors.newFixedThreadPool(3)) {
            for (int i = 0; i < 3; i++) {
                executor.submit(() -> assertThatThrownBy(breaker::acquire)
                        .isInstanceOf(ProviderCircuitBreaker.CircuitDeferred.class)).get(2, TimeUnit.SECONDS);
            }
            assertThat(breaker.state()).isEqualTo(HALF_OPEN);
            assertThat(breaker.probeActive()).isTrue();
            // No timeout releases a still-running probe and admits a second HTTP call.
            clock.addAndGet(TimeUnit.MINUTES.toNanos(1));
            assertThatThrownBy(breaker::acquire).isInstanceOf(ProviderCircuitBreaker.CircuitDeferred.class);
            probe.complete(result(null));
        }
        assertThat(breaker.state()).isEqualTo(CLOSED);
        assertThat(breaker.failures()).isZero();
        assertThat(breaker.probeActive()).isFalse();
    }

    @Test
    void failedProbeStartsNewCooldownAndStaleResultsCannotChangeNewState() {
        var oldSuccess = breaker.acquire(); var oldFailure = breaker.acquire();
        open();
        oldSuccess.complete(result(null));
        assertThat(breaker.state()).isEqualTo(OPEN);
        clock.set(TimeUnit.SECONDS.toNanos(1));
        try (var probe = breaker.acquire()) { probe.complete(result("HTTP_STATUS_503")); }
        assertThat(breaker.state()).isEqualTo(OPEN);
        clock.set(TimeUnit.MILLISECONDS.toNanos(1999));
        assertThatThrownBy(breaker::acquire).isInstanceOf(ProviderCircuitBreaker.CircuitDeferred.class);
        clock.set(TimeUnit.SECONDS.toNanos(2));
        complete(null);
        oldFailure.complete(result("HTTP_STATUS_500"));
        assertThat(breaker.state()).isEqualTo(CLOSED);
        assertThat(breaker.failures()).isZero();
    }

    @Test
    void unusedAndIgnoredProbesReleaseSlotWithoutPretendingRecovery() {
        open(); clock.set(TimeUnit.SECONDS.toNanos(1));
        var cancelled = breaker.acquire(); cancelled.close(); cancelled.complete(result(null));
        assertThat(breaker.state()).isEqualTo(HALF_OPEN);
        assertThat(breaker.probeActive()).isFalse();
        complete("HTTP_STATUS_429");
        assertThat(breaker.state()).isEqualTo(HALF_OPEN);
        assertThat(breaker.probeActive()).isFalse();
        complete(null);
        assertThat(breaker.state()).isEqualTo(CLOSED);
    }

    @Test
    void permitCannotReportOrReleaseTwice() {
        var permit = breaker.acquire();
        permit.complete(result("HTTP_STATUS_500"));
        permit.complete(result("HTTP_STATUS_500")); permit.close();
        assertThat(breaker.failures()).isEqualTo(1);
        open(); clock.set(TimeUnit.SECONDS.toNanos(1));
        var cancelled = breaker.acquire(); cancelled.close();
        try (var next = breaker.acquire()) {
            cancelled.close();
            assertThat(breaker.probeActive()).isTrue();
            next.complete(result(null));
        }
    }

    @Test
    void disabledCircuitDoesNotBlockOrRecordFailures() {
        var disabled = new ProviderCircuitBreaker(false, 1, 100, clock::get, new SimpleMeterRegistry());
        for (int i = 0; i < 4; i++) try (var permit = disabled.acquire()) { permit.complete(result("HTTP_STATUS_503")); }
        assertThat(disabled.state()).isEqualTo(CLOSED);
        assertThat(disabled.failures()).isZero();
    }

    @Test
    void invalidBoundsAndNonHttpProviderAreRejected() {
        assertThatThrownBy(() -> new ProviderCircuitBreaker(true, 0, 100, clock::get, new SimpleMeterRegistry())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProviderCircuitBreaker(true, 21, 100, clock::get, new SimpleMeterRegistry())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProviderCircuitBreaker(true, 1, 99, clock::get, new SimpleMeterRegistry())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProviderCircuitBreaker(true, 1, 60001, clock::get, new SimpleMeterRegistry())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProviderCircuitBreaker(true, 1, 100, "MOCK", new SimpleMeterRegistry())).isInstanceOf(IllegalArgumentException.class);
    }

    private void open() {
        complete("HTTP_STATUS_503");
        if (breaker.state() != OPEN) complete("HTTP_STATUS_503");
        assertThat(breaker.state()).isEqualTo(OPEN);
    }
    private void complete(String code) { try (var permit = breaker.acquire()) { permit.complete(result(code)); } }
    private Notification result(String code) {
        return new Notification(UUID.randomUUID(), "synthetic", "circuit", "test",
                code == null ? NotificationStatus.PROVIDER_ACCEPTED
                        : code.equals("HTTP_STATUS_400") ? NotificationStatus.PERMANENT_FAILURE
                        : code.equals("INTERNAL_PROVIDER_ERROR") ? NotificationStatus.INTERNAL_FAILURE
                        : NotificationStatus.RETRYABLE_FAILURE,
                code == null ? "http-mock-test" : null, code, Instant.EPOCH, Instant.EPOCH);
    }
}
