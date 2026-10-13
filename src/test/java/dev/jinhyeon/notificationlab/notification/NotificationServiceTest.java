package dev.jinhyeon.notificationlab.notification;

import dev.jinhyeon.notificationlab.config.StoreProperties;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import dev.jinhyeon.notificationlab.provider.PushResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NotificationServiceTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-13T00:00:00Z"), ZoneOffset.UTC);
    private final NotificationRequest request = new NotificationRequest("user-1", "title", "body");
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final InMemoryNotificationStore store = new InMemoryNotificationStore(new StoreProperties(10));

    @Test
    void acceptedResultIsStoredAndCounted() {
        var service = service(r -> new PushResult(PushResult.Outcome.ACCEPTED, "mock-id", null));
        var result = service.send(request);
        assertThat(service.get(result.notificationId())).isEqualTo(result);
        assertThat(result.status()).isEqualTo(NotificationStatus.PROVIDER_ACCEPTED);
        assertThat(result.createdAt()).isEqualTo(clock.instant());
        assertThat(result.completedAt()).isEqualTo(clock.instant());
        assertThat(registry.get("notification.provider.attempts").counter().count()).isEqualTo(1);
        assertThat(registry.get("notification.completed").tag("status", "PROVIDER_ACCEPTED")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("notification.processing").timer().count()).isEqualTo(1);
    }

    @Test
    void retryableFailureDoesNotTriggerAnImplicitRetry() {
        var attempts = new AtomicInteger();
        var service = service(r -> {
            attempts.incrementAndGet();
            return new PushResult(PushResult.Outcome.RETRYABLE_FAILURE, null, "UNAVAILABLE");
        });
        var result = service.send(request);
        assertThat(result.status()).isEqualTo(NotificationStatus.RETRYABLE_FAILURE);
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void unexpectedProviderFailureIsNotLeftProcessingOrMarkedAccepted() {
        var result = service(r -> { throw new IllegalStateException("sensitive internal text"); }).send(request);
        assertThat(result.status()).isEqualTo(NotificationStatus.INTERNAL_FAILURE);
        assertThat(result.errorCode()).isEqualTo("INTERNAL_PROVIDER_ERROR");
        assertThat(result.providerMessageId()).isNull();
    }

    @Test
    void capacityRejectionHappensBeforeProviderInvocation() {
        var attempts = new AtomicInteger();
        var smallStore = new InMemoryNotificationStore(new StoreProperties(1));
        var service = new NotificationService(smallStore, r -> {
            attempts.incrementAndGet();
            return new PushResult(PushResult.Outcome.ACCEPTED, "mock-id", null);
        }, clock, registry);
        service.send(request);
        assertThatThrownBy(() -> service.send(request)).isInstanceOf(StoreCapacityExceededException.class);
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void baselineDoesNotClaimRequestIdempotency() {
        var service = service(r -> new PushResult(PushResult.Outcome.ACCEPTED, "mock-id", null));
        var first = service.send(request);
        var second = service.send(request);
        assertThat(first.notificationId()).isNotEqualTo(second.notificationId());
    }

    private NotificationService service(PushProvider provider) {
        return new NotificationService(store, provider, clock, registry);
    }
}
