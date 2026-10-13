package dev.jinhyeon.notificationlab.notification;

import dev.jinhyeon.notificationlab.provider.PushProvider;
import dev.jinhyeon.notificationlab.provider.PushRequest;
import dev.jinhyeon.notificationlab.provider.PushResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);
    private final NotificationStore store;
    private final PushProvider provider;
    private final Clock clock;
    private final MeterRegistry registry;

    public NotificationService(NotificationStore store, PushProvider provider,
                               Clock clock, MeterRegistry registry) {
        this.store = store;
        this.provider = provider;
        this.clock = clock;
        this.registry = registry;
    }

    public Notification send(NotificationRequest request) {
        var notification = new Notification(UUID.randomUUID(), request.recipientId(), request.title(),
                request.body(), NotificationStatus.PROCESSING, null, null, clock.instant(), null);
        // Reserve bounded storage before calling the provider. No background work in phase 1.
        store.create(notification);
        Timer.Sample sample = Timer.start(registry);
        registry.counter("notification.provider.attempts").increment();

        Notification completed;
        try {
            PushResult result = provider.send(new PushRequest(notification.notificationId(),
                    request.recipientId(), request.title(), request.body()));
            var status = switch (result.outcome()) {
                case ACCEPTED -> NotificationStatus.PROVIDER_ACCEPTED;
                case RETRYABLE_FAILURE -> NotificationStatus.RETRYABLE_FAILURE;
                case PERMANENT_FAILURE -> NotificationStatus.PERMANENT_FAILURE;
            };
            completed = notification.completed(status, result.providerMessageId(),
                    result.errorCode(), clock.instant());
        } catch (RuntimeException exception) {
            // Do not log payload, recipient, tokens or exception messages that may contain them.
            log.warn("Mock attempt failed: notificationId={}, exceptionType={}",
                    notification.notificationId(), exception.getClass().getSimpleName());
            completed = notification.completed(NotificationStatus.INTERNAL_FAILURE,
                    null, "INTERNAL_PROVIDER_ERROR", clock.instant());
        }
        store.update(completed);
        String status = completed.status().name();
        registry.counter("notification.completed", "status", status).increment();
        sample.stop(Timer.builder("notification.processing")
                .tag("status", status).publishPercentileHistogram().register(registry));
        return completed;
    }

    public Notification get(UUID id) {
        return store.find(id).orElseThrow(() -> new NotificationNotFoundException(id));
    }
}
