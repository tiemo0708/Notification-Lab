package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongBinaryOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!memory & !sync")
public class RetryPolicy {
    private final DeliveryProperties properties;
    private final LongBinaryOperator random;

    @Autowired
    public RetryPolicy(DeliveryProperties properties) {
        this(properties, (origin, bound) -> ThreadLocalRandom.current().nextLong(origin, bound));
    }

    RetryPolicy(DeliveryProperties properties, LongBinaryOperator random) {
        this.properties = properties;
        this.random = random;
    }

    public boolean shouldRetry(NotificationStatus status, int attempts, int maxAttempts) {
        return status == NotificationStatus.RETRYABLE_FAILURE && attempts < maxAttempts;
    }

    public long delayMillis(int attempts) {
        if (attempts < 1 || attempts > 10) throw new IllegalArgumentException("Invalid attempt count");
        long ceiling = Math.min(properties.retryMaxDelayMs(), properties.retryBaseDelayMs() * (1L << (attempts - 1)));
        // Equal jitter: a positive wait in [ceil(ceiling / 2), ceiling], never above the cap.
        return random.applyAsLong((ceiling + 1) / 2, ceiling + 1);
    }
}
