package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {
    @Test
    void onlyExplicitTransientFailuresHaveRemainingRetryBudget() {
        var policy = new RetryPolicy(new DeliveryProperties(3, 1000, 60000));
        for (var status : NotificationStatus.values()) {
            assertThat(policy.shouldRetry(status, 1, 3)).isEqualTo(status == NotificationStatus.RETRYABLE_FAILURE);
            assertThat(policy.shouldRetry(status, 3, 3)).isFalse();
        }
        assertThat(policy.shouldRetry(NotificationStatus.RETRYABLE_FAILURE, 1, 1)).isFalse();
    }

    @Test
    void exponentialEqualJitterHasPositiveBoundsAndNeverExceedsCap() {
        var properties = new DeliveryProperties(10, 1000, 60000);
        var lower = new RetryPolicy(properties, (origin, bound) -> origin);
        var upper = new RetryPolicy(properties, (origin, bound) -> bound - 1);
        assertThat(lower.delayMillis(1)).isEqualTo(500);
        assertThat(upper.delayMillis(1)).isEqualTo(1000);
        assertThat(lower.delayMillis(2)).isEqualTo(1000);
        assertThat(upper.delayMillis(2)).isEqualTo(2000);
        assertThat(lower.delayMillis(7)).isEqualTo(30000);
        assertThat(upper.delayMillis(10)).isEqualTo(60000);
        assertThatThrownBy(() -> upper.delayMillis(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> upper.delayMillis(11)).isInstanceOf(IllegalArgumentException.class);
        var random = new RetryPolicy(properties);
        for (int i = 0; i < 100; i++) assertThat(random.delayMillis(10)).isBetween(30000L, 60000L);
    }

    @Test
    void invalidDelayAndAttemptConfigurationIsRejected() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(new DeliveryProperties(3, 1000, 60000))).isEmpty();
            assertThat(validator.validate(new DeliveryProperties(0, 1000, 60000))).isNotEmpty();
            assertThat(validator.validate(new DeliveryProperties(11, 1000, 60000))).isNotEmpty();
            assertThat(validator.validate(new DeliveryProperties(3, 0, 60000))).isNotEmpty();
            assertThat(validator.validate(new DeliveryProperties(3, 1000, 60001))).isNotEmpty();
            assertThat(validator.validate(new DeliveryProperties(3, 2000, 1000))).isNotEmpty();
        }
    }
}
