package dev.jinhyeon.notificationlab.config;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncPropertiesTest {
    @Test
    void publishBatchSizeIsBoundedToLimitUnmarkedAcknowledgements() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(new RelayProperties(1))).isEmpty();
            assertThat(validator.validate(new RelayProperties(10))).isEmpty();
            assertThat(validator.validate(new RelayProperties(0))).isNotEmpty();
            assertThat(validator.validate(new RelayProperties(11))).isNotEmpty();
        }
    }

    @Test
    void topicCollisionsAreRejectedBeforeDlqCanReachAWorker() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(new AsyncProperties("requests", "retry", "dlq", "worker", 10))).isEmpty();
            assertThat(validator.validate(new AsyncProperties("requests", "requests", "dlq", "worker", 10))).isNotEmpty();
            assertThat(validator.validate(new AsyncProperties("requests", "retry", "requests", "worker", 10))).isNotEmpty();
            assertThat(validator.validate(new AsyncProperties("requests", "retry", "retry", "worker", 10))).isNotEmpty();
            assertThat(validator.validate(new AsyncProperties("requests", "retry", " ", "worker", 10))).isNotEmpty();
        }
        var properties = new AsyncProperties("requests", "retry", "dlq", "worker", 10);
        assertThatThrownBy(() -> properties.destination("UNKNOWN")).isInstanceOf(IllegalArgumentException.class);
    }
}
