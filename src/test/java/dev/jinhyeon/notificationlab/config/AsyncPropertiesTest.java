package dev.jinhyeon.notificationlab.config;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncPropertiesTest {
    @Test
    void submissionWindowDefaultsToSerialAndBindsIndependentlyOfCompletionBatch() {
        var runner = new ApplicationContextRunner().withUserConfiguration(RelayConfiguration.class)
                .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class));
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(RelayProperties.class)).isEqualTo(new RelayProperties(1, 1));
        });
        runner.withPropertyValues("app.async.relay.publish-batch-size=1", "app.async.relay.submission-window-size=10")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RelayProperties.class)).isEqualTo(new RelayProperties(1, 10));
                });
        for (int invalid : new int[]{0, 11}) {
            runner.withPropertyValues("app.async.relay.submission-window-size=" + invalid)
                    .run(context -> assertThat(context).hasFailed());
        }
    }

    // Imported explicitly by this runner; no component stereotype for unrelated app scans.
    @EnableConfigurationProperties(RelayProperties.class)
    static class RelayConfiguration { }

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
