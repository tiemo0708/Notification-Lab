package dev.jinhyeon.notificationlab.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.delivery")
public record DeliveryProperties(
        @Min(1) @Max(10) int maxAttempts,
        @Min(100) @Max(60000) long retryBaseDelayMs,
        @Min(100) @Max(60000) long retryMaxDelayMs) {

    @AssertTrue(message = "retry-max-delay-ms must be at least retry-base-delay-ms")
    public boolean isRetryDelayRangeValid() {
        return retryMaxDelayMs >= retryBaseDelayMs;
    }
}
