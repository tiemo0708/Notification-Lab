package dev.jinhyeon.notificationlab.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.mock")
public record MockProperties(
        @Min(0) @Max(5000) long delayMs,
        @NotNull Outcome outcome) {
    public enum Outcome {
        SUCCESS, TRANSIENT_FAILURE, PERMANENT_FAILURE
    }
}
