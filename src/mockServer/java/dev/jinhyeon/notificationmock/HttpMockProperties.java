package dev.jinhyeon.notificationmock;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("mock.http")
public record HttpMockProperties(
        @NotNull Mode mode,
        @Min(0) @Max(10000) long responseDelayMs,
        @Min(1) @Max(10000) int requestsPerWindow,
        @Min(100) @Max(60000) long windowMs,
        @Min(1) @Max(8) int maxInFlight,
        @Min(1) @Max(1_100_000) int idempotencyCapacity,
        @NotBlank String idempotencyFile) {
    public enum Mode { SUCCESS, TOO_MANY_REQUESTS, SERVER_ERROR, INVALID_RECIPIENT }
}
