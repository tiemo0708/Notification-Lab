package dev.jinhyeon.notificationlab.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.async")
public record AsyncProperties(@NotBlank String topic, @NotBlank String group,
                              @Min(1) @Max(100) int maxPublishAttempts) { }
