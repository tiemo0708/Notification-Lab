package dev.jinhyeon.notificationlab.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.delivery")
public record DeliveryProperties(@Min(1) @Max(10) int maxAttempts) { }
