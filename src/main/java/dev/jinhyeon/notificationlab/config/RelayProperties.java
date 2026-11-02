package dev.jinhyeon.notificationlab.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.async.relay")
public record RelayProperties(@DefaultValue("1") @Min(1) @Max(10) int publishBatchSize) { }
