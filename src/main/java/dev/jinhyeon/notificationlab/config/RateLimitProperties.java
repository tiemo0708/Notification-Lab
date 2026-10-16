package dev.jinhyeon.notificationlab.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("app.rate-limit")
public record RateLimitProperties(
        @DefaultValue("127.0.0.1") String host,
        @DefaultValue("16379") int port,
        @DefaultValue("notification-lab:provider:mock:quota") String key,
        @DefaultValue("10") int ratePerSecond,
        @DefaultValue("10") int burst,
        @DefaultValue("2") int maxConnections,
        @DefaultValue("250") int timeoutMs) {
    public RateLimitProperties {
        // Local simulator only. One stable provider key, never a recipient/request key.
        if (!"127.0.0.1".equals(host) || port < 1 || port > 65535
                || key == null || !key.matches("[a-zA-Z0-9:_-]{1,128}")
                || ratePerSecond < 1 || ratePerSecond > 10000 || burst < 1 || burst > 10000
                || maxConnections < 1 || maxConnections > 4 || timeoutMs < 1 || timeoutMs > 1000) {
            throw new IllegalArgumentException("Invalid bounded local Redis rate limit settings");
        }
    }
}
