package dev.jinhyeon.notificationlab.config;

import java.net.URI;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.provider")
public record ProviderProperties(Mode mode, Http http) {
    public enum Mode { MOCK, HTTP }

    public ProviderProperties {
        if (mode == null || http == null) throw new IllegalArgumentException("Provider settings required");
    }

    public record Http(URI baseUrl, int maxConnections, int maxPending, int acquireTimeoutMs,
                       int connectTimeoutMs, int responseTimeoutMs, int deadlineMs) {
        public Http {
            // This adapter is only for the local simulator. Literal IP avoids unbounded DNS resolution.
            if (baseUrl == null || !"http".equals(baseUrl.getScheme())
                    || !"127.0.0.1".equals(baseUrl.getHost()) || baseUrl.getPort() < 1 || baseUrl.getPort() > 65535
                    || baseUrl.getRawUserInfo() != null || baseUrl.getRawQuery() != null || baseUrl.getRawFragment() != null
                    || !(baseUrl.getPath().isEmpty() || baseUrl.getPath().equals("/"))) {
                throw new IllegalArgumentException("HTTP Mock requires a loopback HTTP origin with an explicit port");
            }
            if (maxConnections < 1 || maxConnections > 8 || maxPending < 0 || maxPending > 16
                    || acquireTimeoutMs < 1 || acquireTimeoutMs > 1000
                    || connectTimeoutMs < 1 || connectTimeoutMs > 2000
                    || responseTimeoutMs < 1 || responseTimeoutMs > 5000
                    || deadlineMs < 1 || deadlineMs > 10000
                    || deadlineMs < Math.max(acquireTimeoutMs, Math.max(connectTimeoutMs, responseTimeoutMs))) {
                throw new IllegalArgumentException("Invalid bounded HTTP Mock settings");
            }
        }
    }
}
