package dev.jinhyeon.notificationlab.config;

import dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.rate-limit.client-enabled", havingValue = "true")
@EnableConfigurationProperties({RateLimitProperties.class, TrafficProperties.class})
public class RateLimitConfiguration {
    // The async Worker uses this shared client for initial sends and retries when enabled.
    @Bean(destroyMethod = "close")
    RedisRateLimiter redisRateLimiter(RateLimitProperties settings, TrafficProperties traffic, MeterRegistry registry) {
        return new RedisRateLimiter(settings, traffic, registry);
    }
}
