package dev.jinhyeon.notificationlab.config;

import dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RateLimitConfigurationTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(RateLimitConfiguration.class)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);

    @Test
    void defaultDoesNotCreateRedisClient() {
        context.run(app -> assertThat(app).doesNotHaveBean(RedisRateLimiter.class));
    }

    @Test
    void enabledClientBindsDefaultsWithoutConnectingAndClosesWithContext() {
        RedisRateLimiter[] instance = new RedisRateLimiter[1];
        context.withPropertyValues("app.rate-limit.client-enabled=true").run(app -> {
            assertThat(app).hasSingleBean(RedisRateLimiter.class);
            instance[0] = app.getBean(RedisRateLimiter.class);
            assertThat(app.getBean(RateLimitProperties.class))
                    .isEqualTo(new RateLimitProperties("127.0.0.1", 16379,
                            "notification-lab:provider:mock:quota", 10, 10, 2, 250));
        });
        assertThat(instance[0].tryAcquire().outcome()).isEqualTo(RedisRateLimiter.Outcome.UNAVAILABLE);
    }

    @Test
    void invalidEnabledConfigurationFailsStartup() {
        for (String property : new String[]{"rate-per-second=0", "rate-per-second=10001", "burst=0", "burst=10001",
                "port=0", "port=65536", "host=redis.example.com", "key=", "key=bad key", "max-connections=0",
                "max-connections=5", "timeout-ms=0", "timeout-ms=1001"}) {
            context.withPropertyValues("app.rate-limit.client-enabled=true", "app.rate-limit." + property)
                    .run(app -> assertThat(app).hasFailed());
        }
        assertThatThrownBy(() -> new RateLimitProperties("127.0.0.1", 16379, "k".repeat(129), 10, 10, 2, 250))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
