package dev.jinhyeon.notificationlab.ratelimit;

import dev.jinhyeon.notificationlab.config.RateLimitProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.core.io.ClassPathResource;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/** Atomic provider-wide permit acquisition; callers must explicitly handle every outcome. */
public final class RedisRateLimiter implements AutoCloseable {
    public enum Outcome { ALLOWED, LIMITED, UNAVAILABLE }

    // retryAfterMs is advisory, not a reservation. It is meaningful only for LIMITED.
    public record Decision(Outcome outcome, long retryAfterMs) { }

    private static final String SCRIPT = loadScript();
    private final JedisPool pool;
    private final Semaphore connectionSlots;
    private final List<String> keys;
    private final List<String> arguments;
    private final Counter allowed;
    private final Counter limited;
    private final Counter unavailable;

    public RedisRateLimiter(RateLimitProperties settings, MeterRegistry registry) {
        var config = new GenericObjectPoolConfig<Jedis>();
        config.setMaxTotal(settings.maxConnections());
        config.setMaxIdle(settings.maxConnections());
        config.setMinIdle(0);
        config.setBlockWhenExhausted(false);
        pool = new JedisPool(config, settings.host(), settings.port(), settings.timeoutMs());
        connectionSlots = new Semaphore(settings.maxConnections());
        keys = List.of(settings.key());
        arguments = List.of(Integer.toString(settings.ratePerSecond()), Integer.toString(settings.burst()));
        allowed = registry.counter("notification.rate.limit.decisions", "outcome", "allowed");
        limited = registry.counter("notification.rate.limit.decisions", "outcome", "limited");
        unavailable = registry.counter("notification.rate.limit.decisions", "outcome", "unavailable");
        registry.gauge("notification.rate.limit.connections.active", pool, JedisPool::getNumActive);
    }

    public Decision tryAcquire() {
        // Count connection creation too: the pool can contend inside its factory
        // before blockWhenExhausted is checked. Never queue callers at that boundary.
        if (!connectionSlots.tryAcquire()) {
            unavailable.increment();
            return new Decision(Outcome.UNAVAILABLE, 0);
        }
        try (Jedis connection = pool.getResource()) {
            // One EVAL, no automatic retry: a timeout may already have consumed a token.
            // Fixed script/arguments, no per-call script generation or client wall clock.
            var reply = (List<?>) connection.eval(SCRIPT, keys, arguments);
            long admitted = ((Number) reply.get(0)).longValue();
            long retryAfterMs = ((Number) reply.get(1)).longValue();
            if (admitted == 1) {
                allowed.increment();
                return new Decision(Outcome.ALLOWED, 0);
            }
            limited.increment();
            return new Decision(Outcome.LIMITED, retryAfterMs);
        } catch (JedisException | IllegalStateException failure) {
            // Pool full/closed, Redis timeout, incompatible policy or invalid state never grant a permit.
            // Do not expose Redis exception text (which may contain connection/key details).
            unavailable.increment();
            return new Decision(Outcome.UNAVAILABLE, 0);
        } finally {
            connectionSlots.release();
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private static String loadScript() {
        try {
            return new ClassPathResource("redis/provider-token-bucket.lua").getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException failure) {
            throw new UncheckedIOException("Missing provider quota script", failure);
        }
    }
}
