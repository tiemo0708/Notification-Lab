package dev.jinhyeon.notificationlab.ratelimit;

import dev.jinhyeon.notificationlab.config.RateLimitProperties;
import dev.jinhyeon.notificationlab.config.TrafficProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.core.io.ClassPathResource;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/** Atomic provider-wide permit acquisition; callers must explicitly handle every outcome. */
public final class RedisRateLimiter implements AutoCloseable {
    public enum Outcome { ALLOWED, LIMITED, UNAVAILABLE }

    // retryAfterMs is advisory, not a reservation. It is meaningful only for LIMITED.
    public enum FailureReason {
        NONE, SLOT_BUSY, TIMEOUT, CONNECTION, POOL_CLOSED, STATE_ERROR, UNKNOWN;
        public boolean retryable() { return this == SLOT_BUSY || this == TIMEOUT || this == CONNECTION; }
    }
    private enum Phase { SLOT, ACQUIRE, EVAL, RELEASE }

    public record Decision(Outcome outcome, long retryAfterMs, FailureReason failureReason) {
        public Decision(Outcome outcome, long retryAfterMs) {
            this(outcome, retryAfterMs, outcome == Outcome.UNAVAILABLE ? FailureReason.UNKNOWN : FailureReason.NONE);
        }
    }

    private static final String SCRIPT = loadScript("redis/provider-token-bucket.lua");
    private static final String SPLIT_SCRIPT = loadScript("redis/provider-split-bucket.lua");
    private final String script;
    private final JedisPool pool;
    private final Semaphore connectionSlots;
    private final List<String> keys;
    private final List<String> arguments;
    private final Counter allowed;
    private final Counter limited;
    private final Counter unavailable;
    private final Map<FailureReason, Counter> failures = new EnumMap<>(FailureReason.class);
    private final AtomicReference<Map<String, Object>> firstUnavailable = new AtomicReference<>();
    private final int maxConnections;
    private final int timeoutMs;

    public RedisRateLimiter(RateLimitProperties settings, MeterRegistry registry) {
        this(settings, new TrafficProperties(TrafficProperties.Lane.REALTIME, false, 2, 2), registry);
    }

    public RedisRateLimiter(RateLimitProperties settings, TrafficProperties traffic, MeterRegistry registry) {
        if (traffic.splitEnabled() && (traffic.campaignRate() >= settings.ratePerSecond()
                || traffic.campaignBurst() >= settings.burst())) {
            throw new IllegalArgumentException("Both traffic roles require a positive reserved rate and burst");
        }
        maxConnections = settings.maxConnections();
        timeoutMs = settings.timeoutMs();
        script = traffic.splitEnabled() ? SPLIT_SCRIPT : SCRIPT;
        var config = new GenericObjectPoolConfig<Jedis>();
        config.setMaxTotal(settings.maxConnections());
        config.setMaxIdle(settings.maxConnections());
        config.setMinIdle(0);
        config.setBlockWhenExhausted(false);
        pool = new JedisPool(config, settings.host(), settings.port(), settings.timeoutMs());
        connectionSlots = new Semaphore(settings.maxConnections());
        keys = List.of(settings.key());
        arguments = traffic.splitEnabled()
                ? List.of(Integer.toString(settings.ratePerSecond()), Integer.toString(settings.burst()),
                    Integer.toString(traffic.campaignRate()), Integer.toString(traffic.campaignBurst()), traffic.lane().name())
                : List.of(Integer.toString(settings.ratePerSecond()), Integer.toString(settings.burst()));
        allowed = registry.counter("notification.rate.limit.decisions", "outcome", "allowed");
        limited = registry.counter("notification.rate.limit.decisions", "outcome", "limited");
        unavailable = registry.counter("notification.rate.limit.decisions", "outcome", "unavailable");
        for (var reason : FailureReason.values()) {
            if (reason != FailureReason.NONE) failures.put(reason,
                    registry.counter("notification.rate.limit.unavailable", "reason", reason.name()));
        }
        registry.gauge("notification.rate.limit.connections.active", pool, JedisPool::getNumActive);
    }

    public Decision tryAcquire() {
        long start = System.nanoTime();
        if (!connectionSlots.tryAcquire()) return unavailable(FailureReason.SLOT_BUSY, Phase.SLOT, start);
        Phase phase = Phase.ACQUIRE;
        Decision decision;
        try (Jedis connection = pool.getResource()) {
            phase = Phase.EVAL;
            // Exactly one command per call. Uncertain permits are discarded, never refunded.
            var reply = (List<?>) connection.eval(script, keys, arguments);
            long admitted = ((Number) reply.get(0)).longValue();
            long retryAfterMs = ((Number) reply.get(1)).longValue();
            decision = new Decision(admitted == 1 ? Outcome.ALLOWED : Outcome.LIMITED, retryAfterMs);
            phase = Phase.RELEASE;
        } catch (JedisException | IllegalStateException failure) {
            return unavailable(classify(failure), phase, start);
        } finally {
            connectionSlots.release();
        }
        // A close failure must not also count as a returned permit.
        if (decision.outcome() == Outcome.ALLOWED) allowed.increment(); else limited.increment();
        return decision;
    }

    private FailureReason classify(RuntimeException failure) {
        if (pool.isClosed()) return FailureReason.POOL_CLOSED;
        if (failure instanceof JedisDataException) return FailureReason.STATE_ERROR;
        boolean connection = false;
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 8; depth++, cause = cause.getCause()) {
            if (cause instanceof JedisDataException) return FailureReason.STATE_ERROR;
            if (cause instanceof SocketTimeoutException) return FailureReason.TIMEOUT;
            if (cause instanceof JedisConnectionException) connection = true;
        }
        return connection ? FailureReason.CONNECTION : FailureReason.UNKNOWN;
    }

    private Decision unavailable(FailureReason reason, Phase phase, long start) {
        unavailable.increment(); failures.get(reason).increment();
        if (firstUnavailable.get() == null) {
            var observation = new LinkedHashMap<String, Object>();
            observation.put("observedAtUtc", Instant.now().toString());
            observation.put("reason", reason.name()); observation.put("phase", phase.name());
            observation.put("elapsedMillis", (System.nanoTime() - start) / 1_000_000.0);
            observation.put("configuredTimeoutMillis", timeoutMs); observation.put("maxConnections", maxConnections);
            observation.put("poolActive", pool.getNumActive()); observation.put("poolIdle", pool.getNumIdle());
            observation.put("occupiedSlots", maxConnections - connectionSlots.availablePermits());
            firstUnavailable.compareAndSet(null, Map.copyOf(observation));
        }
        return new Decision(Outcome.UNAVAILABLE, 0, reason);
    }

    public Map<String, Object> diagnostics() {
        var result = new LinkedHashMap<String, Object>();
        result.put("configuredTimeoutMillis", timeoutMs); result.put("maxConnections", maxConnections);
        result.put("poolClosed", pool.isClosed()); result.put("poolActive", pool.getNumActive());
        result.put("poolIdle", pool.getNumIdle());
        var counts = new LinkedHashMap<String, Object>();
        failures.forEach((reason, counter) -> counts.put(reason.name(), counter.count()));
        result.put("unavailableByReason", Map.copyOf(counts));
        var first = firstUnavailable.get(); result.put("firstUnavailablePresent", first != null);
        if (first != null) result.put("firstUnavailable", first);
        return Map.copyOf(result);
    }

    @Override
    public void close() {
        pool.close();
    }

    private static String loadScript(String path) {
        try {
            return new ClassPathResource(path).getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException failure) {
            throw new UncheckedIOException("Missing provider quota script", failure);
        }
    }
}
