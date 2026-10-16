package dev.jinhyeon.notificationlab.ratelimit;

import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.config.RateLimitProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;

import static dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter.Outcome.ALLOWED;
import static dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter.Outcome.LIMITED;
import static dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter.Outcome.UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeout;

@Testcontainers
class RedisRateLimiterTest {
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4.11-alpine")
            .withExposedPorts(6379)
            .withCommand("redis-server", "--maxmemory", "32mb", "--maxmemory-policy", "noeviction")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    @Test
    void independentClientsShareOneBurstUnderConcurrentAcquisition() throws Exception {
        String key = key();
        var registry = new SimpleMeterRegistry();
        var clients = new ArrayList<RedisRateLimiter>();
        try (Jedis admin = admin(); var executor = Executors.newFixedThreadPool(4)) {
            for (int i = 0; i < 4; i++) clients.add(client(key, 1, 7, registry));
            // Freeze refill via persisted future time, not a test clock in the production script.
            seed(admin, key, 1, 7, 7000, now(admin) + 10000);
            var start = new CountDownLatch(1);
            List<Future<List<RedisRateLimiter.Outcome>>> tasks = new ArrayList<>();
            for (var client : clients) tasks.add(executor.submit(() -> {
                assertThat(start.await(2, TimeUnit.SECONDS)).isTrue();
                var outcomes = new ArrayList<RedisRateLimiter.Outcome>();
                for (int j = 0; j < 10; j++) outcomes.add(client.tryAcquire().outcome());
                return outcomes;
            }));
            start.countDown();
            var results = new ArrayList<RedisRateLimiter.Outcome>();
            for (var task : tasks) results.addAll(task.get(5, TimeUnit.SECONDS));
            assertThat(results.stream().filter(o -> o == ALLOWED).count()).isEqualTo(7);
            assertThat(results.stream().filter(o -> o == LIMITED).count()).isEqualTo(33);
            assertThat(results).doesNotContain(UNAVAILABLE);
            assertThat(admin.hget(key, "credit")).isEqualTo("0");
            assertThat(registry.get("notification.rate.limit.decisions").tag("outcome", "allowed").counter().count()).isEqualTo(7);
            assertThat(registry.get("notification.rate.limit.decisions").tag("outcome", "limited").counter().count()).isEqualTo(33);
            System.out.println("REDIS_QUOTA clients=4 requests=40 allowed=7 limited=33 unavailable=0");
        } finally { clients.forEach(RedisRateLimiter::close); }
    }

    @Test
    void realServerTimeRefillsFractionalCreditAcrossRepeatedDenials() {
        String key = key();
        try (var client = client(key, 2, 1, new SimpleMeterRegistry()); Jedis admin = admin()) {
            assertThat(client.tryAcquire().outcome()).isEqualTo(ALLOWED);
            long started = Long.parseLong(admin.hget(key, "last_ms"));
            var denied = client.tryAcquire();
            assertThat(denied.outcome()).isEqualTo(LIMITED);
            assertThat(denied.retryAfterMs()).isBetween(1L, 500L);
            await().pollInterval(Duration.ofMillis(20)).atMost(Duration.ofSeconds(2))
                    .until(() -> client.tryAcquire().outcome() == ALLOWED);
            assertThat(Long.parseLong(admin.hget(key, "last_ms")) - started).isGreaterThanOrEqualTo(500);
            assertThat(client.tryAcquire().outcome()).isEqualTo(LIMITED);
        }
    }

    @Test
    void concurrentClientsWithLiveClockStayWithinSharedRateAndBurstEnvelope() throws Exception {
        String key = key();
        var clients = new ArrayList<RedisRateLimiter>();
        try (Jedis admin = admin(); var executor = Executors.newFixedThreadPool(4)) {
            for (int i = 0; i < 4; i++) clients.add(client(key, 10, 3, new SimpleMeterRegistry()));
            var start = new CountDownLatch(1);
            var tasks = new ArrayList<Future<Integer>>();
            for (var client : clients) tasks.add(executor.submit(() -> {
                assertThat(start.await(2, TimeUnit.SECONDS)).isTrue();
                int admitted = 0;
                for (int j = 0; j < 20; j++) {
                    var result = client.tryAcquire();
                    assertThat(result.outcome()).isIn(ALLOWED, LIMITED);
                    if (result.outcome() == ALLOWED) admitted++;
                    TimeUnit.MILLISECONDS.sleep(25);
                }
                return admitted;
            }));
            long began = now(admin);
            start.countDown();
            int admitted = 0;
            for (var task : tasks) admitted += task.get(5, TimeUnit.SECONDS);
            long elapsed = now(admin) - began;
            assertThat(admitted).isGreaterThan(3).isLessThanOrEqualTo((int) (3 + elapsed * 10 / 1000));
            System.out.println("REDIS_RATE clients=4 requests=80 rate=10 burst=3 allowed=" + admitted + " elapsedMs=" + elapsed);
        } finally { clients.forEach(RedisRateLimiter::close); }
    }

    @Test
    void refusedConnectionNeverGrantsPermission() throws Exception {
        int port;
        try (var socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            port = socket.getLocalPort();
        }
        try (var client = new RedisRateLimiter(new RateLimitProperties("127.0.0.1", port, key(), 10, 10, 1, 100),
                new SimpleMeterRegistry())) {
            assertTimeout(Duration.ofSeconds(2), () -> assertThat(client.tryAcquire().outcome()).isEqualTo(UNAVAILABLE));
        }
    }

    @Test
    void connectingClientAlsoOccupiesCapacityWithoutQueueingOtherCalls() throws Exception {
        // Accept TCP but deliberately never answer the Redis handshake.
        try (var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
                var client = new RedisRateLimiter(new RateLimitProperties("127.0.0.1", server.getLocalPort(),
                        key(), 10, 10, 1, 1000), new SimpleMeterRegistry());
                var executor = Executors.newSingleThreadExecutor()) {
            server.setSoTimeout(2000);
            var connecting = executor.submit(client::tryAcquire);
            try (var accepted = server.accept()) {
                assertTimeout(Duration.ofMillis(200), () -> assertThat(client.tryAcquire().outcome()).isEqualTo(UNAVAILABLE));
                assertThat(connecting.get(3, TimeUnit.SECONDS).outcome()).isEqualTo(UNAVAILABLE);
            }
        }
    }

    @Test
    void idleRefillIsCappedAndClientRestartDoesNotResetBudget() {
        String key = key();
        try (Jedis admin = admin()) {
            seed(admin, key, 1, 3, 0, now(admin) - 86400000);
            try (var client = client(key, 1, 3, new SimpleMeterRegistry())) {
                for (int i = 0; i < 3; i++) assertThat(client.tryAcquire().outcome()).isEqualTo(ALLOWED);
                assertThat(client.tryAcquire().outcome()).isEqualTo(LIMITED);
            }
            try (var restarted = client(key, 1, 3, new SimpleMeterRegistry())) {
                assertThat(restarted.tryAcquire().outcome()).isEqualTo(LIMITED);
            }
            assertThat(admin.pttl(key)).isEqualTo(-1); // One persistent provider bucket, no premature expiry.
            assertThat(admin.hlen(key)).isEqualTo(5);
        }
    }

    @Test
    void backwardsClockDoesNotMintTokensOrMoveStoredTimeBackwards() {
        String key = key();
        try (Jedis admin = admin(); var client = client(key, 10, 10, new SimpleMeterRegistry())) {
            long future = now(admin) + 10000;
            seed(admin, key, 10, 10, 0, future);
            var denied = client.tryAcquire();
            assertThat(denied.outcome()).isEqualTo(LIMITED);
            assertThat(denied.retryAfterMs()).isBetween(9000L, 10100L);
            assertThat(admin.hget(key, "last_ms")).isEqualTo(Long.toString(future));
            assertThat(admin.hget(key, "credit")).isEqualTo("0");
        }
    }

    @Test
    void conflictingPolicyOrCorruptStateFailsClosedWithoutOverwritingIt() {
        String key = key();
        try (Jedis admin = admin(); var client = client(key, 10, 10, new SimpleMeterRegistry())) {
            for (Map<String, String> state : List.of(
                    Map.of("version", "1", "rate", "20", "burst", "10", "credit", "10000", "last_ms", "1"),
                    Map.of("version", "1", "rate", "10", "burst", "20", "credit", "10000", "last_ms", "1"),
                    Map.of("version", "2", "rate", "10", "burst", "10", "credit", "10000", "last_ms", "1"),
                    Map.of("version", "1", "rate", "10", "burst", "10", "credit", "-1", "last_ms", "1"),
                    Map.of("version", "1", "rate", "10", "burst", "10", "credit", "10001", "last_ms", "1"),
                    Map.of("version", "1", "rate", "10", "burst", "10", "credit", "0.5", "last_ms", "1"),
                    Map.of("version", "1", "rate", "10", "burst", "10", "credit", "0", "last_ms", "bad"),
                    Map.of("version", "1"))) {
                admin.del(key); admin.hset(key, state);
                assertThat(client.tryAcquire().outcome()).isEqualTo(UNAVAILABLE);
                assertThat(admin.hgetAll(key)).isEqualTo(state);
            }
            admin.del(key); admin.set(key, "wrong-type");
            assertThat(client.tryAcquire().outcome()).isEqualTo(UNAVAILABLE);
            assertThat(admin.get(key)).isEqualTo("wrong-type");
        }
    }

    @Test
    void scriptCacheFlushDoesNotResetBudgetOrRequireUnsafeRetry() {
        String key = key();
        try (Jedis admin = admin(); var client = client(key, 1, 1, new SimpleMeterRegistry())) {
            assertThat(client.tryAcquire().outcome()).isEqualTo(ALLOWED);
            admin.scriptFlush();
            assertThat(client.tryAcquire().outcome()).isEqualTo(LIMITED);
        }
    }

    @Test
    void redisPauseTimesOutAndFullPoolRejectsWithoutQueueing() throws Exception {
        String key = key();
        var registry = new SimpleMeterRegistry();
        var settings = new RateLimitProperties("127.0.0.1", REDIS.getMappedPort(6379), key, 1, 1, 1, 250);
        try (Jedis admin = admin(); var client = new RedisRateLimiter(settings, registry);
                var executor = Executors.newSingleThreadExecutor()) {
            assertThat(client.tryAcquire().outcome()).isEqualTo(ALLOWED); // Warm the sole connection.
            admin.clientPause(1500);
            try {
                var pending = executor.submit(client::tryAcquire);
                await().pollInterval(Duration.ofMillis(5)).atMost(Duration.ofMillis(200)).until(() ->
                        registry.get("notification.rate.limit.connections.active").gauge().value() == 1);
                assertTimeout(Duration.ofMillis(200), () -> assertThat(client.tryAcquire().outcome()).isEqualTo(UNAVAILABLE));
                assertThat(pending.get(1, TimeUnit.SECONDS).outcome()).isEqualTo(UNAVAILABLE);
                assertThat(registry.get("notification.rate.limit.decisions").tag("outcome", "unavailable").counter().count()).isEqualTo(2);
            } finally { admin.clientUnpause(); }
            await().atMost(Duration.ofSeconds(3)).until(() -> client.tryAcquire().outcome() == ALLOWED);
        }
    }

    private static RedisRateLimiter client(String key, int rate, int burst, SimpleMeterRegistry registry) {
        return new RedisRateLimiter(new RateLimitProperties("127.0.0.1", REDIS.getMappedPort(6379), key,
                rate, burst, 2, 1000), registry);
    }

    private static Jedis admin() { return new Jedis("127.0.0.1", REDIS.getMappedPort(6379), 3000); }
    private static String key() { return "test:provider:" + UUID.randomUUID(); }
    private static long now(Jedis admin) {
        var time = admin.time();
        return Long.parseLong(time.get(0)) * 1000 + Long.parseLong(time.get(1)) / 1000;
    }
    private static void seed(Jedis admin, String key, int rate, int burst, long credit, long time) {
        admin.hset(key, Map.of("version", "1", "rate", Integer.toString(rate), "burst", Integer.toString(burst),
                "credit", Long.toString(credit), "last_ms", Long.toString(time)));
    }
}
