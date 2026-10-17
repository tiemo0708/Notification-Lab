package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import com.sun.net.httpserver.HttpServer;
import dev.jinhyeon.notificationlab.provider.ProviderCircuitBreaker;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeout;

@Testcontainers
class WorkerCircuitBreakerIntegrationTest {
    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4.8")
            .withDatabaseName("notification_lab").withUsername("notification_test")
            .withPassword(UUID.randomUUID().toString()).withCommand("--default-time-zone=+00:00")
            .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.2"))
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4.11-alpine").withExposedPorts(6379)
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    private ServletWebServerApplicationContext app;
    private Admin kafka;
    private HttpServer server;
    private ExecutorService serverExecutor;
    private final AtomicInteger httpStatus = new AtomicInteger(500);
    private final AtomicInteger requests = new AtomicInteger();
    private volatile CountDownLatch responseGate = new CountDownLatch(0);
    private String url;
    private String topic;
    private String group;
    private String quotaKey;
    private int threshold;
    private int openMs;
    private int consumers;
    private boolean relay;
    private boolean quotaEnabled = true;

    @BeforeEach
    void isolateDatabaseAndMessaging() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String database = "circuit_" + suffix;
        // Each case owns a schema inside this disposable container, never the user's local database.
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) { statement.execute("CREATE DATABASE " + database); }
        url = MYSQL.getJdbcUrl().replace("/notification_lab", "/" + database);
        topic = "circuit-" + suffix; group = topic; quotaKey = "test:circuit:" + suffix;
        kafka = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }


    @Test
    void realRelayRetriesPauseBeforeClaimAndRecoverThroughSingleProbe() throws Exception {
        start(2, 3000, 1, true);
        Event event = create();
        paused();
        assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.OPEN);
        assertThat(status(event)).isEqualTo("WAITING_RETRY");
        assertThat(attempts(event)).isEqualTo(2);
        assertThat(requests.get()).isEqualTo(2);
        assertThat(allowed(app)).isEqualTo(2);
        assertThat(offsets()).isEqualTo(2);
        assertThat(app.getBean(WorkerAdmission.class).active()).isZero();
        assertTimeout(Duration.ofMillis(500), () -> jdbc().update(
                "UPDATE delivery_task SET max_attempts = max_attempts WHERE notification_id = ?", event.id()));
        httpStatus.set(202);
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertThat(attempts(event)).isEqualTo(2);
            assertThat(requests.get()).isEqualTo(2);
            assertThat(offsets()).isEqualTo(2);
        });
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(status(event)).isEqualTo("PROVIDER_ACCEPTED");
            assertThat(offsets()).isEqualTo(3);
        });
        assertThat(history(event)).containsExactly("RETRYABLE_FAILURE", "RETRYABLE_FAILURE", "PROVIDER_ACCEPTED");
        assertThat(requests.get()).isEqualTo(3);
        assertThat(allowed(app)).isEqualTo(3);
        assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.CLOSED);
        System.out.println("CIRCUIT_RELAY attemptsDuringOpen=2 httpDuringOpen=2 finalAttempts=3 finalOffsets=3 recovered=true");
    }

    @Test
    void concurrentConsumersAdmitOnlyOneHalfOpenProbeAndFailedProbeReopens() throws Exception {
        start(1, 1500, 3, false);
        Event initial = create(); publish(initial, 0);
        await().atMost(Duration.ofSeconds(10)).until(() -> offsets() == 1);
        responseGate = new CountDownLatch(1);
        Event first = create(); Event other = create();
        publish(first, 0); publish(other, 2);
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() == 2 && circuit().probeActive());
            await().during(Duration.ofMillis(400)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(requests.get()).isEqualTo(2);
                assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.HALF_OPEN);
                assertThat(attempts(first) + attempts(other)).isEqualTo(1);
                assertThat(List.of(status(first), status(other))).containsExactlyInAnyOrder("PROCESSING", "QUEUED");
                assertThat(offsets()).isEqualTo(1);
            });
        } finally { responseGate.countDown(); }
        await().atMost(Duration.ofSeconds(5)).until(() -> offsets() == 2 && circuit().state() == ProviderCircuitBreaker.State.OPEN);
        httpStatus.set(202);
        await().during(Duration.ofMillis(400)).atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(requests.get()).isEqualTo(2));
        await().atMost(Duration.ofSeconds(10)).until(() -> offsets() == 3);
        assertThat(requests.get()).isEqualTo(3);
        assertThat(attempts(first) + attempts(other)).isEqualTo(2);
        assertThat(List.of(status(first), status(other))).containsExactlyInAnyOrder("WAITING_RETRY", "PROVIDER_ACCEPTED");
        assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.CLOSED);
        assertThat(circuit().probeActive()).isFalse();
        System.out.println("CIRCUIT_CONCURRENT consumers=3 halfOpenHttp=1 blockedAttempts=0 failedProbeReopened=true");
    }

    @Test
    void completedReplayBypassesOpenCircuitAndRedisFailureReleasesUnusedProbe() throws Exception {
        httpStatus.set(202);
        start(1, 2500, 1, false);
        Event completed = create(); publish(completed, 0);
        await().atMost(Duration.ofSeconds(10)).until(() -> offsets() == 1);
        httpStatus.set(500);
        Event failed = create(); publish(failed, 0);
        await().atMost(Duration.ofSeconds(10)).until(() -> offsets() == 2);
        try (Jedis redis = redis()) { redis.del(quotaKey); redis.set(quotaKey, "invalid-state"); }
        publish(completed, 0);
        await().atMost(Duration.ofSeconds(2)).until(() -> offsets() == 3);
        assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.OPEN);
        assertThat(allowed(app)).isEqualTo(2);
        assertThat(requests.get()).isEqualTo(2);
        Event fresh = create(); publish(fresh, 0);
        awaitStopped(); // Half-open permit acquired, but Redis fails before a DB claim or HTTP request.
        assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.HALF_OPEN);
        assertThat(circuit().probeActive()).isFalse();
        assertThat(status(fresh)).isEqualTo("QUEUED");
        assertThat(attempts(fresh)).isZero();
        assertThat(history(fresh)).isEmpty();
        assertThat(offsets()).isEqualTo(3);
        assertThat(requests.get()).isEqualTo(2);
        try (Jedis redis = redis()) { redis.del(quotaKey); } // Reset only this isolated test fixture.
        seedQuota();
        httpStatus.set(202); container(app).start();
        await().atMost(Duration.ofSeconds(15)).until(() -> offsets() == 4);
        assertThat(history(fresh)).containsExactly("PROVIDER_ACCEPTED");
        assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.CLOSED);
        assertThat(requests.get()).isEqualTo(3);
    }

    @Test
    void shutdownCancelsOpenPauseAndNewContextRecoversUnclaimedWork() throws Exception {
        quotaEnabled = false; // Circuit protection also works independently of the Redis gate.
        start(1, 5000, 1, false);
        Event failed = create(); publish(failed, 0);
        await().atMost(Duration.ofSeconds(10)).until(() -> offsets() == 1);
        Event fresh = create(); publish(fresh, 0); paused();
        assertThat(status(fresh)).isEqualTo("QUEUED");
        assertThat(attempts(fresh)).isZero();
        var handler = app.getBean(WorkerPauseErrorHandler.class);
        app.close(); app = null;
        assertThat(handler.pendingCount()).isZero();
        assertThat(offsets()).isEqualTo(1);
        httpStatus.set(202); app = application();
        await().atMost(Duration.ofSeconds(15)).until(() -> offsets() == 2);
        assertThat(circuit().state()).isEqualTo(ProviderCircuitBreaker.State.CLOSED);
        assertThat(history(fresh)).containsExactly("PROVIDER_ACCEPTED");
        assertThat(requests.get()).isEqualTo(2);
    }
    private void start(int threshold, int openMs, int consumers, boolean relay) throws Exception {
        this.threshold = threshold; this.openMs = openMs; this.consumers = consumers; this.relay = relay;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 8);
        serverExecutor = Executors.newFixedThreadPool(3);
        server.setExecutor(serverExecutor);
        server.createContext("/pushes", exchange -> {
            requests.incrementAndGet();
            int status = httpStatus.get();
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                try {
                    if (!responseGate.await(4, TimeUnit.SECONDS)) throw new java.io.IOException("Test response gate timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new java.io.IOException("Test server interrupted");
                }
                byte[] body = ("{\"providerMessageId\":\"http-mock-" + UUID.randomUUID() + "\"}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
        seedQuota();
        app = application();
        await().atMost(Duration.ofSeconds(20)).until(() -> assigned(app) == 6);
    }

    private ServletWebServerApplicationContext application() {
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class).run(
                "--server.port=0", "--app.provider.mode=HTTP", "--app.async.relay-enabled=" + relay,
                "--app.provider.http.base-url=http://127.0.0.1:" + server.getAddress().getPort(),
                "--app.provider.http.response-timeout-ms=5000", "--app.provider.http.deadline-ms=10000",
                "--spring.datasource.url=" + url, "--spring.datasource.username=root", "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(), "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--app.worker.consumers=" + consumers, "--app.worker.max-active=" + consumers,
                "--app.circuit-breaker.enabled=true", "--app.circuit-breaker.failure-threshold=" + threshold,
                "--app.circuit-breaker.open-ms=" + openMs,
                "--app.rate-limit.client-enabled=" + quotaEnabled, "--app.rate-limit.pause-enabled=true",
                "--app.rate-limit.port=" + REDIS.getMappedPort(6379), "--app.rate-limit.key=" + quotaKey,
                "--app.rate-limit.rate-per-second=10", "--app.rate-limit.burst=10", "--app.rate-limit.timeout-ms=100",
                "--app.delivery.retry-base-delay-ms=100", "--app.delivery.retry-max-delay-ms=100",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
    }

    private void seedQuota() {
        // Isolated fixture: preserve the original circuit scenarios' starting budget.
        try (Jedis redis = redis()) {
            redis.hset(quotaKey, Map.of("version", "2", "rate", "10", "burst", "10", "credit", "10000", "last_ms", "0"));
        }
    }

    private ProviderCircuitBreaker circuit() { return app.getBean(ProviderCircuitBreaker.class); }
    private void paused() {
        await().atMost(Duration.ofSeconds(10)).until(() -> container(app).isContainerPaused());
    }
    @AfterEach
    void close() {
        responseGate.countDown();
        if (app != null) app.close();
        if (server != null) server.stop(0);
        if (serverExecutor != null) serverExecutor.shutdownNow();
        if (kafka != null) kafka.close(Duration.ofSeconds(2));
    }
    private record Event(String id, String payload) { }
    private Event create() throws Exception {
        try (var http = HttpClient.newHttpClient()) {
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.getWebServer().getPort() + "/notifications"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"recipientId\":\"synthetic\",\"title\":\"circuit\",\"body\":\"test\"}"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(202);
            String id = new ObjectMapper().readTree(response.body()).path("notificationId").asText();
            return new Event(id, jdbc().queryForObject("SELECT payload FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_REQUESTED'", String.class, id));
        }
    }
    @SuppressWarnings("unchecked")
    private void publish(Event e, int partition) throws Exception {
        // Manual publication cases keep Outbox PENDING. They do not pretend an ACK was recorded by Relay.
        app.getBean(KafkaTemplate.class).send(topic, partition, e.id(), e.payload()).get(5, TimeUnit.SECONDS);
    }
    private Jedis redis() { return new Jedis("127.0.0.1", REDIS.getMappedPort(6379), 3000); }
    private long offsets() throws Exception {
        return kafka.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .entrySet().stream().filter(e -> e.getKey().topic().equals(topic) || e.getKey().topic().equals(topic + ".retry"))
                .mapToLong(e -> e.getValue().offset()).sum();
    }
    private void awaitStopped() {
        await().atMost(Duration.ofSeconds(15)).until(() -> !container(app).isRunning());
        await().atMost(Duration.ofSeconds(10)).until(() -> app.getBean(WorkerAdmission.class).active() == 0);
        await().atMost(Duration.ofSeconds(10)).until(() -> kafka.describeConsumerGroups(List.of(group)).all()
                .get(5, TimeUnit.SECONDS).get(group).members().isEmpty());
    }
    private int assigned(ServletWebServerApplicationContext ctx) {
        var parts = container(ctx).getAssignedPartitions(); return parts == null ? 0 : parts.size();
    }
    private ConcurrentMessageListenerContainer<?, ?> container(ServletWebServerApplicationContext ctx) {
        return (ConcurrentMessageListenerContainer<?, ?>) ctx.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
    }
    private double allowed(ServletWebServerApplicationContext ctx) { return ctx.getBean(MeterRegistry.class).get("notification.rate.limit.decisions").tag("outcome", "allowed").counter().count(); }
    private JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private String status(Event e) { return jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, e.id()); }
    private int attempts(Event e) { return jdbc().queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, e.id()); }
    private List<String> history(Event e) { return jdbc().queryForList("SELECT a.outcome FROM delivery_attempt a JOIN delivery_task t USING(task_id) WHERE t.notification_id = ? ORDER BY attempt_no", String.class, e.id()); }
}
