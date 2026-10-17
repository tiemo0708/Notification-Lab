package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationmock.MockPushEndpoint;
import dev.jinhyeon.notificationmock.MockServerApplication;
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
class WorkerQuotaIntegrationTest {
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
    private ServletWebServerApplicationContext second;
    private ServletWebServerApplicationContext mock;
    private Admin kafka;
    private String url;
    private String topic;
    private String group;
    private String quotaKey;
    private int burst;
    private int waitMs;
    private boolean relay;
    private boolean pausing;
    private int maxPollMs = 60000;

    @BeforeEach
    void isolateDatabaseAndMessaging() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String database = "quota_" + suffix;
        // Each case owns a schema inside this disposable container, never the user's local database.
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) { statement.execute("CREATE DATABASE " + database); }
        url = MYSQL.getJdbcUrl().replace("/notification_lab", "/" + database);
        topic = "quota-" + suffix; group = topic; quotaKey = "test:quota:" + suffix;
        kafka = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    @Test
    void initialAndRetryAttemptsUseSameQuotaThroughRealRelay() throws Exception {
        start("SERVER_ERROR", 1, 5000, true);
        Event event = create();
        await().atMost(Duration.ofSeconds(10)).until(() -> attempts(event) == 1 && waiting(app) == 1);
        assertThat(status(event)).isEqualTo("WAITING_RETRY");
        assertThat(stats().requests()).isEqualTo(1);
        assertThat(allowed(app)).isEqualTo(1);
        // Waiting for quota holds neither the task lock nor a second attempt/lease.
        assertTimeout(Duration.ofMillis(500), () -> jdbc().update(
                "UPDATE delivery_task SET max_attempts = max_attempts WHERE notification_id = ?", event.id()));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(status(event)).isEqualTo("RETRYABLE_FAILURE");
            assertThat(offsets()).isEqualTo(3);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE notification_id = ? AND status <> 'PUBLISHED'", Integer.class, event.id())).isZero();
        });
        assertThat(attempts(event)).isEqualTo(3);
        assertThat(stats().requests()).isEqualTo(3);
        assertThat(allowed(app)).isEqualTo(3);
        assertThat(history(event)).containsExactly("RETRYABLE_FAILURE", "RETRYABLE_FAILURE", "RETRYABLE_FAILURE");
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_DEAD_LETTER'", Integer.class, event.id())).isEqualTo(1);
        System.out.println("WORKER_QUOTA_RETRY permits=3 http=3 attempts=3 offsets=3 dlq=1");
    }

    @Test
    void twoApplicationsShareBudgetAndWaitWithoutClaimsOrOffsets() throws Exception {
        start("SUCCESS", 1, 5000, false);
        second = application();
        await().atMost(Duration.ofSeconds(25)).until(() -> assigned(app) + assigned(second) == 6
                && assigned(app) > 0 && assigned(second) > 0);
        assertThat(container(app).getAssignedPartitions().contains(new TopicPartition(topic, 0)))
                .isNotEqualTo(container(app).getAssignedPartitions().contains(new TopicPartition(topic, 2)));
        Event first = create(); Event other = create();
        freeze(1000);
        publish(first, 0); publish(other, 2);
        await().atMost(Duration.ofSeconds(2)).until(() -> waiting(app) == 1 && waiting(second) == 1);
        assertThat(status(first)).isEqualTo("QUEUED"); assertThat(status(other)).isEqualTo("QUEUED");
        assertThat(attempts(first) + attempts(other)).isZero();
        assertThat(stats().requests()).isZero(); assertThat(offsets()).isZero();
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE worker_token IS NOT NULL", Integer.class)).isZero();
        assertTimeout(Duration.ofMillis(500), () -> jdbc().update("UPDATE delivery_task SET max_attempts = max_attempts"));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(status(first)).withFailMessage(() -> quotaDiagnostics(app) + " / " + quotaDiagnostics(second)).isEqualTo("PROVIDER_ACCEPTED");
            assertThat(status(other)).isEqualTo("PROVIDER_ACCEPTED");
            assertThat(offsets()).isEqualTo(2);
        });
        assertThat(allowed(app)).isEqualTo(1); assertThat(allowed(second)).isEqualTo(1);
        assertThat(stats().requests()).isEqualTo(2);
        assertThat(attempts(first) + attempts(other)).isEqualTo(2);
        System.out.println("WORKER_QUOTA_SHARED contexts=2 burst=1 permits=2 http=2 offsets=2 preClaimAttempts=0");
    }

    @Test
    void quotaTimeoutPreservesPartitionAndReplayNeedsNoRedisPermit() throws Exception {
        start("SUCCESS", 2, 100, false);
        Event first = create(); Event later = create();
        freeze(10000); publish(first, 0); publish(later, 0);
        awaitStopped();
        assertThat(attempts(first) + attempts(later)).isZero();
        assertThat(history(first)).isEmpty(); assertThat(stats().requests()).isZero(); assertThat(offsets()).isZero();
        assertThat(status(first)).isEqualTo("QUEUED"); assertThat(status(later)).isEqualTo("QUEUED");
        assertThat(rejected("timeout")).isEqualTo(1);
        refill(); container(app).start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(offsets()).isEqualTo(2));
        assertThat(attempts(first) + attempts(later)).isEqualTo(2);
        assertThat(allowed(app)).isEqualTo(2);
        try (Jedis redis = redis()) { redis.del(quotaKey); redis.set(quotaKey, "invalid-state"); }
        publish(first, 0);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(offsets()).isEqualTo(3));
        assertThat(allowed(app)).isEqualTo(2); assertThat(stats().requests()).isEqualTo(2);
        assertThat(app.getBean(MeterRegistry.class).get("notification.rate.limit.decisions").tag("outcome", "unavailable").counter().count()).isZero();
    }

    @Test
    void unavailableRedisStopsBeforeClaimAndExplicitRestartRecovers() throws Exception {
        start("SUCCESS", 1, 5000, false);
        Event event = create();
        try (Jedis redis = redis()) {
            redis.clientPause(1500);
            try {
                publish(event, 0);
                awaitStopped();
                assertThat(rejected("unavailable")).isEqualTo(1);
                assertThat(status(event)).isEqualTo("QUEUED");
                assertThat(attempts(event)).isZero(); assertThat(history(event)).isEmpty();
                assertThat(offsets()).isZero(); assertThat(stats().requests()).isZero();
            } finally { redis.clientUnpause(); assertThat(redis.ping()).isEqualTo("PONG"); }
        }
        refill(); container(app).start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(offsets()).isEqualTo(1));
        assertThat(attempts(event)).isEqualTo(1); assertThat(stats().requests()).isEqualTo(1);
    }

    @Test
    void shutdownDuringQuotaWaitLeavesUnclaimedWorkForNextContext() throws Exception {
        start("SUCCESS", 1, 5000, false);
        Event event = create(); freeze(10000); publish(event, 0);
        await().atMost(Duration.ofSeconds(2)).until(() -> waiting(app) == 1);
        assertThat(attempts(event)).isZero();
        app.close(); app = null;
        try (var connection = DriverManager.getConnection(url, "root", MYSQL.getPassword());
             var statement = connection.prepareStatement("SELECT n.status, t.attempt_count FROM notification n JOIN delivery_task t USING(notification_id) WHERE notification_id = ?")) {
            statement.setString(1, event.id());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString(1)).isEqualTo("QUEUED");
                assertThat(result.getInt(2)).isZero();
            }
        }
        assertThat(offsets()).isZero(); assertThat(stats().requests()).isZero();
        // Use a new context to inspect the same database and resume the unchanged offset.
        refill(); app = application();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(offsets()).isEqualTo(1));
        assertThat(attempts(event)).isEqualTo(1);
        assertThat(history(event)).containsExactly("PROVIDER_ACCEPTED");
        assertThat(stats().requests()).isEqualTo(1);
    }

    @Test
    void quotaProbePreservesExpiredAttemptAndExhaustionDoesNotNeedPermit() throws Exception {
        start("SUCCESS", 1, 100, false);
        var store = app.getBean(WorkerStore.class);
        Event recoverable = create(); Event exhausted = create();
        for (var event : List.of(recoverable, exhausted)) store.claim(UUID.fromString(event.id()), eventId(event), event.id());
        jdbc().update("UPDATE notification SET worker_lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))");
        jdbc().update("UPDATE delivery_task SET max_attempts = 1 WHERE notification_id = ?", exhausted.id());
        var probe = store.claim(UUID.fromString(recoverable.id()), eventId(recoverable), "probe", null, false).orElseThrow();
        assertThat(probe.needsPermit()).isTrue();
        assertThat(attempts(recoverable)).isEqualTo(1); assertThat(history(recoverable)).containsExactly("STARTED");
        assertThat(jdbc().queryForObject("SELECT worker_token FROM notification WHERE notification_id = ?", String.class, recoverable.id())).isEqualTo(recoverable.id());
        freeze(10000); publish(exhausted, 0);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(offsets()).isEqualTo(1));
        assertThat(status(exhausted)).isEqualTo("INTERNAL_FAILURE");
        assertThat(history(exhausted)).containsExactly("UNKNOWN");
        assertThat(allowed(app)).isZero(); assertThat(stats().requests()).isZero();
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_DEAD_LETTER'", Integer.class, exhausted.id())).isEqualTo(1);
    }

    @Test
    void pausedConsumerKeepsPollingAndHeartbeatBeyondMaxPollWithoutSkippingOffsets() throws Exception {
        pausing = true; maxPollMs = 5000;
        start("SUCCESS", 2, 100, false);
        Event first = create(); Event later = create();
        freeze(11000); publish(first, 0); publish(later, 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> container(app).isContainerPaused());
        double heartbeats = consumerMetric(app, "heartbeat-total");
        double rebalances = consumerMetric(app, "rebalance-total");
        String member = kafka.describeConsumerGroups(List.of(group)).all().get().get(group).members().iterator().next().consumerId();
        // A single pause lasts longer than max.poll.interval; repeated poll and heartbeats must continue.
        await().during(Duration.ofSeconds(7)).atMost(Duration.ofSeconds(9)).untilAsserted(() -> {
            assertThat(container(app).isContainerPaused()).isTrue();
            assertThat(consumerMetric(app, "last-poll-seconds-ago")).isLessThan(2);
            assertThat(consumerMetric(app, "rebalance-total")).isEqualTo(rebalances);
            assertThat(offsets()).isZero();
            assertThat(attempts(first) + attempts(later)).isZero();
            assertThat(stats().requests()).isZero();
            assertThat(app.getBean(WorkerAdmission.class).active()).isZero();
        });
        assertThat(consumerMetric(app, "heartbeat-total")).isGreaterThan(heartbeats);
        assertThat(kafka.describeConsumerGroups(List.of(group)).all().get().get(group).members().iterator().next().consumerId()).isEqualTo(member);
        assertThat(app.getBean(WorkerPauseErrorHandler.class).pendingCount()).isEqualTo(1);
        assertTimeout(Duration.ofMillis(500), () -> jdbc().update("UPDATE delivery_task SET max_attempts = max_attempts"));
        await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> assertThat(offsets()).isEqualTo(2));
        assertThat(history(first)).containsExactly("PROVIDER_ACCEPTED");
        assertThat(history(later)).containsExactly("PROVIDER_ACCEPTED");
        assertThat(stats().requests()).isEqualTo(2);
        assertThat(app.getBean(WorkerPauseErrorHandler.class).pendingCount()).isZero();
        System.out.println("QUOTA_PAUSE pollIntervalMs=5000 observedPauseSeconds=7 offsetsDuringPause=0 finalOffsets=2 heartbeatAdvanced=true rebalanceUnchanged=true");
    }

    @Test
    void retriesPauseAndResumeAutomaticallyThroughRealRelay() throws Exception {
        pausing = true;
        start("SERVER_ERROR", 1, 100, true);
        Event event = create();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(offsets()).isEqualTo(3));
        assertThat(status(event)).isEqualTo("RETRYABLE_FAILURE");
        assertThat(attempts(event)).isEqualTo(3);
        assertThat(stats().requests()).isEqualTo(3);
        assertThat(allowed(app)).isEqualTo(3);
        assertThat(app.getBean(MeterRegistry.class).get("notification.worker.quota.pauses").counter().count()).isGreaterThanOrEqualTo(1);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE event_type = 'NOTIFICATION_DEAD_LETTER'", Integer.class)).isEqualTo(1);
    }

    @Test
    void redisFailureAfterPauseStopsWithoutClaimAndExplicitRestartReplays() throws Exception {
        pausing = true;
        start("SUCCESS", 1, 100, false);
        Event event = create(); freeze(1500); publish(event, 0);
        await().atMost(Duration.ofSeconds(3)).until(() -> container(app).isContainerPaused());
        try (Jedis redis = redis()) { redis.del(quotaKey); redis.set(quotaKey, "invalid-state"); }
        awaitStopped();
        assertThat(rejected("unavailable")).isEqualTo(1);
        assertThat(attempts(event)).isZero(); assertThat(offsets()).isZero(); assertThat(stats().requests()).isZero();
        assertThat(app.getBean(WorkerPauseErrorHandler.class).pendingCount()).isZero();
        refill(); container(app).start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(offsets()).isEqualTo(1));
        assertThat(history(event)).containsExactly("PROVIDER_ACCEPTED");
    }

    @Test
    void stoppingPausedContainerCancelsTimerAndExplicitRestartDoesNotRemainPaused() throws Exception {
        pausing = true;
        start("SUCCESS", 1, 100, false);
        Event event = create(); freeze(10000); publish(event, 0);
        await().atMost(Duration.ofSeconds(3)).until(() -> container(app).isContainerPaused());
        container(app).stop();
        assertThat(app.getBean(WorkerPauseErrorHandler.class).pendingCount()).isZero();
        assertThat(container(app).isPauseRequested()).isFalse();
        assertThat(attempts(event)).isZero(); assertThat(offsets()).isZero();
        refill(); container(app).start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(offsets()).isEqualTo(1));
        assertThat(stats().requests()).isEqualTo(1);
    }

    @Test
    void closingPausedContextPreservesWorkForNewContext() throws Exception {
        pausing = true;
        start("SUCCESS", 1, 100, false);
        Event event = create(); freeze(10000); publish(event, 0);
        await().atMost(Duration.ofSeconds(3)).until(() -> container(app).isContainerPaused());
        var handler = app.getBean(WorkerPauseErrorHandler.class);
        app.close(); app = null;
        assertThat(handler.pendingCount()).isZero();
        assertThat(offsets()).isZero(); assertThat(stats().requests()).isZero();
        refill(); app = application();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(offsets()).isEqualTo(1));
        assertThat(attempts(event)).isEqualTo(1);
        assertThat(stats().requests()).isEqualTo(1);
    }

    @Test
    void joiningWorkerWhilePausedRebalancesUncommittedRecordsWithoutDuplicateAttempts() throws Exception {
        pausing = true;
        start("SUCCESS", 2, 100, false);
        Event first = create(); Event other = create();
        freeze(14000); publish(first, 0); publish(other, 2);
        await().atMost(Duration.ofSeconds(3)).until(() -> container(app).isContainerPaused());
        second = application();
        await().atMost(Duration.ofSeconds(15)).until(() -> assigned(app) + assigned(second) == 6 && assigned(app) > 0 && assigned(second) > 0);
        await().atMost(Duration.ofSeconds(3)).until(() -> container(second).isContainerPaused());
        assertThat(container(app).isPauseRequested()).isTrue();
        assertThat(offsets()).isZero();
        assertThat(attempts(first) + attempts(other)).isZero();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(offsets()).isEqualTo(2));
        assertThat(history(first)).containsExactly("PROVIDER_ACCEPTED");
        assertThat(history(other)).containsExactly("PROVIDER_ACCEPTED");
        assertThat(allowed(app) + allowed(second)).isEqualTo(2);
        assertThat(stats().requests()).isEqualTo(2);
    }

    private String quotaDiagnostics(ServletWebServerApplicationContext context) {
        return "running=" + container(context).isRunning() + " counters=" + context.getBean(MeterRegistry.class).getMeters().stream()
                .filter(meter -> meter.getId().getName().startsWith("notification.worker.quota")
                        || meter.getId().getName().startsWith("notification.rate.limit"))
                .map(meter -> meter.getId() + "=" + meter.measure()).toList();
    }

    private double consumerMetric(ServletWebServerApplicationContext context, String name) {
        var matches = container(context).metrics().values().stream().flatMap(metrics -> metrics.entrySet().stream())
                .filter(entry -> entry.getKey().name().equals(name)).toList();
        assertThat(matches).as("Kafka metric must exist: " + name).isNotEmpty();
        return matches.stream().mapToDouble(entry -> ((Number) entry.getValue().metricValue()).doubleValue()).sum();
    }

    private void start(String mode, int burst, int waitMs, boolean relay) {
        this.burst = burst; this.waitMs = waitMs; this.relay = relay;
        mock = MockServerApplication.start("--server.port=0", "--mock.http.mode=" + mode,
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        app = application();
        await().atMost(Duration.ofSeconds(20)).until(() -> assigned(app) == 6);
    }

    private ServletWebServerApplicationContext application() {
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class).run(
                "--server.port=0", "--app.provider.mode=HTTP", "--app.async.relay-enabled=" + relay,
                "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort(),
                "--spring.datasource.url=" + url, "--spring.datasource.username=root", "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(), "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--app.rate-limit.client-enabled=true", "--app.rate-limit.pause-enabled=" + pausing,
                "--spring.kafka.consumer.properties.max.poll.interval.ms=" + maxPollMs,
                "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000", "--app.rate-limit.port=" + REDIS.getMappedPort(6379),
                "--app.rate-limit.key=" + quotaKey, "--app.rate-limit.rate-per-second=1", "--app.rate-limit.burst=" + burst,
                "--app.rate-limit.timeout-ms=100", "--app.rate-limit.wait-timeout-ms=" + waitMs,
                "--app.delivery.retry-base-delay-ms=100", "--app.delivery.retry-max-delay-ms=100",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
    }

    private record Event(String id, String payload) { }
    private Event create() throws Exception {
        try (var http = HttpClient.newHttpClient()) {
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.getWebServer().getPort() + "/notifications"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"recipientId\":\"synthetic\",\"title\":\"quota\",\"body\":\"test\"}"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(202);
            String id = new ObjectMapper().readTree(response.body()).path("notificationId").asText();
            return new Event(id, jdbc().queryForObject("SELECT payload FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_REQUESTED'", String.class, id));
        }
    }
    private UUID eventId(Event e) throws Exception { return UUID.fromString(new ObjectMapper().readTree(e.payload()).path("eventId").asText()); }
    @SuppressWarnings("unchecked")
    private void publish(Event e, int partition) throws Exception {
        // Manual publication cases keep Outbox PENDING. They do not pretend an ACK was recorded by Relay.
        app.getBean(KafkaTemplate.class).send(topic, partition, e.id(), e.payload()).get(5, TimeUnit.SECONDS);
    }
    private void freeze(long futureMs) {
        try (Jedis redis = redis()) {
            var time = redis.time(); long now = Long.parseLong(time.get(0)) * 1000 + Long.parseLong(time.get(1)) / 1000;
            redis.hset(quotaKey, Map.of("version", "1", "rate", "1", "burst", Integer.toString(burst), "credit", "0", "last_ms", Long.toString(now + futureMs)));
        }
    }
    private void refill() {
        try (Jedis redis = redis()) {
            redis.del(quotaKey); // Isolated test fixture; production never resets/refunds the bucket.
            redis.hset(quotaKey, Map.of("version", "1", "rate", "1", "burst", Integer.toString(burst), "credit", Integer.toString(burst * 1000), "last_ms", "0"));
        }
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
    private double waiting(ServletWebServerApplicationContext ctx) { return ctx.getBean(MeterRegistry.class).get("notification.worker.quota.waiting").gauge().value(); }
    private double allowed(ServletWebServerApplicationContext ctx) { return ctx.getBean(MeterRegistry.class).get("notification.rate.limit.decisions").tag("outcome", "allowed").counter().count(); }
    private double rejected(String reason) { return app.getBean(MeterRegistry.class).get("notification.worker.quota.rejected").tag("reason", reason).counter().count(); }
    private JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private String status(Event e) { return jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, e.id()); }
    private int attempts(Event e) { return jdbc().queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, e.id()); }
    private List<String> history(Event e) { return jdbc().queryForList("SELECT a.outcome FROM delivery_attempt a JOIN delivery_task t USING(task_id) WHERE t.notification_id = ? ORDER BY attempt_no", String.class, e.id()); }
    private MockPushEndpoint.Stats stats() { return mock.getBean(MockPushEndpoint.class).stats(); }
    @AfterEach
    void close() {
        if (second != null) second.close();
        if (app != null) app.close();
        if (mock != null) mock.close();
        if (kafka != null) kafka.close(Duration.ofSeconds(2));
    }
}
