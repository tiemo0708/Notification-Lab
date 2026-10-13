package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.config.AsyncProperties;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import dev.jinhyeon.notificationlab.provider.MockPushProvider;
import dev.jinhyeon.notificationlab.provider.PushRequest;
import dev.jinhyeon.notificationlab.provider.PushResult;
import dev.jinhyeon.notificationlab.worker.WorkerStore;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real infrastructure; no skipped Docker tests and no mock Kafka broker. */
@Testcontainers
class AsyncNotificationTest {
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

    private ServletWebServerApplicationContext app;
    private HttpClient http;
    private Admin admin;
    private String topic;
    private String group;
    private static final String PAYLOAD = """
            {"recipientId":"synthetic-user","title":"비동기 테스트","body":"가상 데이터"}
            """;

    @BeforeEach
    void start(TestInfo info) {
        topic = "notification-test-" + UUID.randomUUID();
        group = "worker-test-" + UUID.randomUUID();
        boolean experiment = info.getTags().contains("reconciliation");
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(
                NotificationLabApplication.class, ProbeConfiguration.class).run(
                "--server.port=0", "--spring.profiles.active=async", "--app.mock.delay-ms=0",
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--app.async.worker-enabled=" + experiment,
                "--app.async.relay-initial-delay-ms=" + (experiment ? "0" : "86400000"),
                "--logging.level.org.apache.kafka=WARN", "--logging.level.org.springframework.kafka=WARN");
        jdbc().update("DELETE FROM outbox_event");
        jdbc().update("DELETE FROM notification");
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        admin = Admin.create(java.util.Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    @AfterEach
    void close() {
        if (http != null) http.close();
        if (app != null) app.close();
        if (admin != null) admin.close(Duration.ofSeconds(3));
    }

    @Test
    void acceptanceCommitsBothRowsBefore202WithoutCallingProvider() throws Exception {
        var response = post();
        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(json(response).path("status").asText()).isEqualTo("QUEUED");
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(probe().attempts.get()).isZero();
        assertThat(jdbc().queryForObject("SELECT payload FROM outbox_event", String.class))
                .contains("schemaVersion", "notificationId", "eventId")
                .doesNotContain("synthetic-user", "가상 데이터", "비동기 테스트");
        var get = http.send(HttpRequest.newBuilder(URI.create(base() + response.headers()
                .firstValue("Location").orElseThrow())).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(json(get)).isEqualTo(json(response));
    }

    @Test
    void outboxInsertFailureRollsBackNotificationAndReturns503() throws Exception {
        root("CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox_event FOR EACH ROW "
                + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sensitive-test-data'");
        try {
            var response = post();
            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.body()).doesNotContain("sensitive-test-data", "synthetic-user");
            assertThat(count("notification")).isZero();
            assertThat(count("outbox_event")).isZero();
            assertThat(probe().attempts.get()).isZero();
        } finally {
            root("DROP TRIGGER reject_outbox");
        }
    }

    @Test
    void concurrentClaimsAndExpiredLeasesFenceStalePublisher() throws Exception {
        post();
        var store = app.getBean(OutboxStore.class);
        Callable<java.util.Optional<OutboxStore.Claim>> claim = () -> store.claim(10);
        OutboxStore.Claim first;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var results = executor.invokeAll(List.of(claim, claim));
            var claims = results.stream().map(f -> {
                try { return f.get(); } catch (Exception e) { throw new RuntimeException(e); }
            }).flatMap(java.util.Optional::stream).toList();
            assertThat(claims).hasSize(1);
            first = claims.getFirst();
        }
        expireOutbox();
        var recovered = store.claim(10).orElseThrow();
        assertThat(recovered.attempt()).isEqualTo(2);
        assertThat(recovered.token()).isNotEqualTo(first.token());
        assertThat(store.published(first)).isFalse();
        assertThat(store.published(recovered)).isTrue();
    }

    @Test
    void publishedStateWaitsForFutureAndPublishDoesNotHoldDatabaseTransaction() throws Exception {
        post();
        CompletableFuture<SendResult<String, String>> ack = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String topic, String key, String data) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                calls.incrementAndGet();
                return ack;
            }
        };
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(() -> relay(fake).relay());
            await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() == 1);
            assertThat(outboxStatus()).isEqualTo("PROCESSING");
            ack.complete(null);
            running.get(5, TimeUnit.SECONDS);
            assertThat(outboxStatus()).isEqualTo("PUBLISHED");
        }
    }

    @Test
    void failedPublishBacksOffAndStopsAtAttemptLimitIncludingExpiredFinalLease() throws Exception {
        post();
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String topic, String key, String data) {
                return CompletableFuture.failedFuture(new IllegalStateException("test outage"));
            }
        };
        relay(fake).relay();
        assertThat(outboxStatus()).isEqualTo("PENDING");
        assertThat(jdbc().queryForObject("SELECT available_at > UTC_TIMESTAMP(6) FROM outbox_event", Boolean.class)).isTrue();
        assertThat(app.getBean(OutboxStore.class).claim(10)).isEmpty();
        jdbc().update("UPDATE outbox_event SET attempt_count = 9");
        expireOutbox();
        relay(fake).relay();
        assertThat(outboxStatus()).isEqualTo("FAILED");
        assertThat(jdbc().queryForObject("SELECT attempt_count FROM outbox_event", Integer.class)).isEqualTo(10);
        // Also recover a process that died during its last attempt.
        jdbc().update("UPDATE outbox_event SET status = 'PROCESSING', lease_token = ?", UUID.randomUUID().toString());
        expireOutbox();
        assertThat(app.getBean(OutboxStore.class).claim(10)).isEmpty();
        assertThat(outboxStatus()).isEqualTo("FAILED");
    }

    @Test
    void realKafkaCompletesBeforeCommittingOffsetAndReplayDoesNotCallProviderAgain() throws Exception {
        String id = json(post()).path("notificationId").asText();
        probe().behavior = request -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            assertThat(status(id)).isEqualTo("PROCESSING");
            return accepted(request);
        };
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED"));
        awaitCommitted(1);
        assertThat(probe().attempts.get()).isEqualTo(1);
        // Simulates a relay replay after ACK but before its durable PUBLISHED update.
        String payload = jdbc().queryForObject("SELECT payload FROM outbox_event", String.class);
        kafka().send(topic, id, payload).get(10, TimeUnit.SECONDS);
        awaitCommitted(2);
        assertThat(probe().attempts.get()).isEqualTo(1);
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(registry().get("outbox.unpublished").gauge().value()).isZero();
        assertThat(registry().get("notification.consumer.lag").gauge().value()).isZero();
    }

    @Test
    void completionFailureStopsConsumerWithoutSkippingOffsetAndCanRecoverExpiredLease() throws Exception {
        String id = json(post()).path("notificationId").asText();
        root("CREATE TRIGGER reject_completion BEFORE UPDATE ON notification FOR EACH ROW "
                + "BEGIN IF NEW.completed_at IS NOT NULL THEN SIGNAL SQLSTATE '45000' "
                + "SET MESSAGE_TEXT = 'sensitive-test-data'; END IF; END");
        try {
            startWorker();
            app.getBean(OutboxRelay.class).relay();
            await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
            assertThat(status(id)).isEqualTo("PROCESSING");
            assertThat(committedTotal()).isZero();
        } finally {
            root("DROP TRIGGER reject_completion");
        }
        jdbc().update("UPDATE notification SET worker_lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))");
        startWorker();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED"));
        awaitCommitted(1);
        assertThat(probe().attempts.get()).isEqualTo(2); // Ambiguous provider outcome can be attempted twice.
    }

    @Test
    void staleWorkerCannotOverwriteNewLeaseAndActiveLeaseIsNotAcknowledged() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        UUID eventId = UUID.fromString(jdbc().queryForObject("SELECT event_id FROM outbox_event", String.class));
        var store = app.getBean(WorkerStore.class);
        var first = store.claim(id, eventId, "old-worker").orElseThrow();
        assertThatThrownBy(() -> store.claim(id, eventId, "concurrent-worker")).isInstanceOf(IllegalStateException.class);
        jdbc().update("UPDATE notification SET worker_lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))");
        var second = store.claim(id, eventId, "new-worker").orElseThrow();
        var result = first.completed(NotificationStatus.PROVIDER_ACCEPTED, "mock-result", null, java.time.Instant.now());
        assertThatThrownBy(() -> store.complete(result, "old-worker")).isInstanceOf(IllegalStateException.class);
        store.complete(second.completed(NotificationStatus.PROVIDER_ACCEPTED, "new-result", null,
                java.time.Instant.now()), "new-worker");
        assertThat(store.claim(id, eventId, "replay")).isEmpty();
    }

    @Test
    void providerFailureIsDurableTerminalResultBeforeOffsetCommit() throws Exception {
        probe().behavior = request -> new PushResult(PushResult.Outcome.RETRYABLE_FAILURE, null, "MOCK_UNAVAILABLE");
        String id = json(post()).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("RETRYABLE_FAILURE");
        assertThat(probe().attempts.get()).isEqualTo(1);
    }

    @Test
    void unsupportedEventStopsWithoutCommittingOffset() throws Exception {
        startWorker();
        kafka().send(topic, "invalid-key", "{\"schemaVersion\":99}").get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
        assertThat(committedTotal()).isZero();
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    @Tag("reconciliation")
    void reconcilesTenThousandHttpRequestsThroughRealKafka() throws Exception {
        long started = System.nanoTime();
        // Eight long-lived tasks: no unbounded submission queue of 10,000 requests.
        AtomicInteger acceptedCount = new AtomicInteger();
        Callable<Void> submit = () -> {
            for (int i = 0; i < 1250; i++) {
                assertThat(post().statusCode()).isEqualTo(202);
                acceptedCount.incrementAndGet();
            }
            return null;
        };
        try (var executor = Executors.newFixedThreadPool(8)) {
            var results = executor.invokeAll(java.util.Collections.nCopies(8, submit));
            for (var result : results) result.get();
        }
        await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status = 'PROVIDER_ACCEPTED'", Integer.class))
                    .isEqualTo(10000);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PUBLISHED'", Integer.class))
                    .isEqualTo(10000);
        });
        awaitCommitted(10000);
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(count("notification")).isEqualTo(acceptedCount.get()).isEqualTo(10000);
        assertThat(count("outbox_event")).isEqualTo(10000);
        assertThat(probe().attempts.get()).isEqualTo(10000);
        assertThat(registry().get("outbox.unpublished").gauge().value()).isZero();
        assertThat(registry().get("notification.consumer.lag").gauge().value()).isZero();
        System.out.printf("RECONCILIATION accepted=10000 providerAccepted=10000 failure=0 pending=0 published=10000 lag=0 elapsedSeconds=%.3f%n",
                (System.nanoTime() - started) / 1_000_000_000.0);
    }

    private HttpResponse<String> post() throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications"))
                .timeout(Duration.ofSeconds(20)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(PAYLOAD)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode json(HttpResponse<String> response) throws Exception {
        return app.getBean(ObjectMapper.class).readTree(response.body());
    }

    private String base() { return "http://127.0.0.1:" + app.getWebServer().getPort(); }
    private JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private Probe probe() { return app.getBean(Probe.class); }
    private MeterRegistry registry() { return app.getBean(MeterRegistry.class); }
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> kafka() { return app.getBean(KafkaTemplate.class); }
    private int count(String table) { return jdbc().queryForObject("SELECT COUNT(*) FROM " + table, Integer.class); }
    private String status(String id) { return jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, id); }
    private String outboxStatus() { return jdbc().queryForObject("SELECT status FROM outbox_event", String.class); }
    private void expireOutbox() { jdbc().update("UPDATE outbox_event SET available_at = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))"); }
    private OutboxRelay relay(KafkaTemplate<String, String> template) {
        return new OutboxRelay(app.getBean(OutboxStore.class), template, app.getBean(AsyncProperties.class), registry());
    }
    private org.springframework.kafka.listener.MessageListenerContainer container() {
        return app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
    }
    private void startWorker() {
        container().start();
        await().atMost(Duration.ofSeconds(20)).until(() -> container().getAssignedPartitions() != null
                && container().getAssignedPartitions().size() == 3);
    }
    private long committedTotal() throws Exception {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .entrySet().stream().filter(e -> e.getKey().topic().equals(topic)).mapToLong(e -> e.getValue().offset()).sum();
    }
    private void awaitCommitted(long expected) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(committedTotal()).isEqualTo(expected));
    }
    private static PushResult accepted(PushRequest request) {
        return new PushResult(PushResult.Outcome.ACCEPTED, "mock-" + request.notificationId(), null);
    }
    private void root(String sql) throws Exception {
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
    @TestConfiguration
    static class ProbeConfiguration {
        @Bean @Primary
        Probe providerProbe(MockPushProvider delegate) { return new Probe(delegate); }
    }
    static class Probe implements PushProvider {
        final AtomicInteger attempts = new AtomicInteger();
        volatile Function<PushRequest, PushResult> behavior;
        Probe(MockPushProvider delegate) { behavior = delegate::send; }
        @Override public PushResult send(PushRequest request) {
            attempts.incrementAndGet();
            return behavior.apply(request);
        }
    }
}
