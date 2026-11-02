package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlabtest.OffsetCommitPolicy;
import dev.jinhyeon.notificationmock.MockPushEndpoint;
import dev.jinhyeon.notificationmock.MockServerApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.common.TopicPartition;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.sql.DriverManager;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class WorkerConcurrencyTest {
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
    private ServletWebServerApplicationContext mock;

    private Admin admin;
    private String topic;
    private String group;
    private OffsetCommitPolicy.Mode offsetMode = OffsetCommitPolicy.Mode.RECORD;

    @ParameterizedTest
    @EnumSource(OffsetCommitPolicy.Mode.class)
    void twoActiveAndOneWaitingKeepOffsetsAndClaimsBehindCompletedWork(OffsetCommitPolicy.Mode mode) throws Exception {
        offsetMode = mode;
        start(3, 2, 1, 5000, 1500);
        var first = create(); var second = create(); var waiting = create(); var later = create();
        publish(first, 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> stats().active() == 1);
        publish(second, 1);
        await().atMost(Duration.ofSeconds(5)).until(() -> stats().active() == 2);
        publish(waiting, 2); publish(later, 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> gate().waiting() == 1);
        assertThat(gate().active()).isEqualTo(2);
        assertThat(attempts(waiting)).isZero();
        assertThat(attempts(later)).isZero();
        assertThat(status(waiting)).isEqualTo("QUEUED");
        assertThat(offset(0)).isZero(); assertThat(offset(1)).isZero(); assertThat(offset(2)).isZero();
        awaitCompleted(List.of(first, second, waiting, later), 4);
        assertThat(stats().maxActive()).isEqualTo(2);
        assertThat(stats().requests()).isEqualTo(4);
        assertThat(offset(0)).isEqualTo(2); assertThat(offset(1)).isEqualTo(1); assertThat(offset(2)).isEqualTo(1);
        assertThat(gate().active()).isZero(); assertThat(gate().waiting()).isZero();
        System.out.println("WORKER_LIMIT consumers=3 activeCap=2 queueCap=1 peakHttp=2 calls=4 committed=4");
    }

    @ParameterizedTest
    @EnumSource(OffsetCommitPolicy.Mode.class)
    void admissionTimeoutLeavesNoClaimAndRecoversUncommittedRecordOnExplicitRestart(OffsetCommitPolicy.Mode mode) throws Exception {
        offsetMode = mode;
        start(3, 1, 2, 100, 0);
        var waiting = create();
        var occupied = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var holderFailure = new AtomicReference<Throwable>();
        // Hold a real admission slot until rejection is observed. A timed HTTP delay can end
        // before the second consumer is scheduled on a slower CI host, bypassing the queue.
        var holder = Thread.ofPlatform().name("test-admission-holder").start(() -> {
            try {
                gate().execute(() -> {
                    occupied.countDown();
                    try {
                        if (!release.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Test holder timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                });
            } catch (Throwable failure) { holderFailure.set(failure); }
        });
        try {
            assertThat(occupied.await(5, TimeUnit.SECONDS)).isTrue();
            publish(waiting, 1);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                var counter = app.getBean(MeterRegistry.class).find("notification.worker.admission.rejected")
                        .tag("reason", "timeout").counter();
                assertThat(counter).isNotNull();
                assertThat(counter.count()).isEqualTo(1);
            });
            await().atMost(Duration.ofSeconds(15)).until(() -> !container().isRunning());
            assertThat(attempts(waiting)).isZero();
            assertThat(status(waiting)).isEqualTo("QUEUED");
            assertThat(offset(1)).isZero();
            assertThat(stats().requests()).isZero();
        } finally {
            release.countDown();
            holder.join(5000);
        }
        assertThat(holder.isAlive()).isFalse();
        assertThat(holderFailure.get()).isNull();
        awaitStopped();
        container().start(); awaitAssigned();
        awaitCompleted(List.of(waiting), 1);
        assertThat(stats().requests()).isEqualTo(1);
        assertThat(attempts(waiting)).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(OffsetCommitPolicy.Mode.class)
    void fullQueueRejectsBeforeClaimWithoutCommittingAndCanBeRestarted(OffsetCommitPolicy.Mode mode) throws Exception {
        offsetMode = mode;
        start(3, 1, 0, 1000, 1200);
        var first = create(); var rejected = create();
        publish(first, 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> stats().active() == 1);
        publish(rejected, 1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var counter = app.getBean(MeterRegistry.class).find("notification.worker.admission.rejected")
                    .tag("reason", "full").counter();
            assertThat(counter).isNotNull();
            assertThat(counter.count()).isEqualTo(1);
        });
        awaitStopped();
        assertThat(attempts(rejected)).isZero(); assertThat(offset(1)).isZero();
        await().atMost(Duration.ofSeconds(5)).until(() -> status(first).equals("PROVIDER_ACCEPTED"));
        container().start(); awaitAssigned();
        awaitCompleted(List.of(first, rejected), 2);
        assertThat(stats().requests()).isEqualTo(2);
    }

    @ParameterizedTest
    @EnumSource(OffsetCommitPolicy.Mode.class)
    void databaseFailureNeverCommitsPastFailedRecordAndRecoversAfterLeaseExpiry(OffsetCommitPolicy.Mode mode) throws Exception {
        offsetMode = mode;
        start(3, 2, 1, 5000, 300);
        var first = create(); var later = create(); var independent = create();
        root("CREATE TRIGGER reject_worker_completion BEFORE UPDATE ON notification FOR EACH ROW "
                + "BEGIN IF NEW.notification_id = '" + first.id() + "' AND NEW.status = 'PROVIDER_ACCEPTED' THEN "
                + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'synthetic write failure'; END IF; END");
        try {
            publish(first, 0); publish(later, 0); publish(independent, 1);
            awaitStopped();
            assertThat(offset(0)).isZero();
            assertThat(attempts(later)).isZero();
            assertThat(status(first)).isEqualTo("PROCESSING");
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt a JOIN delivery_task t USING (task_id) WHERE t.notification_id = ? AND a.outcome='STARTED'", Integer.class, first.id())).isEqualTo(1);
        } finally { root("DROP TRIGGER reject_worker_completion"); }
        // Simulate lease expiry after correcting the DB fault; real elapsed lease recovery is covered by S5.
        jdbc().update("UPDATE notification SET worker_lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6)) WHERE notification_id = ?", first.id());
        container().start(); awaitAssigned();
        awaitCompleted(List.of(first, later, independent), 3);
        assertThat(attempts(first)).isEqualTo(2);
        assertThat(attempts(later)).isEqualTo(1);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt a JOIN delivery_task t USING (task_id) WHERE t.notification_id = ? AND a.outcome='UNKNOWN'", Integer.class, first.id())).isEqualTo(1);
        assertThat(stats().requests()).isEqualTo(4);
    }

    @ParameterizedTest
    @EnumSource(OffsetCommitPolicy.Mode.class)
    void shutdownLeavesQueuedRecordUnclaimedAndNewContextFinishesIt(OffsetCommitPolicy.Mode mode) throws Exception {
        offsetMode = mode;
        start(3, 1, 2, 5000, 1500);
        var first = create(); var queued = create();
        publish(first, 0);
        await().atMost(Duration.ofSeconds(5)).until(() -> stats().active() == 1);
        publish(queued, 1);
        await().atMost(Duration.ofSeconds(5)).until(() -> gate().waiting() == 1);
        var oldGate = gate();
        app.close();
        assertThat(oldGate.active()).isZero(); assertThat(oldGate.waiting()).isZero();
        assertThat(stats().requests()).isEqualTo(1);
        // Inspect durable state without starting another consumer first.
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
             var statement = connection.prepareStatement("SELECT attempt_count FROM delivery_task WHERE notification_id = ?")) {
            statement.setString(1, queued.id());
            try (var rows = statement.executeQuery()) { assertThat(rows.next()).isTrue(); assertThat(rows.getInt(1)).isZero(); }
        }
        assertThat(offset(1)).isZero();
        startApplication(3, 1, 2, 5000);
        awaitCompleted(List.of(first, queued), 2);
        assertThat(stats().requests()).isEqualTo(2);
        assertThat(attempts(queued)).isEqualTo(1);
    }

    private void start(int consumers, int active, int queue, int waitMs, int delayMs) {
        mock = MockServerApplication.start("--server.port=0", "--mock.http.response-delay-ms=" + delayMs,
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        topic = "bounded-worker-" + UUID.randomUUID(); group = "bounded-worker-" + UUID.randomUUID();
        startApplication(consumers, active, queue, waitMs);
    }

    private void startApplication(int consumers, int active, int queue, int waitMs) {
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class, OffsetCommitPolicy.Wiring.class).run(
                "--notification.test.offset.mode=" + offsetMode.name(),
                "--server.port=0", "--app.provider.mode=HTTP", "--app.async.relay-enabled=false",
                "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort(),
                "--app.provider.http.response-timeout-ms=3000", "--app.provider.http.deadline-ms=4000",
                "--app.worker.consumers=" + consumers, "--app.worker.max-active=" + active,
                "--app.worker.queue-capacity=" + queue, "--app.worker.queue-wait-ms=" + waitMs,
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(), "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(), "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        if (admin == null) admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
        OffsetCommitPolicy.requireApplied(app);
        awaitAssigned();
    }

    private record Event(String id, String payload) { }

    @Test
    void countTimeFlushesLowVolumeTailWithoutTenNewRecords() throws Exception {
        offsetMode = OffsetCommitPolicy.Mode.COUNT_TIME;
        start(1, 1, 0, 5000, 0);
        var events = List.of(create(), create(), create());
        for (var event : events) publish(event, 0);
        awaitCompleted(events, 3);
        assertThat(stats().requests()).isEqualTo(3);
        assertThat(offset(0)).isEqualTo(3);
        assertThat(offset(1)).isZero(); assertThat(offset(2)).isZero();
        System.out.println("OFFSET_TIME_TAIL completed=3 committed=3 countThreshold=10");
    }

    private Event create() throws Exception {
        try (var http = HttpClient.newHttpClient()) {
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.getWebServer().getPort() + "/notifications"))
                    .header("Content-Type", "application/json").timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString("{\"recipientId\":\"synthetic-user\",\"title\":\"test\",\"body\":\"synthetic\"}"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(202);
            String id = new ObjectMapper().readTree(response.body()).path("notificationId").asText();
            return new Event(id, jdbc().queryForObject("SELECT payload FROM outbox_event WHERE notification_id = ?", String.class, id));
        }
    }

    @SuppressWarnings("unchecked")
    private void publish(Event event, int partition) throws Exception {
        // Explicit partitions make same-partition ordering observable. Relay is disabled for this experiment.
        app.getBean(KafkaTemplate.class).send(topic, partition, event.id(), event.payload()).get(5, TimeUnit.SECONDS);
    }

    private void awaitAssigned() {
        await().atMost(Duration.ofSeconds(20)).until(() -> container().getAssignedPartitions() != null
                && container().getAssignedPartitions().size() == 6);
    }

    private void awaitStopped() {
        await().atMost(Duration.ofSeconds(15)).until(() -> !container().isRunning());
        await().atMost(Duration.ofSeconds(10)).until(() -> gate().active() == 0 && gate().waiting() == 0);
        await().atMost(Duration.ofSeconds(10)).until(() -> admin.describeConsumerGroups(List.of(group))
                .all().get(5, TimeUnit.SECONDS).get(group).members().isEmpty());
    }

    private void awaitCompleted(List<Event> events, int expectedOffsets) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            for (var event : events) assertThat(status(event)).isEqualTo("PROVIDER_ACCEPTED");
            assertThat(offset(0) + offset(1) + offset(2)).isEqualTo(expectedOffsets);
        });
    }

    private long offset(int partition) throws Exception {
        var value = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .get(new TopicPartition(topic, partition));
        return value == null ? 0 : value.offset();
    }

    private JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private WorkerAdmission gate() { return app.getBean(WorkerAdmission.class); }
    private MockPushEndpoint.Stats stats() { return mock.getBean(MockPushEndpoint.class).stats(); }
    private String status(Event e) { return jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, e.id()); }
    private int attempts(Event e) { return jdbc().queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, e.id()); }
    private ConcurrentMessageListenerContainer<?, ?> container() {
        return (ConcurrentMessageListenerContainer<?, ?>) app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
    }
    private void root(String sql) throws Exception {
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) { statement.execute(sql); }
    }

    @AfterEach
    void close() {
        if (app != null) app.close();
        if (mock != null) mock.close();
        if (admin != null) admin.close(Duration.ofSeconds(2));
    }
}
