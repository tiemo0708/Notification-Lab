package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Stops only this class's broker. The application's scheduler and consumer remain running. */
@Testcontainers
class KafkaRecoveryTest {
    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4.8")
            .withDatabaseName("notification_lab").withUsername("notification_test")
            .withPassword(UUID.randomUUID().toString()).withCommand("--default-time-zone=+00:00")
            .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2")
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            // Port selection and Docker bind are not atomic. Retry initial provisioning only;
            // each create attempt selects a fresh port, while outage recovery keeps the same ID.
            .withStartupAttempts(3)
            // Docker may reassign a binding of 0 after stop/start. Keep one OS-selected port
            // so both the advertised listener and the already-running app retain their endpoint.
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", unusedPort()), ExposedPort.tcp(9092))));

    private ServletWebServerApplicationContext app;
    private HttpClient http;
    private Admin admin;
    private String topic;
    private String group;
    private String bootstrap;

    @Test
    void acceptsWhileBrokerIsStoppedAndAutomaticallyDrainsAfterRecovery() throws Exception {
        startApplication(10);
        String baseline = post("baseline");
        awaitCompleted(List.of(baseline), 1);
        String containerId = KAFKA.getContainerId();
        stopBroker();
        try {
            var accepted = new ArrayList<String>();
            for (int i = 0; i < 5; i++) accepted.add(post("outage-" + i));
            var originalRows = identities();
            assertThat(originalRows).hasSize(6);

            // Observe a real producer failure and its durable handoff back to PENDING.
            await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                assertThat(counter("outbox.publish.failures")).isGreaterThanOrEqualTo(1);
                assertThat(jdbc().queryForObject("""
                        SELECT COUNT(*) FROM outbox_event
                        WHERE status = 'PENDING' AND attempt_count > 0 AND published_at IS NULL
                        """, Integer.class)).isGreaterThanOrEqualTo(1);
            });
            for (String id : accepted) assertQueuedWithoutAttempt(id);
            assertThat(counter("notification.provider.attempts")).isEqualTo(1);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PUBLISHED'", Integer.class))
                    .isEqualTo(1);
            app.getBean(PipelineMetrics.class).refresh();
            assertThat(gauge("outbox.unpublished")).isEqualTo(5);
            assertThat(gauge("notification.consumer.lag")).isNaN();

            startBroker();
            assertThat(KAFKA.getContainerId()).isEqualTo(containerId);
            assertThat(KAFKA.getBootstrapServers()).isEqualTo(bootstrap);
            accepted.add(baseline);
            awaitCompleted(accepted, 6);
            assertThat(identities()).isEqualTo(originalRows);
            assertThat(counter("notification.provider.attempts")).isEqualTo(6);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(6);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PUBLISHED'", Integer.class))
                    .isEqualTo(6);
            assertThat(topicSize(topic + ".retry")).isZero();
            assertThat(topicSize(topic + ".dlq")).isZero();
            app.getBean(PipelineMetrics.class).refresh();
            assertThat(gauge("outbox.unpublished")).isZero();
            assertThat(gauge("outbox.failed")).isZero();
            assertThat(gauge("notification.consumer.lag")).isZero();
            System.out.println("BROKER_RECOVERY outageAccepted=5 totalAccepted=6 published=6 providerAttempts=6 committed=6 pending=0 lag=0");
        } finally {
            restoreBroker();
        }
    }

    @Test
    void exhaustedOutboxRemainsVisibleAfterBrokerRecoversWhileNewRequestsComplete() throws Exception {
        startApplication(1);
        String baseline = post("baseline");
        awaitCompleted(List.of(baseline), 1);
        stopBroker();
        try {
            String exhausted = post("exhausted");
            await().atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                    assertThat(outboxStatus(exhausted)).isEqualTo("FAILED"));
            assertQueuedWithoutAttempt(exhausted);
            var original = identities();
            startBroker();
            String fresh = post("after-recovery");
            awaitCompleted(List.of(baseline, fresh), 2);
            // New work proves the live scheduler/consumer recovered. Exhausted work stays inspectable.
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(outboxStatus(exhausted)).isEqualTo("FAILED");
                assertQueuedWithoutAttempt(exhausted);
                assertThat(counter("notification.provider.attempts")).isEqualTo(2);
            });
            assertThat(identities()).hasSize(3).containsAll(original);
            assertThat(jdbc().queryForObject("SELECT attempt_count FROM outbox_event WHERE notification_id = ?",
                    Integer.class, exhausted)).isEqualTo(1);
            assertThat(topicSize(topic + ".dlq")).isZero(); // Relay exhaustion is not a provider DLQ result.
            app.getBean(PipelineMetrics.class).refresh();
            assertThat(gauge("outbox.unpublished")).isEqualTo(1);
            assertThat(gauge("outbox.failed")).isEqualTo(1);
            assertThat(gauge("notification.consumer.lag")).isZero();
            System.out.println("BROKER_RECOVERY_LIMIT totalAccepted=3 published=2 providerAttempts=2 committed=2 queued=1 failedOutbox=1 lag=0");
        } finally {
            restoreBroker();
        }
    }

    private void startApplication(int publishAttempts) {
        topic = "recovery-test-" + UUID.randomUUID();
        group = "recovery-worker-" + UUID.randomUUID();
        bootstrap = KAFKA.getBootstrapServers();
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class).run(
                "--server.port=0", "--spring.profiles.active=async", "--app.mock.delay-ms=0",
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + bootstrap,
                "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--app.async.max-publish-attempts=" + publishAttempts,
                "--logging.level.org.apache.kafka=ERROR", "--logging.level.org.springframework.kafka=WARN");
        jdbc().update("DELETE FROM dead_letter_redrive");
        jdbc().update("DELETE FROM delivery_attempt");
        jdbc().update("DELETE FROM delivery_task");
        jdbc().update("DELETE FROM outbox_event");
        jdbc().update("DELETE FROM notification");
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap,
                "default.api.timeout.ms", 3000, "request.timeout.ms", 2000));
    }

    private String post(String key) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .header("Idempotency-Key", group + "-" + key)
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"recipientId":"synthetic-user","title":"복구 실험","body":"가상 데이터"}
                        """)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        JsonNode body = app.getBean(ObjectMapper.class).readTree(response.body());
        assertThat(body.path("status").asText()).isEqualTo("QUEUED");
        String id = body.path("notificationId").asText();
        assertThat(response.headers().firstValue("Location")).hasValue("/notifications/" + id);
        return id;
    }

    private void assertQueuedWithoutAttempt(String id) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications/" + id))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(app.getBean(ObjectMapper.class).readTree(response.body()).path("status").asText()).isEqualTo("QUEUED");
        assertThat(jdbc().queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, id))
                .isZero();
        assertThat(jdbc().queryForObject("""
                SELECT COUNT(*) FROM delivery_attempt a JOIN delivery_task t ON a.task_id = t.task_id
                WHERE t.notification_id = ?
                """, Integer.class, id)).isZero();
        assertThat(jdbc().queryForObject("SELECT published_at FROM outbox_event WHERE notification_id = ?",
                java.sql.Timestamp.class, id)).isNull();
    }

    private void awaitCompleted(List<String> ids, long committed) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            for (String id : ids) {
                assertThat(jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, id))
                        .isEqualTo("PROVIDER_ACCEPTED");
                assertThat(outboxStatus(id)).isEqualTo("PUBLISHED");
            }
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            assertThat(offsets.entrySet().stream().filter(e -> e.getKey().topic().equals(topic))
                    .mapToLong(e -> e.getValue().offset()).sum()).isEqualTo(committed);
        });
    }

    private void stopBroker() {
        KAFKA.getDockerClient().stopContainerCmd(KAFKA.getContainerId()).withTimeout(10).exec();
        assertThat(KAFKA.isRunning()).isFalse();
    }

    private void startBroker() {
        KAFKA.getDockerClient().startContainerCmd(KAFKA.getContainerId()).exec();
        await().atMost(Duration.ofSeconds(45)).ignoreExceptions().until(() ->
                admin.describeCluster().nodes().get(4, TimeUnit.SECONDS).size() == 1);
    }

    private void restoreBroker() {
        if (!KAFKA.isRunning()) startBroker();
    }

    private long topicSize(String name) throws Exception {
        var latest = new java.util.HashMap<TopicPartition, OffsetSpec>();
        admin.describeTopics(List.of(name)).allTopicNames().get(5, TimeUnit.SECONDS).get(name)
                .partitions().forEach(p -> latest.put(new TopicPartition(name, p.partition()), OffsetSpec.latest()));
        return admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS).values().stream().mapToLong(o -> o.offset()).sum();
    }

    private List<Map<String, Object>> identities() {
        return jdbc().queryForList("""
                SELECT n.notification_id, t.task_id, e.event_id
                FROM notification n JOIN delivery_task t ON n.notification_id = t.notification_id
                JOIN outbox_event e ON n.notification_id = e.notification_id ORDER BY n.notification_id
                """);
    }

    private String outboxStatus(String id) {
        return jdbc().queryForObject("SELECT status FROM outbox_event WHERE notification_id = ?", String.class, id);
    }

    private double counter(String name) {
        var counter = app.getBean(MeterRegistry.class).find(name).counter();
        return counter == null ? 0 : counter.count();
    }

    private double gauge(String name) { return app.getBean(MeterRegistry.class).get(name).gauge().value(); }
    private JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private String base() { return "http://127.0.0.1:" + app.getWebServer().getPort(); }

    private static int unusedPort() {
        try (var socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    @AfterEach
    void close() {
        if (http != null) http.close();
        if (app != null) app.close();
        if (admin != null) admin.close(Duration.ofSeconds(3));
    }
}
