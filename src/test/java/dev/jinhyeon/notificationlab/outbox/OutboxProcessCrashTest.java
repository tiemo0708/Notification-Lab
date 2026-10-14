package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlabtest.RelayProcess;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class OutboxProcessCrashTest {
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
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    @TempDir
    Path directory;
    private final List<Process> children = new ArrayList<>();
    private final String topic = "outbox-crash-" + UUID.randomUUID();
    private final String group = "outbox-crash-worker-" + UUID.randomUUID();
    private ServletWebServerApplicationContext app;
    private HttpClient http;
    private Admin admin;

    @Test
    @Timeout(180)
    void killAfterAckRepublishesSameEventAfterLeaseWithoutRepeatingCompletedDelivery() throws Exception {
        startApiAndWorker();
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .header("Idempotency-Key", "crash-boundary")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"recipientId":"synthetic-user","title":"ACK 경계 실험","body":"가상 데이터"}
                        """)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        String id = app.getBean(ObjectMapper.class).readTree(response.body()).path("notificationId").asText();
        String eventId = jdbc().queryForObject("SELECT event_id FROM outbox_event", String.class);
        String taskId = jdbc().queryForObject("SELECT task_id FROM delivery_task", String.class);
        Path marker = directory.resolve("ack-received.txt");
        Process original = startRelay("original", marker);
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(original.isAlive()).withFailMessage(() -> childOutput("original")).isTrue();
            assertThat(marker).hasContent(eventId);
        });

        // Observe durable Kafka and Worker evidence independently of the test marker.
        awaitCommitted(1);
        assertThat(topicSize(topic)).isEqualTo(1);
        assertThat(jdbc().queryForObject("SELECT status FROM notification", String.class)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(counter("notification.provider.attempts")).isEqualTo(1);
        var completed = jdbc().queryForMap("SELECT * FROM notification");
        var attempts = jdbc().queryForList("SELECT * FROM delivery_attempt");
        assertThat(attempts).hasSize(1);
        var claimed = jdbc().queryForMap("SELECT * FROM outbox_event");
        assertThat(claimed.get("status")).isEqualTo("PROCESSING");
        assertThat(claimed.get("attempt_count")).isEqualTo(1);
        assertThat(claimed.get("published_at")).isNull();
        assertThat(claimed.get("lease_token")).isNotNull();
        assertThat(jdbc().queryForObject("SELECT available_at > UTC_TIMESTAMP(6) FROM outbox_event", Boolean.class)).isTrue();

        // Force-kill only the child created by this test, with no Spring shutdown hooks.
        original.destroyForcibly();
        assertThat(original.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(original.exitValue()).isNotZero();
        assertThat(jdbc().queryForMap("SELECT * FROM outbox_event")).isEqualTo(claimed);
        Process replacement = startRelay("replacement", null);
        assertThat(replacement.pid()).isNotEqualTo(original.pid());

        // Do not rewrite available_at: recovery must wait for the real 30-second lease.
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(replacement.isAlive()).withFailMessage(() -> childOutput("replacement")).isTrue();
            assertThat(jdbc().queryForObject("SELECT status FROM outbox_event", String.class)).isEqualTo("PUBLISHED");
        });
        awaitCommitted(2);
        assertThat(topicSize(topic)).isEqualTo(2);
        var republished = jdbc().queryForMap("SELECT * FROM outbox_event");
        assertThat(republished.get("event_id")).isEqualTo(eventId);
        assertThat(republished.get("notification_id")).isEqualTo(id);
        assertThat(republished.get("payload")).isEqualTo(claimed.get("payload"));
        assertThat(republished.get("attempt_count")).isEqualTo(2);
        assertThat(republished.get("lease_token")).isNull();
        assertThat(jdbc().queryForObject("SELECT published_at >= ? FROM outbox_event", Boolean.class,
                claimed.get("available_at"))).isTrue();
        var records = records();
        assertThat(records).hasSize(2);
        assertThat(records.get(0).key()).isEqualTo(id);
        assertThat(records.get(1).key()).isEqualTo(id);
        assertThat(records.get(1).value()).isEqualTo(records.get(0).value());
        var payload = app.getBean(ObjectMapper.class).readTree(records.get(0).value());
        assertThat(payload.path("eventId").asText()).isEqualTo(eventId);
        assertThat(payload.path("taskId").asText()).isEqualTo(taskId);
        assertThat(records.get(1).partition()).isEqualTo(records.get(0).partition());
        assertThat(records.get(1).offset()).isEqualTo(records.get(0).offset() + 1);
        assertThat(jdbc().queryForMap("SELECT * FROM notification")).usingRecursiveComparison().isEqualTo(completed);
        assertThat(jdbc().queryForList("SELECT * FROM delivery_attempt")).isEqualTo(attempts);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_task", Integer.class)).isEqualTo(1);
        assertThat(jdbc().queryForObject("SELECT task_id FROM delivery_task", String.class)).isEqualTo(taskId);
        assertThat(jdbc().queryForObject("SELECT attempt_count FROM delivery_task", Integer.class)).isEqualTo(1);
        assertThat(counter("notification.provider.attempts")).isEqualTo(1);
        assertThat(counter("notification.worker.replays")).isEqualTo(1);
        assertThat(topicSize(topic + ".retry")).isZero();
        assertThat(topicSize(topic + ".dlq")).isZero();
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(gauge("outbox.unpublished")).isZero();
        assertThat(gauge("notification.consumer.lag")).isZero();
        System.out.println("OUTBOX_PROCESS_CRASH kafkaRecords=2 eventIds=1 outboxAttempts=2 deliveryAttempts=1 providerAttempts=1 replays=1 committed=2 lag=0");
    }

    private void startApiAndWorker() {
        var args = arguments();
        args.add("--app.async.relay-enabled=false");
        args.add("--spring.datasource.password=" + MYSQL.getPassword());
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class)
                .run(args.toArray(String[]::new));
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 5000, "request.timeout.ms", 3000));
    }

    private Process startRelay(String name, Path marker) throws Exception {
        String classpath = System.getProperty("notificationLab.testClasspath");
        assertThat(classpath).as("Run through the Gradle test task to supply the child JVM classpath").isNotBlank();
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", classpath, RelayProcess.class.getName()));
        command.addAll(arguments());
        command.add("--app.async.worker-enabled=false");
        var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve(name + ".log").toFile());
        builder.environment().put("MYSQL_PASSWORD", MYSQL.getPassword());
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword());
        builder.environment().remove("NOTIFICATION_LAB_ACK_MARKER");
        if (marker != null) builder.environment().put("NOTIFICATION_LAB_ACK_MARKER", marker.toString());
        Process child = builder.start();
        children.add(child);
        return child;
    }

    private ArrayList<String> arguments() {
        return new ArrayList<>(List.of("--server.port=0", "--spring.profiles.active=async",
                "--app.mock.delay-ms=0", "--app.mock.outcome=SUCCESS",
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--logging.level.org.apache.kafka=WARN", "--logging.level.org.springframework.kafka=WARN"));
    }

    private void awaitCommitted(long expected) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            assertThat(offsets.entrySet().stream().filter(e -> e.getKey().topic().equals(topic))
                    .mapToLong(e -> e.getValue().offset()).sum()).isEqualTo(expected);
        });
    }

    private long topicSize(String name) throws Exception {
        var latest = new java.util.HashMap<TopicPartition, OffsetSpec>();
        admin.describeTopics(List.of(name)).allTopicNames().get(5, TimeUnit.SECONDS).get(name)
                .partitions().forEach(p -> latest.put(new TopicPartition(name, p.partition()), OffsetSpec.latest()));
        return admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS).values().stream().mapToLong(o -> o.offset()).sum();
    }

    private List<ConsumerRecord<String, String>> records() throws Exception {
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "enable.auto.commit", false,
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"))) {
            var partitions = admin.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS).get(topic)
                    .partitions().stream().map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var records = new ArrayList<ConsumerRecord<String, String>>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
                assertThat(records).hasSize(2);
            });
            return records;
        }
    }

    private String childOutput(String name) {
        try {
            String log = Files.readString(directory.resolve(name + ".log"));
            return log.substring(Math.max(0, log.length() - 6000));
        } catch (java.io.IOException failure) {
            return "Child output unavailable: " + failure.getClass().getSimpleName();
        }
    }

    private double counter(String name) { return app.getBean(MeterRegistry.class).get(name).counter().count(); }
    private double gauge(String name) { return app.getBean(MeterRegistry.class).get(name).gauge().value(); }
    private JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private String base() { return "http://127.0.0.1:" + app.getWebServer().getPort(); }

    @AfterEach
    void close() throws Exception {
        for (Process child : children) {
            if (child.isAlive()) child.destroyForcibly();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
        if (http != null) http.close();
        if (app != null) app.close();
        if (admin != null) admin.close(Duration.ofSeconds(3));
    }
}
