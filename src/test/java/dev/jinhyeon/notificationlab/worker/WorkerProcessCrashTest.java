package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.outbox.PipelineMetrics;
import dev.jinhyeon.notificationlabtest.WorkerProcess;
import dev.jinhyeon.notificationlabtest.OffsetWorkerProcess;
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
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
class WorkerProcessCrashTest {
    @Container
    final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4.8")
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
    private final String topic = "worker-crash-" + UUID.randomUUID();
    private final String group = "worker-crash-worker-" + UUID.randomUUID();
    private ServletWebServerApplicationContext app;
    private HttpClient http;
    private Admin admin;

    @Test
    @Timeout(180)
    void killAfterDatabaseCompletionReconsumesUncommittedRecordWithoutCallingProviderAgain() throws Exception {
        startApiAndRelay();
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .header("Idempotency-Key", "worker-crash-boundary")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"recipientId":"synthetic-user","title":"offset 경계 실험","body":"가상 데이터"}
                        """)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        String id = app.getBean(ObjectMapper.class).readTree(response.body()).path("notificationId").asText();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(jdbc().queryForObject("SELECT status FROM outbox_event", String.class)).isEqualTo("PUBLISHED"));
        assertThat(topicSize(topic)).isEqualTo(1);
        Path marker = directory.resolve("db-completed.txt");
        Process original = startWorker("original", marker);
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(original.isAlive()).withFailMessage(() -> childOutput("original")).isTrue();
            assertThat(marker).hasContent(id);
            assertThat(directory.resolve("original.port")).exists();
        });

        // Read committed DB rows on a different connection and Kafka offsets independently.
        assertThat(jdbc().queryForObject("SELECT status FROM notification", String.class)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(committed()).isZero(); // No offset yet, or initial offset 0, both mean record 0 is uncommitted.
        assertThat(childCounter("original", "notification_provider_attempts_total")).isEqualTo(1);
        var notification = jdbc().queryForMap("SELECT * FROM notification");
        var task = jdbc().queryForMap("SELECT * FROM delivery_task");
        var attempts = jdbc().queryForList("SELECT * FROM delivery_attempt");
        var outbox = jdbc().queryForMap("SELECT * FROM outbox_event");
        assertThat(task.get("attempt_count")).isEqualTo(1);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.getFirst().get("outcome")).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(attempts.getFirst().get("completed_at")).isNotNull();
        assertThat(outbox.get("attempt_count")).isEqualTo(1);
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(gauge("notification.consumer.lag")).isEqualTo(1);

        original.destroyForcibly();
        assertThat(original.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(original.exitValue()).isNotZero();
        assertThat(committed()).isZero();
        assertThat(jdbc().queryForMap("SELECT * FROM notification")).usingRecursiveComparison().isEqualTo(notification);
        Process replacement = startWorker("replacement", null);
        assertThat(replacement.pid()).isNotEqualTo(original.pid());

        // Same group/topic, no seek/reset or republish: wait for the broker's normal reassignment.
        await().atMost(Duration.ofSeconds(75)).untilAsserted(() -> {
            assertThat(replacement.isAlive()).withFailMessage(() -> childOutput("replacement")).isTrue();
            assertThat(committed()).isEqualTo(1);
            assertThat(directory.resolve("replacement.port")).exists();
        });
        assertThat(childCounter("replacement", "notification_worker_replays_total")).isEqualTo(1);
        assertThat(childCounter("replacement", "notification_provider_attempts_total")).isZero();
        assertThat(topicSize(topic)).isEqualTo(1);
        assertThat(jdbc().queryForMap("SELECT * FROM notification")).usingRecursiveComparison().isEqualTo(notification);
        assertThat(jdbc().queryForMap("SELECT * FROM delivery_task")).usingRecursiveComparison().isEqualTo(task);
        assertThat(jdbc().queryForList("SELECT * FROM delivery_attempt")).isEqualTo(attempts);
        assertThat(jdbc().queryForMap("SELECT * FROM outbox_event")).isEqualTo(outbox);
        assertThat(topicSize(topic + ".retry")).isZero();
        assertThat(topicSize(topic + ".dlq")).isZero();
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(gauge("outbox.unpublished")).isZero();
        assertThat(gauge("notification.consumer.lag")).isZero();
        System.out.println("WORKER_PROCESS_CRASH kafkaRecords=1 deliveryAttempts=1 originalProviderAttempts=1 replacementProviderAttempts=0 replays=1 committed=1 lag=0");
    }

    @Test
    @Timeout(150)
    void countTimeCrashReplaysMultipleCompletedRecordsWithoutRepeatingProviderCalls() throws Exception {
        verifyOffsetBoundary(OffsetWorkerProcess.Fault.HOLD);
    }

    @ParameterizedTest
    @EnumSource(value = OffsetWorkerProcess.Fault.class, names = {"BEFORE_RPC", "AFTER_RPC"})
    @Timeout(150)
    void countTimeCommitFailureRestartsFromBrokerObservedOffset(OffsetWorkerProcess.Fault fault) throws Exception {
        verifyOffsetBoundary(fault);
    }

    private void verifyOffsetBoundary(OffsetWorkerProcess.Fault fault) throws Exception {
        startApiAndRelay(false);
        Process original = startOffsetWorker("offset-original", true, fault);
        for (int i = 0; i < 20; i++) publishManual(createManual());
        await().atMost(Duration.ofSeconds(30)).until(() -> committed() == 20 && Files.exists(directory.resolve("offset-original.port")));
        var tail = new ArrayList<ManualEvent>();
        for (int i = 0; i < 12; i++) tail.add(createManual());
        Files.writeString(directory.resolve("offset-armed"), "armed", java.nio.file.StandardOpenOption.CREATE_NEW);
        for (var event : tail) publishManual(event);
        Path marker = directory.resolve("offset-window");
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(original.isAlive()).isTrue();
            assertThat(marker).exists();
            assertThat(Files.readString(marker)).matches("[0-9]+:[0-9]+");
        });
        var values = Files.readString(marker).split(":");
        int before = Integer.parseInt(values[0]); int completed = Integer.parseInt(values[1]);
        assertThat(completed - before).isBetween(2, 10);
        if (fault != OffsetWorkerProcess.Fault.HOLD) {
            await().atMost(Duration.ofSeconds(20)).until(() -> admin.describeConsumerGroups(List.of(group))
                    .all().get(5, TimeUnit.SECONDS).get(group).members().isEmpty());
        }
        int brokerObserved = fault == OffsetWorkerProcess.Fault.AFTER_RPC ? completed : before;
        assertThat(committed()).isEqualTo(brokerObserved);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(completed);
        assertThat(childCounter("offset-original", "notification_provider_attempts_total")).isEqualTo(completed);
        var finishedIds = jdbc().queryForList("SELECT notification_id FROM notification WHERE status='PROVIDER_ACCEPTED' ORDER BY notification_id", String.class);
        String digest = completedFingerprint(finishedIds);
        original.destroyForcibly();
        assertThat(original.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(original.exitValue()).isNotZero();
        assertThat(committed()).isEqualTo(brokerObserved);
        Process replacement = startOffsetWorker("offset-replacement", false);
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            assertThat(replacement.isAlive()).isTrue();
            assertThat(committed()).isEqualTo(32);
            assertThat(directory.resolve("offset-replacement.port")).exists();
        });
        assertThat(childCounter("offset-replacement", "notification_worker_replays_total")).isEqualTo(completed - brokerObserved);
        assertThat(childCounter("offset-replacement", "notification_provider_attempts_total")).isEqualTo(32 - completed);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(32);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_task WHERE attempt_count=1", Integer.class)).isEqualTo(32);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt WHERE outcome='PROVIDER_ACCEPTED' AND attempt_no=1", Integer.class)).isEqualTo(32);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(32);
        assertThat(completedFingerprint(finishedIds)).isEqualTo(digest);
        assertThat(topicSize(topic)).isEqualTo(32);
        assertThat(topicSize(topic + ".retry")).isZero(); assertThat(topicSize(topic + ".dlq")).isZero();
        Path output = Path.of(fault == OffsetWorkerProcess.Fault.HOLD ? "build/reports/offset-commit-safety.json"
                : "build/reports/offset-commit-" + fault.name().toLowerCase(java.util.Locale.ROOT) + ".json");
        Files.createDirectories(output.getParent());
        var report = new java.util.LinkedHashMap<String, Object>();
        report.put("result", "PASS"); report.put("fault", fault.name()); report.put("targets", 32); report.put("warmup", 20);
        report.put("previousSuccessfulCommit", before); report.put("attemptedCommit", completed);
        report.put("brokerObservedBeforeRestart", brokerObserved); report.put("completedBeforeKill", completed);
        report.put("completedReplayed", completed - brokerObserved); report.put("replacementProviderAttempts", 32 - completed);
        report.put("totalProviderAttempts", 32); report.put("finalCommitted", 32); report.put("completedRowsUnchanged", true);
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
        System.out.printf("OFFSET_WINDOW_CRASH fault=%s targets=32 completedReplayed=%d totalProviderAttempts=32 committed=32%n", fault, completed - brokerObserved);
    }

    private record ManualEvent(String id, String payload) { }

    private ManualEvent createManual() throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"recipientId\":\"synthetic\",\"title\":\"offset\",\"body\":\"synthetic\"}"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        String id = new ObjectMapper().readTree(response.body()).path("notificationId").asText();
        return new ManualEvent(id, jdbc().queryForObject("SELECT payload FROM outbox_event WHERE notification_id=?", String.class, id));
    }

    @SuppressWarnings("unchecked")
    private void publishManual(ManualEvent event) throws Exception {
        app.getBean(org.springframework.kafka.core.KafkaTemplate.class).send(topic, 0, event.id(), event.payload()).get(5, TimeUnit.SECONDS);
    }

    private String completedFingerprint(List<String> ids) throws Exception {
        String placeholders = String.join(",", java.util.Collections.nCopies(ids.size(), "?"));
        var digest = java.security.MessageDigest.getInstance("SHA-256");
        for (String query : List.of(
                "SELECT notification_id,status,provider_message_id,error_code,completed_at FROM notification WHERE notification_id IN (%s) ORDER BY notification_id",
                "SELECT task_id,notification_id,attempt_count,retry_generation,next_attempt_at FROM delivery_task WHERE notification_id IN (%s) ORDER BY task_id",
                "SELECT a.* FROM delivery_attempt a JOIN delivery_task t USING(task_id) WHERE t.notification_id IN (%s) ORDER BY a.task_id,a.attempt_no")) {
            digest.update(jdbc().queryForList(query.formatted(placeholders), ids.toArray()).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return java.util.HexFormat.of().formatHex(digest.digest());
    }

    private Process startOffsetWorker(String name, boolean gate) throws Exception {
        return startOffsetWorker(name, gate, OffsetWorkerProcess.Fault.HOLD);
    }

    private Process startOffsetWorker(String name, boolean gate, OffsetWorkerProcess.Fault fault) throws Exception {
        String classpath = System.getProperty("notificationLab.testClasspath");
        assertThat(classpath).isNotBlank();
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", classpath, OffsetWorkerProcess.class.getName()));
        command.addAll(arguments());
        command.add("--notification.test.offset.fault=" + fault.name());
        command.addAll(List.of("--app.async.relay-enabled=false", "--notification.test.offset.mode=COUNT_TIME",
                "--app.worker.consumers=1", "--app.worker.max-active=1", "--app.worker.queue-capacity=0",
                "--spring.kafka.consumer.properties.session.timeout.ms=6000", "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000"));
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(directory.resolve(name + ".log").toFile());
        builder.environment().put("MYSQL_PASSWORD", MYSQL.getPassword());
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword());
        builder.environment().put("NOTIFICATION_LAB_WORKER_PORT", directory.resolve(name + ".port").toString());
        builder.environment().remove("NOTIFICATION_LAB_COMPLETION_MARKER");
        builder.environment().remove("NOTIFICATION_LAB_OFFSET_MARKER");
        builder.environment().remove("NOTIFICATION_LAB_OFFSET_ARM");
        if (gate) {
            builder.environment().put("NOTIFICATION_LAB_OFFSET_MARKER", directory.resolve("offset-window").toString());
            builder.environment().put("NOTIFICATION_LAB_OFFSET_ARM", directory.resolve("offset-armed").toString());
        }
        Process child = builder.start(); children.add(child); return child;
    }

    private void startApiAndRelay() { startApiAndRelay(true); }

    private void startApiAndRelay(boolean relay) {
        var args = arguments();
        args.add("--app.async.worker-enabled=false");
        args.add("--app.async.relay-enabled=" + relay);
        args.add("--spring.datasource.password=" + MYSQL.getPassword());
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class)
                .run(args.toArray(String[]::new));
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 5000, "request.timeout.ms", 3000));
    }

    private Process startWorker(String name, Path marker) throws Exception {
        String classpath = System.getProperty("notificationLab.testClasspath");
        assertThat(classpath).as("Run through the Gradle test task to supply the child JVM classpath").isNotBlank();
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", classpath, WorkerProcess.class.getName()));
        command.addAll(arguments());
        command.add("--app.async.relay-enabled=false");
        var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve(name + ".log").toFile());
        builder.environment().put("MYSQL_PASSWORD", MYSQL.getPassword());
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword());
        builder.environment().put("NOTIFICATION_LAB_WORKER_PORT", directory.resolve(name + ".port").toString());
        builder.environment().remove("NOTIFICATION_LAB_COMPLETION_MARKER");
        if (marker != null) builder.environment().put("NOTIFICATION_LAB_COMPLETION_MARKER", marker.toString());
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

    private long committed() throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        return offsets.entrySet().stream().filter(e -> e.getKey().topic().equals(topic))
                .mapToLong(e -> e.getValue().offset()).sum();
    }

    private double childCounter(String name, String metric) throws Exception {
        String port = Files.readString(directory.resolve(name + ".port")).strip();
        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/actuator/prometheus"))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        // Counters are lazily registered: absent means no call, once a valid scrape is confirmed.
        return response.body().lines().filter(line -> line.startsWith(metric + " "))
                .mapToDouble(line -> Double.parseDouble(line.substring(metric.length() + 1))).sum();
    }

    private long topicSize(String name) throws Exception {
        var latest = new java.util.HashMap<TopicPartition, OffsetSpec>();
        admin.describeTopics(List.of(name)).allTopicNames().get(5, TimeUnit.SECONDS).get(name)
                .partitions().forEach(p -> latest.put(new TopicPartition(name, p.partition()), OffsetSpec.latest()));
        return admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS).values().stream().mapToLong(o -> o.offset()).sum();
    }

    private String childOutput(String name) {
        try {
            String log = Files.readString(directory.resolve(name + ".log"));
            return log.substring(Math.max(0, log.length() - 6000));
        } catch (java.io.IOException failure) {
            return "Child output unavailable: " + failure.getClass().getSimpleName();
        }
    }

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
