package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.outbox.PipelineMetrics;
import dev.jinhyeon.notificationlabtest.ShutdownProcess;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
class GracefulShutdownTest {
    @Container
    final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4.8")
            .withDatabaseName("notification_lab").withUsername("notification_test")
            .withPassword(UUID.randomUUID().toString()).withCommand("--default-time-zone=+00:00")
            .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    @Container
    final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2")
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    @TempDir
    Path directory;
    private final List<Process> children = new ArrayList<>();
    private final String topic = "shutdown-" + UUID.randomUUID();
    private final String group = "shutdown-worker-" + UUID.randomUUID();
    private ServletWebServerApplicationContext app;
    private HttpClient http;
    private Admin admin;

    @ParameterizedTest(name = "{0} completesWithinGrace={1}")
    @CsvSource({"worker,true", "worker,false", "relay,true", "relay,false"})
    @Timeout(180)
    void sigtermStopsAcceptanceAndRecoversDurableWork(String role, boolean completesWithinGrace) throws Exception {
        startInspector();
        Process original = startProcess("original", role, true);
        awaitReady(original, "original");
        String first = submit(childBase("original"));
        Path entered = directory.resolve("original/entered");
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(entered).exists());
        String firstEvent = jdbc().queryForObject("SELECT event_id FROM outbox_event WHERE notification_id = ?", String.class, first);
        assertThat(entered).hasContent(role.equals("worker") ? first : firstEvent);
        String second = submit(childBase("original"));
        if (role.equals("worker")) {
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PUBLISHED'", Integer.class)).isEqualTo(2));
            assertThat(Files.readAllLines(directory.resolve("calls"))).containsExactly(first);
        } else {
            assertThat(status("outbox_event", first)).isEqualTo("PROCESSING");
            assertThat(status("outbox_event", second)).isEqualTo("PENDING");
        }
        var taskIds = jdbc().queryForList("SELECT task_id FROM delivery_task ORDER BY task_id", String.class);
        var eventIds = jdbc().queryForList("SELECT event_id FROM outbox_event ORDER BY event_id", String.class);
        assertThat(committed()).isZero();

        long shutdownStarted = System.nanoTime();
        // Process.destroy() sends SIGTERM on the supported macOS/Linux test hosts.
        original.destroy();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(directory.resolve("original/closing")).exists());
        awaitHttpStopped("original");
        assertThat(original.isAlive()).as("In-flight work keeps the process alive during grace").isTrue();
        assertPostRejected("original");
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification", Integer.class)).isEqualTo(2);
        assertThat(committed()).isZero();
        if (completesWithinGrace) Files.writeString(directory.resolve("original/release"), "continue");
        // Do not force-kill to make this pass. The configured lifecycle timeout must end SIGTERM itself.
        assertThat(original.waitFor(25, TimeUnit.SECONDS)).withFailMessage(() -> childOutput("original")).isTrue();
        long shutdownMillis = Duration.ofNanos(System.nanoTime() - shutdownStarted).toMillis();
        assertThat(original.exitValue()).isEqualTo(143);
        if (!completesWithinGrace) assertThat(shutdownMillis).isGreaterThanOrEqualTo(9000);
        assertThat(shutdownMillis).isLessThan(25000);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification", Integer.class)).isEqualTo(2);
        if (role.equals("worker")) {
            assertThat(status("notification", first)).isEqualTo(completesWithinGrace ? "PROVIDER_ACCEPTED" : "PROCESSING");
            assertThat(status("notification", second)).isEqualTo("QUEUED");
            assertThat(jdbc().queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, second)).isZero();
            assertThat(committed()).isEqualTo(completesWithinGrace ? 1 : 0);
        } else {
            assertThat(status("outbox_event", first)).isEqualTo(completesWithinGrace ? "PUBLISHED" : "PROCESSING");
            assertThat(status("outbox_event", second)).isEqualTo("PENDING");
            assertThat(topicSize(topic)).isEqualTo(1);
        }
        // A live lease deliberately rejects a new claim. Wait for real DB time, never rewrite it.
        if (!completesWithinGrace) {
            String sql = role.equals("worker")
                    ? "SELECT worker_lease_until <= UTC_TIMESTAMP(6) FROM notification WHERE notification_id = ?"
                    : "SELECT available_at <= UTC_TIMESTAMP(6) FROM outbox_event WHERE notification_id = ?";
            await().atMost(Duration.ofSeconds(35)).untilAsserted(() ->
                    assertThat(jdbc().queryForObject(sql, Boolean.class, first)).isTrue());
        }
        Process replacement = startProcess("replacement", role, false);
        assertThat(replacement.pid()).isNotEqualTo(original.pid());
        awaitReady(replacement, "replacement");
        long records = role.equals("relay") && !completesWithinGrace ? 3 : 2;
        await().atMost(Duration.ofSeconds(75)).untilAsserted(() -> {
            assertThat(replacement.isAlive()).withFailMessage(() -> childOutput("replacement")).isTrue();
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status = 'PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(2);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PUBLISHED'", Integer.class)).isEqualTo(2);
            assertThat(committed()).isEqualTo(records);
        });
        assertThat(topicSize(topic)).isEqualTo(records);
        assertThat(jdbc().queryForList("SELECT task_id FROM delivery_task ORDER BY task_id", String.class)).isEqualTo(taskIds);
        assertThat(jdbc().queryForList("SELECT event_id FROM outbox_event ORDER BY event_id", String.class)).isEqualTo(eventIds);
        var calls = Files.readAllLines(directory.resolve("calls"));
        boolean uncertainWorker = role.equals("worker") && !completesWithinGrace;
        assertThat(calls).containsExactlyInAnyOrderElementsOf(uncertainWorker ? List.of(first, first, second) : List.of(first, second));
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(uncertainWorker ? 3 : 2);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt WHERE outcome = 'UNKNOWN'", Integer.class)).isEqualTo(uncertainWorker ? 1 : 0);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt WHERE outcome = 'PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(2);
        assertThat(jdbc().queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, first)).isEqualTo(uncertainWorker ? 2 : 1);
        assertThat(jdbc().queryForObject("SELECT attempt_count FROM outbox_event WHERE notification_id = ?", Integer.class, first))
                .isEqualTo(role.equals("relay") && !completesWithinGrace ? 2 : 1);
        assertThat(topicSize(topic + ".retry")).isZero();
        assertThat(topicSize(topic + ".dlq")).isZero();
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(gauge("outbox.unpublished")).isZero();
        assertThat(gauge("notification.consumer.lag")).isZero();
        System.out.printf("GRACEFUL_SHUTDOWN role=%s withinGrace=%s shutdownMs=%d accepted=2 providerCalls=%d records=%d committed=%d unknown=%d lag=0%n",
                role, completesWithinGrace, shutdownMillis, calls.size(), records, committed(), uncertainWorker ? 1 : 0);
    }

    private String status(String table, String id) {
        return jdbc().queryForObject("SELECT status FROM " + table + " WHERE notification_id = ?", String.class, id);
    }

    private String submit(String base) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base + "/notifications"))
                .timeout(Duration.ofSeconds(3)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"recipientId":"synthetic-user","title":"종료 실험","body":"가상 데이터"}
                        """)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        return app.getBean(ObjectMapper.class).readTree(response.body()).path("notificationId").asText();
    }

    private void awaitHttpStopped(String name) {
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            try {
                return http.send(HttpRequest.newBuilder(URI.create(childBase(name) + "/actuator/health"))
                        .timeout(Duration.ofSeconds(1)).GET().build(), HttpResponse.BodyHandlers.discarding()).statusCode() == 503;
            } catch (java.io.IOException stopped) {
                return true;
            }
        });
    }

    private void assertPostRejected(String name) throws Exception {
        try {
            var response = http.send(HttpRequest.newBuilder(URI.create(childBase(name) + "/notifications"))
                    .timeout(Duration.ofSeconds(1)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {"recipientId":"shutdown-probe","title":"차단 확인","body":"가상 데이터"}
                            """)).build(), HttpResponse.BodyHandlers.discarding());
            assertThat(response.statusCode()).isEqualTo(503);
        } catch (java.io.IOException stopped) {
            // Tomcat rejects new TCP/HTTP requests after graceful shutdown starts.
        }
    }

    private void startInspector() {
        var args = arguments();
        args.add("--app.async.worker-enabled=false");
        args.add("--app.async.relay-enabled=false");
        args.add("--spring.datasource.password=" + MYSQL.getPassword());
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class)
                .run(args.toArray(String[]::new));
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 5000, "request.timeout.ms", 3000));
    }

    private Process startProcess(String name, String role, boolean gate) throws Exception {
        String classpath = System.getProperty("notificationLab.testClasspath");
        assertThat(classpath).isNotBlank();
        Path childDirectory = Files.createDirectory(directory.resolve(name));
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", classpath, ShutdownProcess.class.getName()));
        command.addAll(arguments());
        command.add("--app.async.worker-enabled=" + (!gate || role.equals("worker")));
        var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve(name + ".log").toFile());
        builder.environment().put("MYSQL_PASSWORD", MYSQL.getPassword());
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword());
        builder.environment().put("NOTIFICATION_LAB_PROCESS_DIRECTORY", childDirectory.toString());
        builder.environment().put("NOTIFICATION_LAB_PROVIDER_CALLS", directory.resolve("calls").toString());
        builder.environment().remove("NOTIFICATION_LAB_SHUTDOWN_GATE");
        if (gate) builder.environment().put("NOTIFICATION_LAB_SHUTDOWN_GATE", role);
        Process child = builder.start();
        children.add(child);
        return child;
    }

    private void awaitReady(Process child, String name) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(child.isAlive()).withFailMessage(() -> childOutput(name)).isTrue();
            assertThat(directory.resolve(name + "/port")).exists();
        });
    }

    private String childBase(String name) throws Exception {
        return "http://127.0.0.1:" + Files.readString(directory.resolve(name + "/port")).strip();
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
