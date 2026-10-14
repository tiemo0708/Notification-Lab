package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.outbox.PipelineMetrics;
import dev.jinhyeon.notificationlabtest.RebalanceWorkerProcess;
import dev.jinhyeon.notificationlabtest.RelayProcess;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
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
class WorkerRebalanceTest {
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
    private final String topic = "rebalance-" + UUID.randomUUID();
    private final String group = "rebalance-worker-" + UUID.randomUUID();
    private ServletWebServerApplicationContext app;
    private HttpClient http;
    private Admin admin;
    private final List<String> ids = new ArrayList<>();

    @Test
    @Timeout(240)
    void joinsDuringProcessingThenRestartsRelayAndWorkerWithoutSkippingOrRepeatingDelivery() throws Exception {
        startApi();
        Process relay = startChild("relay", true, false);
        Process workerA = startChild("worker-a", false, true);
        awaitAssignments(1);
        String firstId = submit();
        Path entered = directory.resolve("entered");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(workerA.isAlive()).withFailMessage(() -> childOutput("worker-a")).isTrue();
            assertThat(entered).hasContent(firstId);
        });
        assertThat(jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?",
                String.class, firstId)).isEqualTo("PROCESSING");
        assertThat(committed()).isZero();
        submitPartitionCoverage();
        Process workerB = startChild("worker-b", false, false);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(workerB.isAlive()).withFailMessage(() -> childOutput("worker-b")).isTrue();
            assertThat(description().state()).isEqualTo(ConsumerGroupState.PREPARING_REBALANCE);
        });
        // Rebalance began while the record was unfinished. Its partition must not advance.
        assertThat(jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?",
                String.class, firstId)).isEqualTo("PROCESSING");
        assertThat(committed()).isZero();
        assertThat(jdbc().queryForObject("SELECT worker_lease_until > UTC_TIMESTAMP(6) FROM notification WHERE notification_id = ?",
                Boolean.class, firstId)).isTrue();
        System.out.println("REBALANCE_IN_FLIGHT status=PROCESSING committed=0 state=PREPARING_REBALANCE leaseActive=true");
        Files.writeString(directory.resolve("release"), "continue");
        awaitAssignments(2);
        awaitDrained();
        int afterJoin = ids.size();

        // Stop a drained Relay, accept work while it is absent, then restart a real JVM.
        stop(relay);
        int beforeRelayRestart = ids.size();
        submitPartitionCoverage();
        var pendingIds = new ArrayList<>(ids.subList(beforeRelayRestart, ids.size()));
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PENDING'",
                Integer.class)).isEqualTo(pendingIds.size());
        assertThat(jdbc().queryForList("SELECT notification_id FROM outbox_event WHERE status = 'PENDING'",
                String.class)).containsExactlyInAnyOrderElementsOf(pendingIds);
        var eventIds = jdbc().queryForList("SELECT event_id FROM outbox_event ORDER BY event_id", String.class);
        var taskIds = jdbc().queryForList("SELECT task_id FROM delivery_task ORDER BY task_id", String.class);
        Process nextRelay = startChild("relay-restarted", true, false);
        assertThat(nextRelay.pid()).isNotEqualTo(relay.pid());
        awaitDrained();
        assertThat(jdbc().queryForList("SELECT event_id FROM outbox_event ORDER BY event_id", String.class)).isEqualTo(eventIds);
        assertThat(jdbc().queryForList("SELECT task_id FROM delivery_task ORDER BY task_id", String.class)).isEqualTo(taskIds);
        assertThat(childCounter("worker-a", "notification_provider_attempts_total")).isPositive();
        double retiredCalls = childCounter("worker-b", "notification_provider_attempts_total");
        assertThat(retiredCalls).isPositive();
        assertThat(childCounter("worker-b", "notification_worker_failures_total")).isZero();

        // Remove an idle member, verify takeover, then bring it back with a different PID.
        stop(workerB);
        awaitAssignments(1);
        double beforeTakeover = childCounter("worker-a", "notification_provider_attempts_total");
        int beforeTakeoverCount = ids.size();
        submitPartitionCoverage();
        awaitDrained();
        assertThat(childCounter("worker-a", "notification_provider_attempts_total") - beforeTakeover)
                .isEqualTo(ids.size() - beforeTakeoverCount);
        Process nextWorker = startChild("worker-b-restarted", false, false);
        assertThat(nextWorker.pid()).isNotEqualTo(workerB.pid());
        awaitAssignments(2);
        submitPartitionCoverage();
        awaitDrained();
        double newCalls = childCounter("worker-b-restarted", "notification_provider_attempts_total");
        assertThat(newCalls).isPositive();
        double totalCalls = childCounter("worker-a", "notification_provider_attempts_total") + retiredCalls + newCalls;
        assertThat(totalCalls).isEqualTo(ids.size());
        for (String name : List.of("worker-a", "worker-b-restarted")) {
            assertThat(childCounter(name, "notification_worker_failures_total")).isZero();
        }
        assertThat(topicSize(topic + ".retry")).isZero();
        assertThat(topicSize(topic + ".dlq")).isZero();
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(gauge("outbox.unpublished")).isZero();
        assertThat(gauge("notification.consumer.lag")).isZero();
        System.out.printf("WORKER_REBALANCE accepted=%d afterJoin=%d providerAttempts=%.0f records=%d committed=%d members=2 lag=0%n",
                ids.size(), afterJoin, totalCalls, topicSize(topic), committed());
    }

    private String submit() throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"recipientId":"synthetic-user","title":"리밸런스 실험","body":"가상 데이터"}
                        """)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        String id = app.getBean(ObjectMapper.class).readTree(response.body()).path("notificationId").asText();
        ids.add(id);
        return id;
    }

    private void submitPartitionCoverage() throws Exception {
        // Production keyed String records use Kafka's default murmur2 partitioning (3 partitions).
        // Bound the random API UUID sample instead of assuming a fixed batch covers all partitions.
        Set<Integer> partitions = new HashSet<>();
        for (int count = 0; count < 60 && partitions.size() < 3; count++) {
            String id = submit();
            partitions.add(Utils.toPositive(Utils.murmur2(id.getBytes(StandardCharsets.UTF_8))) % 3);
        }
        assertThat(partitions).containsExactlyInAnyOrder(0, 1, 2);
    }

    private ConsumerGroupDescription description() throws Exception {
        return admin.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS).get(group);
    }

    private void awaitAssignments(int members) {
        await().atMost(Duration.ofSeconds(75)).untilAsserted(() -> {
            var description = description();
            assertThat(description.state()).isEqualTo(ConsumerGroupState.STABLE);
            assertThat(description.members()).hasSize(members);
            var assigned = new ArrayList<TopicPartition>();
            for (var member : description.members()) {
                var partitions = member.assignment().topicPartitions();
                assertThat(partitions.stream().filter(p -> p.topic().equals(topic)).count()).isPositive();
                assigned.addAll(partitions);
            }
            var expected = new ArrayList<TopicPartition>();
            for (int partition = 0; partition < 3; partition++) {
                expected.add(new TopicPartition(topic, partition));
                expected.add(new TopicPartition(topic + ".retry", partition));
            }
            assertThat(assigned).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(expected);
            System.out.println("REBALANCE_ASSIGNMENT members=" + members + " partitions=" + assigned.size());
        });
    }

    private void awaitDrained() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(jdbc().queryForList("SELECT notification_id FROM notification WHERE status = 'PROVIDER_ACCEPTED'",
                    String.class)).containsExactlyInAnyOrderElementsOf(ids);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_task WHERE attempt_count = 1", Integer.class))
                    .isEqualTo(ids.size());
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(ids.size());
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt WHERE outcome = 'PROVIDER_ACCEPTED' AND completed_at IS NOT NULL",
                    Integer.class)).isEqualTo(ids.size());
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PUBLISHED' AND attempt_count = 1",
                    Integer.class)).isEqualTo(ids.size());
            assertThat(topicSize(topic)).isEqualTo(ids.size());
            assertThat(committed()).isEqualTo(ids.size());
            // Each individual partition catches up, not just a sum that could hide a gap.
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            var latest = new java.util.HashMap<TopicPartition, OffsetSpec>();
            for (int partition = 0; partition < 3; partition++) latest.put(new TopicPartition(topic, partition), OffsetSpec.latest());
            for (var entry : admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS).entrySet()) {
                assertThat(offsets).containsKey(entry.getKey());
                assertThat(offsets.get(entry.getKey()).offset()).isEqualTo(entry.getValue().offset());
            }
        });
    }

    private void startApi() {
        var args = arguments();
        args.add("--app.async.worker-enabled=false");
        args.add("--app.async.relay-enabled=false");
        args.add("--spring.datasource.password=" + MYSQL.getPassword());
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class)
                .run(args.toArray(String[]::new));
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 5000, "request.timeout.ms", 3000));
    }

    private Process startChild(String name, boolean relay, boolean gate) throws Exception {
        String classpath = System.getProperty("notificationLab.testClasspath");
        assertThat(classpath).as("Run through the Gradle test task to supply the child JVM classpath").isNotBlank();
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", classpath, relay ? RelayProcess.class.getName() : RebalanceWorkerProcess.class.getName()));
        command.addAll(arguments());
        command.add("--app.async.relay-enabled=" + relay);
        command.add("--app.async.worker-enabled=" + !relay);
        var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve(name + ".log").toFile());
        builder.environment().put("MYSQL_PASSWORD", MYSQL.getPassword());
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword());
        builder.environment().put("NOTIFICATION_LAB_WORKER_PORT", directory.resolve(name + ".port").toString());
        builder.environment().remove("NOTIFICATION_LAB_ACK_MARKER");
        builder.environment().remove("NOTIFICATION_LAB_PROVIDER_GATE");
        if (gate) builder.environment().put("NOTIFICATION_LAB_PROVIDER_GATE", directory.toString());
        Process child = builder.start();
        children.add(child);
        return child;
    }

    private void stop(Process child) throws Exception {
        assertThat(child.isAlive()).isTrue();
        child.destroyForcibly();
        assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(child.exitValue()).isNotZero();
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
