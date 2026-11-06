package dev.jinhyeon.notificationlab.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlabtest.TopologyNodeProcess;
import dev.jinhyeon.notificationmock.MockServerApplication;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Correctness of a local four-JVM deployment, not a throughput or physical-host HA benchmark. */
@Testcontainers
class LocalTopologyIntegrationTest {
    @Container
    final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4.8")
            .withDatabaseName("notification_lab").withUsername("notification_test")
            .withPassword(UUID.randomUUID().toString()).withCommand("--default-time-zone=+00:00")
            .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
            .withCreateContainerCmdModifier(cmd -> bound(cmd, 512));
    @Container
    final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.2")
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withCreateContainerCmdModifier(cmd -> bound(cmd, 1024));
    @Container
    final GenericContainer<?> redis = new GenericContainer<>("redis:7.4.11-alpine")
            .withExposedPorts(6379).withCommand("redis-server", "--maxmemory", "16mb", "--maxmemory-policy", "noeviction")
            .withCreateContainerCmdModifier(cmd -> bound(cmd, 64));

    private final ObjectMapper mapper = new ObjectMapper();
    private final String suffix = UUID.randomUUID().toString();
    private final String topic = "topology-" + suffix;
    private final String group = "topology-worker-" + suffix;
    private final String quotaKey = "notification-lab:topology:" + suffix;
    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final Map<String, Integer> expected = new LinkedHashMap<>();
    private final Map<String, Integer> routes = new LinkedHashMap<>();
    private final Map<String, Object> report = new LinkedHashMap<>();
    private final List<Map<String, Object>> phases = new ArrayList<>();
    private final Audit audit = new Audit();
    private Path directory;
    private HttpClient http;
    private Admin admin;
    private JdbcTemplate jdbc;
    private ServletWebServerApplicationContext mock;
    private GenericContainer<?> lb;
    private int ordinal;
    private String base;
    private boolean passed;

    @Test
    @Timeout(360)
    void distributesApiRequestsAndSharesQuotaThenSurvivesSettledApiAndWorkerRemoval() throws Exception {
        directory = Files.createDirectories(Path.of("build/reports/local-topology", Instant.now().toEpochMilli() + "-" + suffix));
        report.put("schemaVersion", 1);
        report.put("scope", "single-host REALTIME; settled API/Worker removal; HTTP Mock acceptance only");
        report.put("startedAt", Instant.now().toString());
        report.put("phases", phases);
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                "default.api.timeout.ms", 5000, "request.timeout.ms", 3000));
        jdbc = new JdbcTemplate(new DriverManagerDataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()));
        initializeQuota();
        startMock();
        startNode("api1", false);
        startNode("api2", false);
        startNode("worker1", true);
        startNode("worker2", true);
        assertThat(nodes.values().stream().map(node -> node.process.pid()).distinct().count()).isEqualTo(4);
        awaitAssignments(2);
        startLb();
        awaitBackend("api1", "UP");
        awaitBackend("api2", "UP");
        freezeBudget();
        String key = "topology-replay-" + suffix;
        String firstId = submit(key, 1, 202, null);
        assertThat(submit(key, 1, 200, null)).isEqualTo(firstId);
        assertThat(routes).containsEntry("api1", 1).containsEntry("api2", 1);
        coverage(3);
        assertThat(routes.keySet()).containsExactlyInAnyOrder("api1", "api2");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(metric("worker1", "notification_worker_quota_pauses_pending")).isEqualTo(1);
            assertThat(metric("worker2", "notification_worker_quota_pauses_pending")).isEqualTo(1);
            assertThat(count("delivery_attempt", "1=1")).isEqualTo(2);
            assertThat(audit.count()).isEqualTo(2);
            assertThat(committedCount()).isEqualTo(2);
        });
        await().during(Duration.ofMillis(1200)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            assertThat(audit.count()).isEqualTo(2);
            assertThat(count("delivery_task", "attempt_count = 0")).isEqualTo(expected.size() - 2);
            assertThat(count("notification", "status = 'QUEUED'")).isEqualTo(expected.size() - 2);
            assertThat(committedCount()).isEqualTo(2);
        });
        try (var connection = connection()) {
            assertThat(connection.hget(quotaKey, "credit")).isEqualTo("0");
        }
        var pausedOffsets = pausedOffsets();
        assertThat(audit.count()).isEqualTo(2);
        phases.add(Map.of("phase", "shared-budget-frozen", "offsets", pausedOffsets, "logicalTasks", expected.size(),
                "httpAccepted", 2, "committed", 2, "pausedWorkers", 2, "fixtureCredits", 2,
                "apiRoutes", Map.copyOf(routes)));
        // Test-only release of the frozen clock. Existing pause deadlines are intentionally respected.
        try (var connection = connection()) { connection.hset(quotaKey, "last_ms", "0"); }
        drained();
        assertThat(metric("worker1", "notification_provider_attempts_total")).isPositive();
        assertThat(metric("worker2", "notification_provider_attempts_total")).isPositive();
        snapshot("two-api-two-worker", 2);

        kill("api1"); // No in-flight POST: this is a completed-request replay boundary.
        awaitBackend("api1", "DOWN");
        assertThat(submit(key, 1, 200, "api2")).isEqualTo(firstId);
        var conflict = post(key, payload(1).replace("synthetic body", "different body"));
        assertThat(conflict.statusCode()).isEqualTo(409);
        assertThat(conflict.headers().firstValue("X-Notification-Lab-Upstream")).contains("api2");
        coverage(1);
        drained();
        snapshot("api1-down-replay-and-conflict", 2);

        double retiredCommitRetries = metric("worker2", "notification_kafka_commit_wakeup_retries_total");
        double retiredCalls = metric("worker2", "notification_provider_attempts_total");
        double survivorBefore = metric("worker1", "notification_provider_attempts_total");
        int before = expected.size();
        kill("worker2"); // The phase above verified DB completion and every committed offset first.
        awaitAssignments(1);
        coverage(1);
        drained();
        assertThat(metric("worker1", "notification_provider_attempts_total") - survivorBefore).isEqualTo(expected.size() - before);
        assertThat(metric("worker1", "notification_provider_attempts_total") + retiredCalls).isEqualTo(expected.size());
        snapshot("worker2-down-six-partition-takeover", 1);
        report.put("distinctApplicationProcesses", 4);
        report.put("roleAssertions", Map.of("apiListenersRunning", false, "apiRelayPresent", false,
                "workerListenersRunning", true, "workerRelayPresent", true));
        report.put("wakeupCommitRetries", retiredCommitRetries + metric("worker1", "notification_kafka_commit_wakeup_retries_total"));
        report.put("logicalTasks", expected.size());
        report.put("httpAudit", audit.summary());
        report.put("apiRoutes", routes);
        report.put("proxyImage", lb.getDockerImageName());
        report.put("proxyImageId", lb.getContainerInfo().getImageId());
        passed = true;
        System.out.println("LOCAL_TOPOLOGY tasks=" + expected.size() + " phases=" + phases.size() + " duplicates=0 invalid=0 lag=0");
    }

    private void initializeQuota() throws Exception {
        try (var connection = connection()) {
            assertThat(connection.eval(new ClassPathResource("redis/initialize-provider-quota.lua")
                    .getContentAsString(StandardCharsets.UTF_8), List.of(quotaKey), List.of("10", "2")))
                    .isEqualTo("QUOTA_INITIALIZED_EMPTY");
        }
    }

    private void freezeBudget() {
        try (var connection = connection()) {
            var time = connection.time();
            long now = Long.parseLong(time.get(0)) * 1000 + Long.parseLong(time.get(1)) / 1000;
            // Controlled two-token fixture: avoids mistaking timing noise for a shared-budget proof.
            connection.hset(quotaKey, Map.of("credit", "2000", "last_ms", Long.toString(now + 30000)));
        }
    }

    private Jedis connection() { return new Jedis("127.0.0.1", redis.getMappedPort(6379), 1000); }

    private void startMock() {
        var application = new SpringApplication(MockServerApplication.class, AuditConfiguration.class);
        application.setDefaultProperties(Map.of("spring.config.name", "mock-server"));
        application.addInitializers(context -> context.getBeanFactory().registerSingleton("topologyAuditState", audit));
        mock = (ServletWebServerApplicationContext) application.run("--server.port=0", "--mock.http.mode=SUCCESS",
                "--mock.http.response-delay-ms=1", "--mock.http.max-in-flight=4", "--mock.http.requests-per-window=1000",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
    }

    private void startNode(String name, boolean worker) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", System.getProperty("notificationLab.testClasspath"), TopologyNodeProcess.class.getName(),
                "--server.port=0", "--server.tomcat.threads.max=8", "--server.tomcat.threads.min-spare=2",
                "--spring.datasource.url=" + mysql.getJdbcUrl(), "--spring.datasource.username=" + mysql.getUsername(),
                "--spring.datasource.hikari.maximum-pool-size=3", "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--spring.kafka.consumer.properties.session.timeout.ms=6000", "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000",
                "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--app.async.relay-enabled=" + worker, "--app.async.worker-enabled=" + worker,
                "--app.provider.mode=HTTP", "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort(),
                "--app.rate-limit.client-enabled=true", "--app.rate-limit.port=" + redis.getMappedPort(6379),
                "--app.rate-limit.key=" + quotaKey, "--app.rate-limit.rate-per-second=10", "--app.rate-limit.burst=2",
                "--app.worker.queue-capacity=0", "--logging.level.root=WARN", "--spring.main.banner-mode=off"));
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(directory.resolve(name + ".log").toFile());
        builder.environment().put("MYSQL_PASSWORD", mysql.getPassword());
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", mysql.getPassword());
        Path ready = directory.resolve(name + ".json");
        builder.environment().put("NOTIFICATION_LAB_TOPOLOGY_READY", ready.toString());
        var node = new Node(builder.start(), ready);
        nodes.put(name, node); // Track immediately, including a child that fails during startup.
        await().atMost(Duration.ofSeconds(75)).untilAsserted(() -> {
            assertThat(node.process.isAlive()).as("Child %s alive; inspect its ignored report log on failure", name).isTrue();
            assertThat(ready).exists();
            var metadata = mapper.readTree(Files.readString(ready));
            assertThat(metadata.path("pid").asLong()).isEqualTo(node.process.pid());
            assertThat(metadata.path("listenerCount").asInt()).isEqualTo(1);
            assertThat(metadata.path("listenerRunning").asBoolean()).isEqualTo(worker);
            assertThat(metadata.path("relayPresent").asBoolean()).isEqualTo(worker);
            assertThat(get("http://127.0.0.1:" + node.port() + "/actuator/health").statusCode()).isEqualTo(200);
        });
    }

    private void startLb() throws Exception {
        int port1 = nodes.get("api1").port();
        int port2 = nodes.get("api2").port();
        org.testcontainers.Testcontainers.exposeHostPorts(port1, port2);
        var configuration = Files.readString(Path.of("deploy/local/haproxy.cfg"))
                .replace("host.docker.internal:18010", "host.testcontainers.internal:" + port1)
                .replace("host.docker.internal:18011", "host.testcontainers.internal:" + port2);
        Path config = directory.resolve("haproxy.cfg");
        Files.writeString(config, configuration);
        lb = new GenericContainer<>("haproxy:3.4.7-alpine").withExposedPorts(8080, 8404)
                .withCopyFileToContainer(MountableFile.forHostPath(config), "/usr/local/etc/haproxy/haproxy.cfg")
                .withCreateContainerCmdModifier(cmd -> bound(cmd, 128));
        lb.start();
        var syntax = lb.execInContainer("haproxy", "-c", "-f", "/usr/local/etc/haproxy/haproxy.cfg");
        assertThat(syntax.getExitCode()).isZero();
        base = "http://127.0.0.1:" + lb.getMappedPort(8080);
    }

    private void awaitBackend(String name, String status) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var response = get("http://127.0.0.1:" + lb.getMappedPort(8404) + "/stats;csv");
            assertThat(response.statusCode()).isEqualTo(200);
            var lines = response.body().lines().toList();
            var columns = Arrays.asList(lines.getFirst().replaceFirst("^# ", "").split(",", -1));
            int statusColumn = columns.indexOf("status");
            assertThat(statusColumn).isGreaterThanOrEqualTo(0);
            var row = lines.stream().filter(line -> line.startsWith("api_nodes," + name + ",")).findFirst().orElseThrow();
            assertThat(row.split(",", -1)[statusColumn]).isEqualTo(status);
        });
    }

    private String payload(int value) throws Exception {
        return mapper.writeValueAsString(Map.of("recipientId", "topology-synthetic-" + value,
                "title", "topology", "body", "synthetic body"));
    }

    private HttpResponse<String> post(String key, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(base + "/notifications")).timeout(Duration.ofSeconds(7))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        if (key != null) request.header("Idempotency-Key", key);
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String submit(String key, int value, int status, String upstream) throws Exception {
        ordinal = Math.max(ordinal, value);
        assertThat(ordinal).isLessThanOrEqualTo(181);
        var response = post(key, payload(value));
        assertThat(response.statusCode()).isEqualTo(status);
        String route = response.headers().firstValue("X-Notification-Lab-Upstream").orElseThrow();
        assertThat(route).isIn("api1", "api2");
        if (upstream != null || !nodes.get("api1").process.isAlive()) assertThat(route).isEqualTo("api2");
        routes.merge(route, 1, Integer::sum);
        String id = mapper.readTree(response.body()).path("notificationId").asText();
        UUID.fromString(id);
        if (status == 202) assertThat(expected.putIfAbsent(id, value)).isNull();
        else assertThat(expected).containsEntry(id, value);
        return id;
    }

    private void coverage(int minimumPerPartition) throws Exception {
        int[] counts = new int[3];
        for (int samples = 0; samples < 60 && Arrays.stream(counts).min().orElseThrow() < minimumPerPartition; samples++) {
            String id = submit(null, ordinal + 1, 202, null);
            counts[Utils.toPositive(Utils.murmur2(id.getBytes(StandardCharsets.UTF_8))) % 3]++;
        }
        assertThat(Arrays.stream(counts).min().orElseThrow()).isGreaterThanOrEqualTo(minimumPerPartition);
    }

    private void awaitAssignments(int members) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            var description = admin.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS).get(group);
            assertThat(description.state()).isEqualTo(ConsumerGroupState.STABLE);
            assertThat(description.members()).hasSize(members);
            var actual = new ArrayList<TopicPartition>();
            var wanted = new ArrayList<TopicPartition>();
            for (var member : description.members()) {
                var partitions = member.assignment().topicPartitions();
                assertThat(partitions.stream().anyMatch(partition -> partition.topic().equals(topic))).isTrue();
                actual.addAll(partitions);
            }
            for (int i = 0; i < 3; i++) {
                wanted.add(new TopicPartition(topic, i));
                wanted.add(new TopicPartition(topic + ".retry", i));
            }
            assertThat(actual).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(wanted);
        });
    }

    private int count(String table, String predicate) { return jdbc.queryForObject("SELECT COUNT(*) FROM " + table + " WHERE " + predicate, Integer.class); }

    private List<Map<String, Object>> pausedOffsets() throws Exception {
        var positions = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        var partitions = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2));
        try (var verifier = new KafkaConsumer<String, String>(Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "enable.auto.commit", false, "max.poll.records", 181, "default.api.timeout.ms", 5000,
                "request.timeout.ms", 3000, "fetch.max.bytes", 1048576, "max.partition.fetch.bytes", 1048576))) {
            verifier.assign(partitions);
            verifier.seekToBeginning(partitions);
            var observed = new HashMap<String, Integer>();
            var firstUnfinished = new HashMap<TopicPartition, Long>();
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (observed.size() < expected.size() && System.nanoTime() < deadline) {
                for (var record : verifier.poll(Duration.ofMillis(100))) {
                    assertThat(observed.size()).isLessThan(181);
                    assertThat(observed.putIfAbsent(record.key(), 1)).isNull();
                    var partition = new TopicPartition(topic, record.partition());
                    long committed = positions.containsKey(partition) ? positions.get(partition).offset() : 0;
                    String status = jdbc.queryForObject("SELECT status FROM notification WHERE notification_id=?", String.class, record.key());
                    if (record.offset() < committed) assertThat(status).isEqualTo("PROVIDER_ACCEPTED");
                    if (!status.equals("PROVIDER_ACCEPTED")) {
                        assertThat(committed).isLessThanOrEqualTo(record.offset());
                        firstUnfinished.merge(partition, record.offset(), Math::min);
                    }
                }
            }
            assertThat(observed.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
            var result = new ArrayList<Map<String, Object>>();
            for (var partition : partitions) {
                var committed = positions.get(partition);
                result.add(Map.of("role", "requested", "partition", partition.partition(),
                        "committedPresent", committed != null, "committed", committed == null ? -1 : committed.offset(),
                        "firstUnfinished", firstUnfinished.getOrDefault(partition, -1L)));
            }
            return result;
        }
    }

    private long committedCount() throws Exception {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .entrySet().stream().filter(entry -> entry.getKey().topic().equals(topic)).mapToLong(entry -> entry.getValue().offset()).sum();
    }

    private List<Map<String, Object>> offsets() throws Exception {
        var latest = new HashMap<TopicPartition, OffsetSpec>();
        for (var name : List.of(topic, topic + ".retry", topic + ".dlq")) {
            for (int p = 0; p < 3; p++) latest.put(new TopicPartition(name, p), OffsetSpec.latest());
        }
        var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        var ends = admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS);
        var result = new ArrayList<Map<String, Object>>();
        for (var partition : latest.keySet().stream().sorted(java.util.Comparator.comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition)).toList()) {
            long end = ends.get(partition).offset();
            var position = committed.get(partition);
            String role = partition.topic().equals(topic) ? "requested" : partition.topic().endsWith(".retry") ? "retry" : "dlq";
            if (role.equals("requested")) {
                assertThat(position).isNotNull();
                assertThat(position.offset()).isEqualTo(end);
            } else assertThat(end).isZero();
            result.add(Map.of("role", role, "partition", partition.partition(), "end", end,
                    "committedPresent", position != null, "committed", position == null ? -1 : position.offset()));
        }
        return result;
    }

    private void drained() {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(jdbc.queryForList("SELECT notification_id FROM notification WHERE status='PROVIDER_ACCEPTED'", String.class))
                    .containsExactlyInAnyOrderElementsOf(expected.keySet());
            assertThat(count("notification", "1=1")).isEqualTo(expected.size());
            assertThat(count("delivery_task", "attempt_count=1")).isEqualTo(expected.size());
            assertThat(count("delivery_attempt", "outcome='PROVIDER_ACCEPTED' AND completed_at IS NOT NULL")).isEqualTo(expected.size());
            assertThat(count("delivery_attempt", "1=1")).isEqualTo(expected.size());
            assertThat(count("outbox_event", "status='PUBLISHED'")).isEqualTo(expected.size());
            assertThat(count("outbox_event", "1=1")).isEqualTo(expected.size());
            assertThat(committedCount()).isEqualTo(expected.size());
            offsets();
            audit.assertMatches(expected);
            audit.assertProviderResults(jdbc);
        });
    }

    private void snapshot(String phase, int members) throws Exception {
        awaitAssignments(members);
        phases.add(Map.of("phase", phase, "logicalTasks", expected.size(), "httpAudit", audit.summary(),
                "members", members, "assignedPartitions", 6, "offsets", offsets()));
    }

    private HttpResponse<String> get(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private double metric(String name, String metric) throws Exception {
        var response = get("http://127.0.0.1:" + nodes.get(name).port() + "/actuator/prometheus");
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body().lines().filter(line -> line.startsWith(metric + " "))
                .mapToDouble(line -> Double.parseDouble(line.substring(metric.length() + 1))).sum();
    }

    private void kill(String name) throws Exception {
        var child = nodes.get(name).process;
        assertThat(child.isAlive()).isTrue();
        child.destroyForcibly();
        assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(child.exitValue()).isNotZero();
    }

    private static void bound(com.github.dockerjava.api.command.CreateContainerCmd cmd, int memoryMiB) {
        cmd.getHostConfig().withMemory(memoryMiB * 1024L * 1024L).withNanoCPUs(1_000_000_000L)
                .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port ->
                        new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList());
    }

    private class Node {
        private final Process process;
        private final Path ready;
        Node(Process process, Path ready) { this.process = process; this.ready = ready; }
        int port() throws IOException { return mapper.readTree(Files.readString(ready)).path("port").asInt(); }
    }

    @AfterEach
    void close() throws Exception {
        var failures = new ArrayList<Throwable>();
        for (var node : nodes.values()) {
            try {
                if (node.process.isAlive()) node.process.destroy();
                if (!node.process.waitFor(12, TimeUnit.SECONDS)) {
                    node.process.destroyForcibly();
                    assertThat(node.process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                }
            } catch (Throwable failure) { failures.add(failure); }
        }
        for (var resource : new AutoCloseable[]{lb, mock, admin, http}) {
            if (resource != null) try { resource.close(); } catch (Throwable failure) { failures.add(failure); }
        }
        if (directory != null) {
            report.put("passed", passed && failures.isEmpty());
            report.put("cleanupFailures", failures.size());
            report.put("finishedAt", Instant.now().toString());
            try { mapper.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("result.json").toFile(), report); }
            catch (Throwable failure) { failures.add(failure); }
        }
        if (!failures.isEmpty()) {
            var failure = new AssertionError("Topology cleanup/report failed");
            failures.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    static class Audit {
        private record Observation(JsonNode payload, String providerMessageId) { }
        private final Map<String, Observation> seen = new HashMap<>();
        private int duplicates;
        private int invalid;
        synchronized int count() { return seen.size() + duplicates + invalid; }
        synchronized Map<String, Integer> summary() { return Map.of("unique", seen.size(), "duplicates", duplicates, "invalid", invalid); }
        synchronized void observe(byte[] body, int status, byte[] response) {
            try {
                var value = new ObjectMapper().readTree(body);
                String id = value.path("notificationId").asText();
                UUID.fromString(id);
                if (status != 202 || seen.size() >= 181) { invalid++; return; }
                String providerId = new ObjectMapper().readTree(response).path("providerMessageId").asText();
                if (!providerId.matches("http-mock-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}")) { invalid++; return; }
                if (seen.putIfAbsent(id, new Observation(value, providerId)) != null) duplicates++;
            } catch (Exception failure) { invalid++; }
        }
        synchronized void assertProviderResults(JdbcTemplate jdbc) {
            jdbc.query("SELECT notification_id, provider_message_id FROM notification", (org.springframework.jdbc.core.RowCallbackHandler) row ->
                    assertThat(row.getString(2)).isEqualTo(seen.get(row.getString(1)).providerMessageId()));
        }
        synchronized void assertMatches(Map<String, Integer> expected) {
            assertThat(duplicates).isZero();
            assertThat(invalid).isZero();
            assertThat(seen.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
            expected.forEach((id, ordinal) -> {
                var value = seen.get(id).payload();
                assertThat(value.path("recipientId").asText()).isEqualTo("topology-synthetic-" + ordinal);
                assertThat(value.path("title").asText()).isEqualTo("topology");
                assertThat(value.path("body").asText()).isEqualTo("synthetic body");
            });
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class AuditConfiguration {
        @Bean
        FilterRegistrationBean<OncePerRequestFilter> topologyAudit(Audit audit) {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>();
            registration.setFilter(new OncePerRequestFilter() {
                @Override
                protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
                        throws ServletException, IOException {
                    var cached = new ContentCachingRequestWrapper(request, 8192);
                    var result = new ContentCachingResponseWrapper(response);
                    try { chain.doFilter(cached, result); }
                    finally {
                        audit.observe(cached.getContentAsByteArray(), result.getStatus(), result.getContentAsByteArray());
                        result.copyBodyToResponse();
                    }
                }
            });
            registration.addUrlPatterns("/pushes");
            return registration;
        }
    }
}
