package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlabtest.OffsetCommitPolicy;
import dev.jinhyeon.notificationlab.provider.HttpPushProvider;
import dev.jinhyeon.notificationlab.provider.MockPushProvider;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import dev.jinhyeon.notificationmock.MockPushEndpoint;
import dev.jinhyeon.notificationmock.MockServerApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
class HttpWorkerTest {
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

    @ParameterizedTest
    @CsvSource({"RECORD,SUCCESS,0,PROVIDER_ACCEPTED,1,1,", "COUNT_TIME,SUCCESS,0,PROVIDER_ACCEPTED,1,1,",
            "RECORD,SERVER_ERROR,0,RETRYABLE_FAILURE,3,0,HTTP_STATUS_500", "COUNT_TIME,SERVER_ERROR,0,RETRYABLE_FAILURE,3,0,HTTP_STATUS_500",
            "RECORD,TOO_MANY_REQUESTS,0,RETRYABLE_FAILURE,3,0,HTTP_STATUS_429", "COUNT_TIME,TOO_MANY_REQUESTS,0,RETRYABLE_FAILURE,3,0,HTTP_STATUS_429",
            "RECORD,INVALID_RECIPIENT,0,PERMANENT_FAILURE,1,0,HTTP_STATUS_400", "COUNT_TIME,INVALID_RECIPIENT,0,PERMANENT_FAILURE,1,0,HTTP_STATUS_400",
            "RECORD,SUCCESS,700,RETRYABLE_FAILURE,3,3,HTTP_UNKNOWN_TIMEOUT", "COUNT_TIME,SUCCESS,700,RETRYABLE_FAILURE,3,3,HTTP_UNKNOWN_TIMEOUT"})
    void deliversThroughHttpAndPersistsBoundedAttemptsBeforeOffsets(OffsetCommitPolicy.Mode offsetMode, String mode, int delay, String finalStatus,
                                                                   int attempts, int accepted, String error) throws Exception {
        mock = MockServerApplication.start("--server.port=0", "--mock.http.mode=" + mode,
                "--mock.http.response-delay-ms=" + delay, "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        String topic = "http-worker-" + UUID.randomUUID();
        String group = "http-worker-" + UUID.randomUUID();
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class, OffsetCommitPolicy.Wiring.class).run(
                "--notification.test.offset.mode=" + offsetMode.name(),
                "--server.port=0", "--app.provider.mode=HTTP",
                "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort(),
                "--app.provider.http.response-timeout-ms=" + (delay == 0 ? 1000 : 200),
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(), "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--app.delivery.retry-base-delay-ms=100", "--app.delivery.retry-max-delay-ms=100",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        assertThat(app.getBean(PushProvider.class)).isInstanceOf(HttpPushProvider.class);
        assertThat(app.getBeansOfType(MockPushProvider.class)).isEmpty();
        OffsetCommitPolicy.requireApplied(app);
        var jdbc = app.getBean(JdbcTemplate.class);
        try (var http = HttpClient.newHttpClient();
             var admin = Admin.create(java.util.Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.getWebServer().getPort() + "/notifications"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {"recipientId":"synthetic-user","title":"HTTP 연동","body":"가상 데이터"}
                            """)).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(202);
            String id = new ObjectMapper().readTree(response.body()).path("notificationId").asText();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(jdbc.queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, id))
                        .isEqualTo(finalStatus);
                assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE notification_id = ? AND status <> 'PUBLISHED'", Integer.class, id)).isZero();
                long committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                        .entrySet().stream().filter(e -> e.getKey().topic().equals(topic) || e.getKey().topic().equals(topic + ".retry"))
                        .mapToLong(e -> e.getValue().offset()).sum();
                assertThat(committed).isEqualTo(attempts);
            });
            assertThat(jdbc.queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, id)).isEqualTo(attempts);
            var history = jdbc.queryForList("SELECT a.outcome, a.error_code FROM delivery_attempt a JOIN delivery_task t USING (task_id) WHERE t.notification_id = ? ORDER BY a.attempt_no", id);
            assertThat(history).hasSize(attempts).allSatisfy(row -> {
                assertThat(row.get("outcome")).isEqualTo(finalStatus);
                assertThat(row.get("error_code")).isEqualTo(error);
            });
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_DEAD_LETTER'", Integer.class, id))
                    .isEqualTo(accepted == 1 ? 0 : 1);
            var requested = new LinkedHashMap<TopicPartition, OffsetSpec>();
            for (String name : List.of(topic, topic + ".retry", topic + ".dlq")) {
                for (int p = 0; p < 3; p++) requested.put(new TopicPartition(name, p), OffsetSpec.latest());
            }
            var ends = admin.listOffsets(requested).all().get(5, TimeUnit.SECONDS);
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            for (var entry : ends.entrySet()) {
                if (!entry.getKey().topic().equals(topic + ".dlq")) {
                    long committed = offsets.containsKey(entry.getKey()) ? offsets.get(entry.getKey()).offset() : 0;
                    assertThat(committed).isEqualTo(entry.getValue().offset());
                }
            }
            assertThat(ends.entrySet().stream().filter(e -> e.getKey().topic().equals(topic)).mapToLong(e -> e.getValue().offset()).sum()).isEqualTo(1);
            assertThat(ends.entrySet().stream().filter(e -> e.getKey().topic().equals(topic + ".retry")).mapToLong(e -> e.getValue().offset()).sum()).isEqualTo(attempts - 1);
            assertThat(ends.entrySet().stream().filter(e -> e.getKey().topic().equals(topic + ".dlq")).mapToLong(e -> e.getValue().offset()).sum()).isEqualTo(accepted == 1 ? 0 : 1);
            await().atMost(Duration.ofSeconds(3)).until(() -> mock.getBean(MockPushEndpoint.class).stats().active() == 0);
            var stats = mock.getBean(MockPushEndpoint.class).stats();
            assertThat(stats.requests()).isEqualTo(attempts);
            assertThat(stats.accepted()).isEqualTo(accepted);
            System.out.printf("HTTP_WORKER offsetMode=%s mode=%s delayMs=%d status=%s attempts=%d accepted=%d committed=%d%n",
                    offsetMode, mode, delay, finalStatus, attempts, accepted, attempts);
        }
    }

    @AfterEach
    void close() {
        if (app != null) app.close();
        if (mock != null) mock.close();
    }
}
