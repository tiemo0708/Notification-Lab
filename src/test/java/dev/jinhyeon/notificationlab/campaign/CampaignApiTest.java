package dev.jinhyeon.notificationlab.campaign;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.config.TrafficRouting;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class CampaignApiTest {
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
    private static ServletWebServerApplicationContext first;
    private static ServletWebServerApplicationContext second;
    private static ServletWebServerApplicationContext realtime;
    private static HttpClient http;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PAYLOAD = "{\"name\":\"테스트 캠페인 🌱\",\"title\":\"알림\",\"body\":\"가상 데이터\",\"targetCount\":100000}";
    private static final String TOPIC = "campaign-api-" + UUID.randomUUID();

    @BeforeAll
    static void open() {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        first = application("CAMPAIGN"); second = application("CAMPAIGN"); realtime = application("REALTIME");
    }

    private static ServletWebServerApplicationContext application(String lane) {
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class).run(
                "--server.port=0", "--app.traffic.lane=" + lane, "--app.traffic.split-enabled=true",
                "--app.rate-limit.client-enabled=true", "--app.async.worker-enabled=false", "--app.async.relay-enabled=false",
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(), "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + TOPIC, "--app.async.group=" + TOPIC,
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
    }

    @BeforeEach
    void clearCampaigns() { jdbc().update("DELETE FROM campaign"); }

    @AfterAll
    static void close() {
        if (realtime != null) realtime.close();
        if (second != null) second.close();
        if (first != null) first.close();
        if (http != null) http.close();
    }

    @Test
    void createStartAndRestartPreserveDurableIntentWithoutSendingAnything() throws Exception {
        String key = UUID.randomUUID().toString();
        var created = call(first, "POST", "/campaigns", PAYLOAD, key);
        assertThat(created.statusCode()).isEqualTo(201);
        String location = created.headers().firstValue("Location").orElseThrow();
        var draft = json(created);
        assertThat(draft.path("status").asText()).isEqualTo("DRAFT");
        assertThat(draft.path("audience").asText()).isEqualTo("SYNTHETIC_V1");
        assertThat(draft.path("targetCount").asInt()).isEqualTo(100000);
        assertThat(draft.path("startRequestedAt").isNull()).isTrue();
        assertThat(json(call(second, "GET", location, null, null))).isEqualTo(draft);
        var started = call(first, "POST", location + "/start", null, null);
        assertThat(started.statusCode()).isEqualTo(202);
        var requested = json(started);
        assertThat(requested.path("status").asText()).isEqualTo("START_REQUESTED");
        assertThat(requested.path("startRequestedAt").asText()).isNotBlank();
        assertThat(started.headers().firstValue("Location")).contains(location);
        first.close(); first = application("CAMPAIGN");
        var replay = call(first, "POST", location + "/start", null, null);
        assertThat(replay.statusCode()).isEqualTo(200);
        assertThat(json(replay)).isEqualTo(requested);
        assertThat(json(call(first, "POST", "/campaigns", PAYLOAD, key))).isEqualTo(requested);
        assertThat(json(call(second, "GET", location, null, null))).isEqualTo(requested);
        assertNoDispatch();
    }

    @Test
    void concurrentStartsAcrossTwoApplicationsAcceptExactlyOnce() throws Exception {
        String location = create();
        var results = concurrent((app, i) -> call(app, "POST", location + "/start", null, null));
        assertThat(results.stream().filter(r -> r.statusCode() == 202).count()).isEqualTo(1);
        assertThat(results.stream().filter(r -> r.statusCode() == 200).count()).isEqualTo(11);
        var accepted = json(results.getFirst());
        for (var response : results) assertThat(json(response)).isEqualTo(accepted);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM campaign WHERE status = 'START_REQUESTED'", Integer.class)).isEqualTo(1);
        assertNoDispatch();
        System.out.println("CAMPAIGN_START callers=12 apps=2 accepted=1 replays=11 durableRows=1 sends=0");
    }

    @Test
    void concurrentCreateUsesDatabaseUniquenessAndExactPayloadComparison() throws Exception {
        String key = UUID.randomUUID().toString();
        var results = concurrent((app, i) -> call(app, "POST", "/campaigns", PAYLOAD, key));
        assertThat(results.stream().filter(r -> r.statusCode() == 201).count()).isEqualTo(1);
        assertThat(results.stream().filter(r -> r.statusCode() == 200).count()).isEqualTo(11);
        for (var response : results) assertThat(json(response)).isEqualTo(json(results.getFirst()));
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM campaign", Integer.class)).isEqualTo(1);
        for (String changed : List.of(PAYLOAD.replace("캠페인", "캠페인 "), PAYLOAD.replace("알림", "다른 제목"),
                PAYLOAD.replace("가상 데이터", "다른 본문"), PAYLOAD.replace("100000", "99999"))) {
            assertThat(call(second, "POST", "/campaigns", changed, key).statusCode()).isEqualTo(409);
        }
        assertThat(json(call(first, "GET", results.getFirst().headers().firstValue("Location").orElseThrow(), null, null)))
                .isEqualTo(json(results.getFirst()));
    }

    @Test
    void concurrentConflictingCreatesCommitOnlyOneDefinition() throws Exception {
        String key = UUID.randomUUID().toString();
        var results = concurrent((app, i) -> call(app, "POST", "/campaigns", PAYLOAD.replace("100000", Integer.toString(i + 1)), key));
        assertThat(results.stream().filter(r -> r.statusCode() == 201).count()).isEqualTo(1);
        assertThat(results.stream().filter(r -> r.statusCode() == 409).count()).isEqualTo(11);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM campaign", Integer.class)).isEqualTo(1);
    }

    @Test
    void invalidInputsDoNotReserveKeyAndErrorResponsesDoNotEchoPayload() throws Exception {
        String key = UUID.randomUUID().toString();
        for (String body : List.of(PAYLOAD.replace("100000", "0"), PAYLOAD.replace("100000", "1000001"),
                PAYLOAD.replace("100000", "null"), PAYLOAD.replace("알림", " "),
                PAYLOAD.replace("가상 데이터", "secret-payload".repeat(50)), "{invalid-json")) {
            var response = call(first, "POST", "/campaigns", body, key);
            assertThat(response.statusCode()).isEqualTo(400);
            assertThat(response.body()).doesNotContain("secret-payload", "invalid-json", "SQL");
        }
        for (String bad : List.of("bad key", "k".repeat(129))) {
            assertThat(call(first, "POST", "/campaigns", PAYLOAD, bad).statusCode()).isEqualTo(400);
        }
        assertThat(call(first, "POST", "/campaigns", PAYLOAD, null).statusCode()).isEqualTo(400);
        var duplicateHeader = HttpRequest.newBuilder(URI.create(base(first) + "/campaigns"))
                .header("Content-Type", "application/json").header("Idempotency-Key", key).header("Idempotency-Key", key)
                .POST(HttpRequest.BodyPublishers.ofString(PAYLOAD)).build();
        assertThat(http.send(duplicateHeader, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(400);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM campaign", Integer.class)).isZero();
        assertThat(call(first, "POST", "/campaigns", PAYLOAD, key).statusCode()).isEqualTo(201);
        for (String count : List.of("1.5", "100000.0", "\"100000\"", "2147483648", "true", "[]")) {
            assertThat(call(first, "POST", "/campaigns", PAYLOAD.replace("100000", count), key).statusCode()).isEqualTo(400);
        }
        for (int count : new int[]{1, 1000000}) {
            assertThat(call(first, "POST", "/campaigns", PAYLOAD.replace("100000", Integer.toString(count)), UUID.randomUUID().toString()).statusCode()).isEqualTo(201);
        }
    }

    @Test
    void campaignEndpointsAreAbsentOnRealtimeAndUnknownIdsReturn404() throws Exception {
        String location = create();
        for (String path : List.of(location, "/campaigns/" + UUID.randomUUID())) {
            assertThat(call(realtime, "GET", path, null, null).statusCode()).isEqualTo(404);
            assertThat(call(realtime, "POST", path + "/start", null, null).statusCode()).isEqualTo(404);
        }
        assertThat(call(realtime, "POST", "/campaigns", PAYLOAD, UUID.randomUUID().toString()).statusCode()).isEqualTo(404);
        assertThat(call(first, "GET", "/campaigns/" + UUID.randomUUID(), null, null).statusCode()).isEqualTo(404);
        assertThat(call(first, "POST", "/campaigns/" + UUID.randomUUID() + "/start", null, null).statusCode()).isEqualTo(404);
        assertThat(call(first, "POST", "/campaigns/not-a-uuid/start", null, null).statusCode()).isEqualTo(400);
        assertThat(jdbc().queryForObject("SELECT status FROM campaign", String.class)).isEqualTo("DRAFT");
    }

    @Test
    void failedStartStatementRollsBackAndRetryCanAcceptOnce() throws Exception {
        String location = create();
        // Inject a DB failure after the UPDATE, before commit, inside this disposable schema.
        executeAsRoot("CREATE TRIGGER reject_campaign_start AFTER UPDATE ON campaign FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test payload must not escape'");
        try {
            var response = call(first, "POST", location + "/start", null, null);
            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.body()).doesNotContain("test payload", "SQLSTATE", "reject_campaign_start");
            assertThat(jdbc().queryForObject("SELECT status FROM campaign", String.class)).isEqualTo("DRAFT");
            assertThat(jdbc().queryForObject("SELECT start_requested_at FROM campaign", String.class)).isNull();
        } finally { executeAsRoot("DROP TRIGGER reject_campaign_start"); }
        assertThat(call(second, "POST", location + "/start", null, null).statusCode()).isEqualTo(202);
        assertThat(call(first, "POST", location + "/start", null, null).statusCode()).isEqualTo(200);
    }

    @Test
    void databaseConstraintsRejectImpossibleStatesAndOversizedAudiences() throws Exception {
        create();
        for (String sql : List.of("UPDATE campaign SET status = 'RUNNING'", "UPDATE campaign SET status = 'START_REQUESTED'",
                "UPDATE campaign SET start_requested_at = UTC_TIMESTAMP(6)", "UPDATE campaign SET target_count = 0",
                "UPDATE campaign SET target_count = 1000001", "UPDATE campaign SET audience = 'REAL_USERS'")) {
            assertThatThrownBy(() -> jdbc().update(sql)).isInstanceOf(DataAccessException.class)
                    .satisfies(error -> {
                        Throwable cause = ((DataAccessException) error).getMostSpecificCause();
                        assertThat(cause).isInstanceOf(SQLException.class);
                        assertThat(((SQLException) cause).getErrorCode()).isEqualTo(3819);
                    });
        }
        assertThat(jdbc().queryForObject("SELECT status FROM campaign", String.class)).isEqualTo("DRAFT");
    }

    @Test
    void startAndCreateReplayObserveCommittedStateOutsideAnOlderCallerSnapshot() throws Exception {
        String key = UUID.randomUUID().toString();
        var response = call(first, "POST", "/campaigns", PAYLOAD, key);
        UUID id = UUID.fromString(json(response).path("campaignId").asText());
        var tx = new TransactionTemplate(first.getBean(PlatformTransactionManager.class));
        tx.executeWithoutResult(status -> {
            assertThat(jdbc().queryForObject("SELECT status FROM campaign", String.class)).isEqualTo("DRAFT");
            var accepted = second.getBean(CampaignService.class).start(id);
            assertThat(accepted.replayed()).isFalse();
            var replay = first.getBean(CampaignService.class).start(id);
            assertThat(replay.replayed()).isTrue();
            assertThat(replay.campaign()).isEqualTo(accepted.campaign());
            var creation = first.getBean(CampaignService.class).create(new CampaignRequest("테스트 캠페인 🌱", "알림", "가상 데이터", 100000), key);
            assertThat(creation.replayed()).isTrue();
            assertThat(creation.campaign()).isEqualTo(accepted.campaign());
            status.setRollbackOnly();
        });
        assertThat(jdbc().queryForObject("SELECT status FROM campaign", String.class)).isEqualTo("START_REQUESTED");
    }

    private void executeAsRoot(String sql) throws SQLException {
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private interface RequestCall { HttpResponse<String> run(ServletWebServerApplicationContext app, int number) throws Exception; }
    private List<HttpResponse<String>> concurrent(RequestCall call) throws Exception {
        try (var executor = Executors.newFixedThreadPool(12)) {
            var gate = new CountDownLatch(1);
            var results = new ArrayList<Future<HttpResponse<String>>>();
            for (int i = 0; i < 12; i++) {
                int number = i;
                results.add(executor.submit(() -> { assertThat(gate.await(3, TimeUnit.SECONDS)).isTrue(); return call.run(number % 2 == 0 ? first : second, number); }));
            }
            gate.countDown();
            var responses = new ArrayList<HttpResponse<String>>();
            for (var result : results) responses.add(result.get(10, TimeUnit.SECONDS));
            return responses;
        }
    }
    private String create() throws Exception { return call(first, "POST", "/campaigns", PAYLOAD, UUID.randomUUID().toString()).headers().firstValue("Location").orElseThrow(); }
    private static JdbcTemplate jdbc() { return first.getBean(JdbcTemplate.class); }
    private String base(ServletWebServerApplicationContext app) { return "http://127.0.0.1:" + app.getWebServer().getPort(); }
    private JsonNode json(HttpResponse<String> response) throws Exception { return JSON.readTree(response.body()); }
    private HttpResponse<String> call(ServletWebServerApplicationContext app, String method, String path, String body, String key) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base(app) + path)).timeout(Duration.ofSeconds(8));
        if (key != null) builder.header("Idempotency-Key", key);
        if (body != null) builder.header("Content-Type", "application/json");
        return http.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    private void assertNoDispatch() throws Exception {
        for (String table : List.of("notification", "delivery_task", "outbox_event", "delivery_attempt")) {
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM " + table, Integer.class)).isZero();
        }
        for (var app : List.of(first, second)) {
            assertThat(app.getBean(MeterRegistry.class).get("notification.rate.limit.decisions").tag("outcome", "allowed").counter().count()).isZero();
        }
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var routes = first.getBean(TrafficRouting.class);
            var names = List.of(routes.topic(), routes.retryTopic(), routes.deadLetterTopic());
            var ends = new java.util.HashMap<TopicPartition, OffsetSpec>();
            admin.describeTopics(names).allTopicNames().get(5, TimeUnit.SECONDS).forEach((name, description) ->
                    description.partitions().forEach(part -> ends.put(new TopicPartition(name, part.partition()), OffsetSpec.latest())));
            assertThat(admin.listOffsets(ends).all().get(5, TimeUnit.SECONDS).values()).allSatisfy(offset -> assertThat(offset.offset()).isZero());
        }
    }
}
