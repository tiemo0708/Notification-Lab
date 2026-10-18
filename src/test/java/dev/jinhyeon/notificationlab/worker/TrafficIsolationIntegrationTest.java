package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import com.sun.net.httpserver.HttpServer;
import dev.jinhyeon.notificationlab.config.TrafficRouting;
import dev.jinhyeon.notificationlab.outbox.PipelineMetrics;
import dev.jinhyeon.notificationlab.outbox.OutboxRelay;
import org.springframework.core.io.ClassPathResource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class TrafficIsolationIntegrationTest {
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
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4.11-alpine").withExposedPorts(6379)
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    private ServletWebServerApplicationContext realtime;
    private ServletWebServerApplicationContext campaign;
    private Admin kafka;
    private HttpServer server;
    private ExecutorService serverExecutor;
    private final AtomicInteger realtimeCalls = new AtomicInteger();
    private final AtomicInteger campaignCalls = new AtomicInteger();
    private final AtomicInteger campaignStatus = new AtomicInteger(202);
    private volatile CountDownLatch campaignGate = new CountDownLatch(0);
    private String url;
    private String topic;
    private String quotaKey;

    @BeforeEach
    void isolateDatabaseAndMessaging() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String database = "traffic_" + suffix;
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) { statement.execute("CREATE DATABASE " + database); }
        url = MYSQL.getJdbcUrl().replace("/notification_lab", "/" + database);
        topic = "traffic-" + suffix; quotaKey = "test:traffic:" + suffix;
        kafka = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 8);
        serverExecutor = Executors.newFixedThreadPool(3);
        server.setExecutor(serverExecutor);
        server.createContext("/pushes", exchange -> {
            try (exchange) {
                String title = new ObjectMapper().readTree(exchange.getRequestBody()).path("title").asText();
                boolean bulk = "CAMPAIGN".equals(title);
                if (bulk) campaignCalls.incrementAndGet(); else realtimeCalls.incrementAndGet();
                try {
                    if (bulk && !campaignGate.await(4, TimeUnit.SECONDS)) throw new java.io.IOException("Test gate timeout");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new java.io.IOException("Test interrupted");
                }
                byte[] response = ("{\"providerMessageId\":\"http-mock-" + UUID.randomUUID() + "\"}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(bulk ? campaignStatus.get() : 202, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        server.start();
        try (Jedis redis = redis()) {
            redis.eval(new ClassPathResource("redis/initialize-split-quota.lua").getContentAsString(StandardCharsets.UTF_8),
                    List.of(quotaKey), List.of("10", "10", "2", "2"));
        }
    }

    @Test
    void campaignOccupiesItsOnlyHttpAndWorkerSlotWhileRealtimeCompletes() throws Exception {
        startBoth();
        campaignGate = new CountDownLatch(1);
        var bulk = create(campaign, "CAMPAIGN", null);
        await().atMost(Duration.ofSeconds(10)).until(() -> campaignCalls.get() == 1);
        var next = create(campaign, "CAMPAIGN", null);
        var urgent = create(realtime, "REALTIME", null);
        try {
            await().atMost(Duration.ofSeconds(3)).until(() -> status(urgent).equals("PROVIDER_ACCEPTED") && offsets(realtime) == 1);
            assertThat(campaign.getBean(WorkerAdmission.class).active()).isEqualTo(1);
            assertThat(realtime.getBean(WorkerAdmission.class).active()).isZero();
            assertThat(status(bulk)).isEqualTo("PROCESSING");
            assertThat(attempts(next)).isZero();
            assertThat(offsets(campaign)).isZero();
            assertThat(realtimeCalls.get()).isEqualTo(1);
            assertThat(campaignCalls.get()).isEqualTo(1);
        } finally { campaignGate.countDown(); }
        await().atMost(Duration.ofSeconds(10)).until(() -> offsets(campaign) == 2);
        assertThat(status(bulk)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(status(next)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(attempts(bulk)).isEqualTo(1); assertThat(attempts(next)).isEqualTo(1);
        assertThat(allowed(campaign)).isEqualTo(2);
        assertThat(allowed(realtime)).isEqualTo(1);
        System.out.println("TRAFFIC_RESOURCES campaignActive=1 campaignOffsetsWhileBlocked=0 realtimeCompleted=1 finalCampaignOffsets=2");
    }

    @Test
    void campaignQuotaPauseAndContextRestartDoNotPauseRealtimeOrSpendItsReservation() throws Exception {
        startBoth();
        try (Jedis redis = redis()) {
            var t = redis.time(); long now = Long.parseLong(t.get(0)) * 1000 + Long.parseLong(t.get(1)) / 1000;
            redis.hset(quotaKey, Map.of("realtime_credit", "8000", "campaign_credit", "0", "last_ms", Long.toString(now + 15000)));
        }
        var bulk = create(campaign, "CAMPAIGN", null);
        await().atMost(Duration.ofSeconds(10)).until(() -> container(campaign).isContainerPaused());
        var urgent = create(realtime, "REALTIME", null);
        await().atMost(Duration.ofSeconds(5)).until(() -> offsets(realtime) == 1);
        assertThat(status(urgent)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(attempts(bulk)).isZero();
        assertThat(jdbc().queryForObject("SELECT worker_token FROM notification WHERE notification_id = ?", String.class, bulk.id())).isNull();
        assertThat(offsets(campaign)).isZero();
        assertThat(campaignCalls.get()).isZero();
        assertThat(campaign.getBean(WorkerAdmission.class).active()).isZero();
        assertThat(realtime.getBean(WorkerPauseErrorHandler.class).pendingCount()).isZero();
        campaign.getBean(OutboxRelay.class).stopClaiming();
        var unpublished = create(campaign, "CAMPAIGN", null);
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(jdbc().queryForObject("SELECT status FROM outbox_event WHERE notification_id = ?", String.class, unpublished.id()))
                        .isEqualTo("PENDING"));
        // Metrics are role-scoped even though both applications use the same database.
        realtime.getBean(PipelineMetrics.class).refresh(); campaign.getBean(PipelineMetrics.class).refresh();
        assertThat(realtime.getBean(MeterRegistry.class).get("outbox.unpublished").gauge().value()).isZero();
        assertThat(campaign.getBean(MeterRegistry.class).get("outbox.unpublished").gauge().value()).isEqualTo(1);
        assertThat(realtime.getBean(MeterRegistry.class).get("notification.consumer.lag").gauge().value()).isZero();
        assertThat(campaign.getBean(MeterRegistry.class).get("notification.consumer.lag").gauge().value()).isEqualTo(1);
        var old = campaign.getBean(WorkerPauseErrorHandler.class);
        campaign.close(); campaign = null;
        assertThat(old.pendingCount()).isZero();
        var another = create(realtime, "REALTIME", null);
        await().atMost(Duration.ofSeconds(5)).until(() -> offsets(realtime) == 2);
        assertThat(status(another)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(jdbc().queryForObject("SELECT status FROM outbox_event WHERE notification_id = ?", String.class, unpublished.id()))
                .isEqualTo("PENDING");
        try (Jedis redis = redis()) {
            assertThat(redis.hget(quotaKey, "realtime_credit")).isEqualTo("6000");
            assertThat(redis.hget(quotaKey, "campaign_credit")).isEqualTo("0");
            redis.hset(quotaKey, "last_ms", "0"); // Test-only release of frozen refill, never a production recovery procedure.
        }
        campaign = application("CAMPAIGN");
        await().atMost(Duration.ofSeconds(20)).until(() -> offsets(campaign) == 2);
        assertThat(status(bulk)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(attempts(bulk)).isEqualTo(1);
        assertThat(status(unpublished)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(attempts(unpublished)).isEqualTo(1);
        assertThat(campaignCalls.get()).isEqualTo(2);
        System.out.println("TRAFFIC_PAUSE campaignAttemptsBeforeRestart=0 realtimeCompleted=2 campaignRecovered=2 scopedUnpublished=1");
    }

    @Test
    void retriesDlqRedriveAndIdempotencyPreserveCampaignRole() throws Exception {
        startBoth(); campaignStatus.set(500);
        String key = "cross-role-" + UUID.randomUUID();
        var bulk = create(campaign, "CAMPAIGN", key);
        await().atMost(Duration.ofSeconds(15)).until(() -> jdbc().queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_DEAD_LETTER' AND status = 'PUBLISHED'",
                Integer.class, bulk.id()) == 1);
        assertThat(attempts(bulk)).isEqualTo(2);
        assertThat(offsets(campaign)).isEqualTo(2);
        assertThat(campaignCalls.get()).isEqualTo(2);
        assertThat(allowed(campaign)).isEqualTo(2);
        assertThat(allowed(realtime)).isZero();
        assertThat(jdbc().queryForList("SELECT DISTINCT traffic_class FROM outbox_event WHERE notification_id = ?", String.class, bulk.id()))
                .containsExactly("CAMPAIGN");
        assertThat(post(campaign, "/notifications", payload("CAMPAIGN"), key).statusCode()).isEqualTo(200);
        assertThat(post(realtime, "/notifications", payload("CAMPAIGN"), key).statusCode()).isEqualTo(409);
        String dlq = jdbc().queryForObject("SELECT event_id FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_DEAD_LETTER'", String.class, bulk.id());
        String redrive = "{\"reason\":\"PROVIDER_RECOVERED\",\"acknowledgePossibleDuplicate\":true}";
        assertThat(post(realtime, "/dead-letters/" + dlq + "/redrive", redrive, null).statusCode()).isEqualTo(409);
        campaignStatus.set(202);
        assertThat(post(campaign, "/dead-letters/" + dlq + "/redrive", redrive, null).statusCode()).isEqualTo(202);
        String target = jdbc().queryForObject("SELECT target_notification_id FROM dead_letter_redrive WHERE source_event_id = ?", String.class, dlq);
        await().atMost(Duration.ofSeconds(10)).until(() -> offsets(campaign) == 3);
        assertThat(jdbc().queryForObject("SELECT traffic_class FROM notification WHERE notification_id = ?", String.class, target)).isEqualTo("CAMPAIGN");
        assertThat(jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, target)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(campaignCalls.get()).isEqualTo(3);
        assertThat(realtimeCalls.get()).isZero();
        assertThat(allowed(campaign)).isEqualTo(3);
        System.out.println("TRAFFIC_RETRY campaignAttempts=2 redrive=1 realtimeQuota=0 crossRoleIdempotency=409 crossRoleRedrive=409");
    }

    @Test
    void wrongTopicCannotPromoteCampaignWorkOrAdvanceRealtimeOffset() throws Exception {
        startBoth(); container(campaign).stop();
        var bulk = create(campaign, "CAMPAIGN", null);
        @SuppressWarnings("unchecked") var template = (KafkaTemplate<String, String>) realtime.getBean(KafkaTemplate.class);
        template.send(topic, 0, bulk.id(), bulk.payload()).get(5, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(15)).until(() -> !container(realtime).isRunning());
        assertThat(attempts(bulk)).isZero();
        assertThat(status(bulk)).isEqualTo("QUEUED");
        assertThat(realtimeCalls.get() + campaignCalls.get()).isZero();
        assertThat(allowed(realtime)).isZero();
        assertThat(offsets(realtime)).isZero();
        assertThatThrownBy(() -> jdbc().update("UPDATE outbox_event SET traffic_class = 'REALTIME' WHERE notification_id = ?", bulk.id()))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }

    @Test
    void migrationKeepsExistingVersionSixRowsInRealtime() {
        var flyway = Flyway.configure().dataSource(url, "root", MYSQL.getPassword()).target("6").load();
        flyway.migrate();
        var db = new JdbcTemplate(new DriverManagerDataSource(url, "root", MYSQL.getPassword()));
        String id = UUID.randomUUID().toString();
        db.update("INSERT INTO notification(notification_id,recipient_id,title,body,status,created_at) VALUES(?,'synthetic','legacy','test','QUEUED',UTC_TIMESTAMP(6))", id);
        db.update("INSERT INTO outbox_event(event_id,notification_id,event_type,payload,created_at,available_at) VALUES(?,?,'NOTIFICATION_REQUESTED','{}',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))", UUID.randomUUID().toString(), id);
        Flyway.configure().dataSource(url, "root", MYSQL.getPassword()).load().migrate();
        assertThat(db.queryForObject("SELECT traffic_class FROM notification WHERE notification_id = ?", String.class, id)).isEqualTo("REALTIME");
        assertThat(db.queryForObject("SELECT traffic_class FROM outbox_event WHERE notification_id = ?", String.class, id)).isEqualTo("REALTIME");
    }

    private void startBoth() {
        realtime = application("REALTIME"); campaign = application("CAMPAIGN");
        await().atMost(Duration.ofSeconds(20)).until(() -> assigned(realtime) == 6 && assigned(campaign) == 6);
    }

    private ServletWebServerApplicationContext application(String lane) {
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class).run(
                "--server.port=0", "--app.provider.mode=HTTP", "--app.traffic.split-enabled=true", "--app.traffic.lane=" + lane,
                "--app.provider.http.base-url=http://127.0.0.1:" + server.getAddress().getPort(),
                "--app.provider.http.max-connections=1", "--app.provider.http.response-timeout-ms=5000", "--app.provider.http.deadline-ms=10000",
                "--spring.datasource.url=" + url, "--spring.datasource.username=root", "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(), "--app.async.topic=" + topic, "--app.async.group=" + topic,
                "--app.rate-limit.client-enabled=true", "--app.rate-limit.pause-enabled=true",
                "--app.rate-limit.port=" + REDIS.getMappedPort(6379), "--app.rate-limit.key=" + quotaKey,
                "--app.rate-limit.rate-per-second=10", "--app.rate-limit.burst=10", "--app.rate-limit.timeout-ms=250",
                "--app.delivery.max-attempts=2", "--app.delivery.retry-base-delay-ms=100", "--app.delivery.retry-max-delay-ms=100",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
    }

    @AfterEach
    void close() {
        campaignGate.countDown();
        if (campaign != null) campaign.close();
        if (realtime != null) realtime.close();
        if (server != null) server.stop(0);
        if (serverExecutor != null) serverExecutor.shutdownNow();
        if (kafka != null) kafka.close(Duration.ofSeconds(2));
    }

    private record Event(String id, String payload) { }
    private Event create(ServletWebServerApplicationContext app, String lane, String key) throws Exception {
        var response = post(app, "/notifications", payload(lane), key);
        assertThat(response.statusCode()).isEqualTo(202);
        String id = new ObjectMapper().readTree(response.body()).path("notificationId").asText();
        return new Event(id, jdbc().queryForObject("SELECT payload FROM outbox_event WHERE notification_id = ? AND event_type = 'NOTIFICATION_REQUESTED'", String.class, id));
    }
    private String payload(String lane) { return "{\"recipientId\":\"synthetic\",\"title\":\"" + lane + "\",\"body\":\"test\"}"; }
    private HttpResponse<String> post(ServletWebServerApplicationContext app, String path, String body, String key) throws Exception {
        try (var http = HttpClient.newHttpClient()) {
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.getWebServer().getPort() + path))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json");
            if (key != null) request.header("Idempotency-Key", key);
            return http.send(request.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        }
    }
    private Jedis redis() { return new Jedis("127.0.0.1", REDIS.getMappedPort(6379), 3000); }
    private long offsets(ServletWebServerApplicationContext app) throws Exception {
        var routes = app.getBean(TrafficRouting.class);
        return kafka.listConsumerGroupOffsets(routes.group()).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .entrySet().stream().filter(e -> e.getKey().topic().equals(routes.topic()) || e.getKey().topic().equals(routes.retryTopic()))
                .mapToLong(e -> e.getValue().offset()).sum();
    }
    private int assigned(ServletWebServerApplicationContext app) {
        var parts = container(app).getAssignedPartitions(); return parts == null ? 0 : parts.size();
    }
    private ConcurrentMessageListenerContainer<?, ?> container(ServletWebServerApplicationContext app) {
        return (ConcurrentMessageListenerContainer<?, ?>) app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
    }
    private double allowed(ServletWebServerApplicationContext app) { return app.getBean(MeterRegistry.class).get("notification.rate.limit.decisions").tag("outcome", "allowed").counter().count(); }
    private JdbcTemplate jdbc() { return realtime.getBean(JdbcTemplate.class); }
    private String status(Event e) { return jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, e.id()); }
    private int attempts(Event e) { return jdbc().queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?", Integer.class, e.id()); }
}
