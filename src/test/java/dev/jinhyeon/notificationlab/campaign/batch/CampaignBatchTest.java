package dev.jinhyeon.notificationlab.campaign.batch;

import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.campaign.Campaign;
import dev.jinhyeon.notificationlab.campaign.CampaignChunk;
import dev.jinhyeon.notificationlab.campaign.CampaignRequest;
import dev.jinhyeon.notificationlab.campaign.CampaignService;
import dev.jinhyeon.notificationlab.campaign.SyntheticCampaignAudience;
import dev.jinhyeon.notificationlab.config.AsyncProperties;
import dev.jinhyeon.notificationlab.config.TrafficRouting;
import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.outbox.OutboxRelay;
import dev.jinhyeon.notificationlab.outbox.OutboxStore;
import dev.jinhyeon.notificationlab.worker.CampaignCompletionBatch;
import dev.jinhyeon.notificationlab.worker.WorkerStore;
import dev.jinhyeon.notificationmock.HttpMockProperties.Mode;
import dev.jinhyeon.notificationmock.MockPushEndpoint;
import dev.jinhyeon.notificationmock.MockServerApplication;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.item.Chunk;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
class CampaignBatchTest {
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
    private static TimeZone originalZone;

    @BeforeAll
    static void useUtcMetadata() {
        originalZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @AfterAll
    static void restoreZone() { TimeZone.setDefault(originalZone); }

    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();
    private ConfigurableApplicationContext app;
    private String url;
    private String topic;
    private String quotaKey;

    @BeforeEach
    void isolate() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String database = "batch_" + suffix;
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
            statement.execute("GRANT ALL ON " + database + ".* TO 'notification_test'@'%'");
        }
        url = MYSQL.getJdbcUrl().replace("/notification_lab", "/" + database);
        topic = "batch-" + suffix; quotaKey = "test:batch:" + suffix;
        try (Jedis redis = new Jedis("127.0.0.1", REDIS.getMappedPort(6379), 3000)) {
            redis.eval(new ClassPathResource("redis/initialize-split-quota.lua").getContentAsString(StandardCharsets.UTF_8),
                    List.of(quotaKey), List.of("100", "100", "50", "50"));
        }
        app = application(false);
    }

    private ArrayList<String> arguments(boolean worker) {
        return new ArrayList<>(List.of(
                "--app.traffic.lane=CAMPAIGN", "--app.traffic.split-enabled=true",
                "--app.rate-limit.client-enabled=true", "--app.async.worker-enabled=" + worker,
                "--app.async.relay-enabled=false", "--app.mock.delay-ms=0",
                "--spring.datasource.url=" + url, "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + topic,
                "--app.rate-limit.port=" + REDIS.getMappedPort(6379), "--app.rate-limit.key=" + quotaKey,
                "--app.rate-limit.rate-per-second=100", "--app.rate-limit.burst=100",
                "--app.traffic.campaign-rate=50", "--app.traffic.campaign-burst=50",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off"));
    }

    private ConfigurableApplicationContext application(boolean worker, String... options) {
        var arguments = arguments(worker);
        arguments.addAll(List.of("--app.campaign.batch.enabled=true", "--app.campaign.batch.chunk-size=3",
                "--spring.datasource.password=" + MYSQL.getPassword()));
        arguments.addAll(List.of(options));
        var builder = new SpringApplicationBuilder(NotificationLabApplication.class).web(WebApplicationType.NONE);
        var context = builder.run(arguments.toArray(String[]::new));
        contexts.add(context);
        return context;
    }

    @Test
    void groupedOutboxRollsBackPartialTailAndResumesInTheRecordedFormat() throws Exception {
        app.close();
        app = application(false, "--app.campaign.batch.grouped-outbox=true");
        var campaign = requested(101);
        root("CREATE TRIGGER fail_target BEFORE INSERT ON campaign_target FOR EACH ROW BEGIN IF NEW.target_ordinal = 101 THEN SIGNAL SQLSTATE '45000'; END IF; END");
        JobExecution failed;
        try { failed = run(campaign); } finally { root("DROP TRIGGER fail_target"); }
        assertThat(failed.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(checkpoint(failed)).isEqualTo(100);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM campaign_target", Integer.class)).isEqualTo(100);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM outbox_event", Integer.class)).isEqualTo(1);

        app.close();
        app = application(false);
        var resumed = run(campaign);
        assertThat(resumed.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(resumed.getJobInstance().getInstanceId()).isEqualTo(failed.getJobInstance().getInstanceId());
        assertThat(checkpoint(resumed)).isEqualTo(101);
        assertThat(db().queryForList("""
                SELECT COUNT(*) FROM campaign_target GROUP BY event_id ORDER BY COUNT(*) DESC
                """, Integer.class)).containsExactly(100, 1);
        assertThat(db().queryForList("""
                SELECT event_id FROM outbox_event
                WHERE notification_id IS NULL AND event_type=? ORDER BY CAST(payload->>'$.firstOrdinal' AS UNSIGNED)
                """, String.class, CampaignChunk.EVENT_TYPE)).containsExactly(
                new CampaignChunk(campaign.campaignId(), 1, 100).eventId().toString(),
                new CampaignChunk(campaign.campaignId(), 101, 1).eventId().toString());
        assertThat(run(campaign).getId()).isEqualTo(resumed.getId());
        assertThat(db().queryForObject("SELECT COUNT(*) FROM delivery_task", Integer.class)).isEqualTo(101);
        assertThat(endOffset()).isZero();
    }

    @Test
    void groupedRecordStaysUncommittedAfterPartialFailureAndResumesPerTarget() throws Exception {
        app.close();
        app = application(false, "--app.campaign.batch.grouped-outbox=true");
        var campaign = requested(3);
        assertThat(run(campaign).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        relay(template()).relay();
        assertThat(endOffset()).isEqualTo(1);
        root("""
                CREATE TRIGGER fail_claim BEFORE UPDATE ON notification FOR EACH ROW
                BEGIN
                    IF NEW.status='PROCESSING' AND NEW.recipient_id='synthetic-v1-2' THEN
                        SIGNAL SQLSTATE '45000';
                    END IF;
                END
                """);
        try (var worker = application(true)) {
            var listener = worker.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
            await().atMost(Duration.ofSeconds(20)).until(() -> !listener.isRunning());
            assertThat(committedOffset(worker)).isZero();
            assertThat(db().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(1);
            assertThat(db().queryForList("SELECT status FROM notification ORDER BY recipient_id", String.class))
                    .containsExactly("PROVIDER_ACCEPTED", "QUEUED", "QUEUED");
        } finally { root("DROP TRIGGER fail_claim"); }

        try (var worker = application(true, "--app.mock.outcome=PERMANENT_FAILURE")) {
            await().atMost(Duration.ofSeconds(20)).until(() -> committedOffset(worker) == 1);
            assertThat(db().queryForList("SELECT status FROM notification ORDER BY recipient_id", String.class))
                    .containsExactly("PROVIDER_ACCEPTED", "PERMANENT_FAILURE", "PERMANENT_FAILURE");
            assertThat(db().queryForList("SELECT attempt_count FROM delivery_task", Integer.class))
                    .containsExactly(1, 1, 1);
            assertThat(db().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(3);
            assertThat(db().queryForObject("""
                    SELECT COUNT(*) FROM outbox_event
                    WHERE event_type='NOTIFICATION_DEAD_LETTER' AND payload->>'$.sourceEventId'=?
                    """, Integer.class, new CampaignChunk(campaign.campaignId(), 1, 3).eventId().toString())).isEqualTo(2);
        }
    }

    @Test
    @Timeout(160)
    void killingWorkerBeforeFiveSuccessesCommitRecoversEveryOriginalMockReceipt(@TempDir Path directory) throws Exception {
        app.close();
        app = application(false, "--app.campaign.batch.grouped-outbox=true");
        assertThat(run(requested(5)).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        relay(template()).relay();
        assertThat(endOffset()).isOne();
        var ledger = directory.resolve("mock-acceptances");
        var marker = directory.resolve("five-before-commit");
        try (var mock = startHttpMock(ledger.toString())) {
            String endpoint = "http://127.0.0.1:" + mock.getWebServer().getPort();
            var provider = mock.getBean(MockPushEndpoint.class);
            Path originalLog = directory.resolve("original.log");
            Process first = startCompletionWorker(endpoint, originalLog, marker);
            try {
                await().atMost(Duration.ofSeconds(45)).until(() -> Files.exists(marker) || !first.isAlive());
                assertThat(first.isAlive()).as("original worker: %s", workerExitDetails(originalLog)).isTrue();
                assertThat(marker).hasContent("5");
                assertThat(provider.stats().requests()).isEqualTo(5);
                assertThat(provider.stats().accepted()).isEqualTo(5);
                assertThat(count("SELECT COUNT(*) FROM notification WHERE status='PROCESSING'")).isEqualTo(5);
                assertThat(count("SELECT COUNT(*) FROM delivery_attempt WHERE outcome='STARTED'")).isEqualTo(5);
                assertThat(committedOffset(app)).isZero();
            } finally {
                stopCompletionWorker(first);
            }
            assertThat(first.exitValue()).isNotZero();
            assertThat(count("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'")).isZero();
            assertThat(committedOffset(app)).isZero();
            await().atMost(Duration.ofSeconds(35)).until(() ->
                    count("SELECT COUNT(*) FROM notification WHERE worker_lease_until <= UTC_TIMESTAMP(6)") == 5);
            Path replacementLog = directory.resolve("replacement.log");
            Process replacement = startCompletionWorker(endpoint, replacementLog, null);
            try {
                await().atMost(Duration.ofSeconds(50)).until(() -> !replacement.isAlive() || committedOffset(app) == 1);
                assertThat(replacement.isAlive()).as("replacement worker: %s", workerExitDetails(replacementLog)).isTrue();
                assertThat(committedOffset(app)).isOne();
                assertThat(count("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'")).isEqualTo(5);
                assertThat(count("SELECT COUNT(*) FROM delivery_attempt WHERE attempt_no=1 AND outcome='UNKNOWN'")).isEqualTo(5);
                assertThat(count("SELECT COUNT(*) FROM delivery_attempt WHERE attempt_no=2 AND outcome='PROVIDER_ACCEPTED'")).isEqualTo(5);
                assertThat(count("SELECT COUNT(*) FROM delivery_attempt")).isEqualTo(10);
                assertThat(count("SELECT COUNT(*) FROM outbox_event")).isOne();
                assertThat(endOffset()).isOne();
                assertThat(provider.stats().requests()).isEqualTo(10);
                assertThat(provider.stats().accepted()).isEqualTo(5);
                assertThat(provider.stats().idempotencyReplays()).isEqualTo(5);
                try (var connection = DriverManager.getConnection("jdbc:h2:file:" + ledger + ";WRITE_DELAY=0", "sa", "");
                     var query = connection.createStatement();
                     var rows = query.executeQuery("SELECT notification_id, provider_id FROM mock_acceptance")) {
                    int matched = 0;
                    while (rows.next()) {
                        assertThat(db().queryForObject("SELECT provider_message_id FROM notification WHERE notification_id=?",
                                String.class, rows.getString(1))).isEqualTo("http-mock-" + rows.getString(2));
                        matched++;
                    }
                    assertThat(matched).isEqualTo(5);
                }
                System.out.println("CAMPAIGN_COMPLETION_CRASH targets=5 beforeCommitVisible=0 finalDb=5 httpRequests=10 "
                        + "firstAcceptances=5 replayedReceipts=5 matchedReceipts=5 offsetBefore=0 offsetAfter=1");
            } finally {
                stopCompletionWorker(replacement);
            }
        }
    }

    private ServletWebServerApplicationContext startHttpMock(String ledger) {
        return startHttpMock(ledger, Mode.SUCCESS);
    }

    private ServletWebServerApplicationContext startHttpMock(String ledger, Mode mode) {
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(MockServerApplication.class)
                .run("--spring.config.name=mock-server", "--server.port=0", "--mock.http.response-delay-ms=1",
                        "--mock.http.idempotency-file=" + ledger, "--mock.http.mode=" + mode, "--logging.level.root=WARN");
    }

    @Test
    void staleSecondOwnerRollsBackBothResultsAndTheirCompletionCounts() throws Exception {
        app.close();
        try (var mock = startHttpMock(":memory:")) {
            app = application(false, "--spring.profiles.active=campaign-flexible", "--app.provider.mode=HTTP",
                    "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort());
            var campaign = requested(2);
            assertThat(run(campaign).getStatus()).isEqualTo(BatchStatus.COMPLETED);
            var eventId = new CampaignChunk(campaign.campaignId(), 1, 2).eventId();
            var store = app.getBean(WorkerStore.class);
            var service = app.getBean(NotificationService.class);
            var meters = app.getBean(MeterRegistry.class);
            var factory = app.getBean(CampaignCompletionBatch.Factory.class);
            var tokens = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            var successes = new ArrayList<Notification>();
            for (int index = 0; index < tokens.size(); index++) {
                var ids = CampaignTargetIds.of(campaign.campaignId(), index + 1);
                var claim = store.claim(ids.notificationId(), eventId, tokens.get(index), ids.taskId(), true, true).orElseThrow();
                successes.add(service.attempt(claim.notification()));
            }

            var completed = meters.counter("notification.completed", "status", "PROVIDER_ACCEPTED");
            var batch = factory.open();
            batch.accepted(successes.getFirst(), tokens.getFirst(), Timer.start(meters));
            batch.accepted(successes.getLast(), UUID.randomUUID().toString(), Timer.start(meters));
            assertThatThrownBy(batch::close).isInstanceOf(IllegalStateException.class)
                    .hasStackTraceContaining("Worker ownership was lost");
            assertThat(count("SELECT COUNT(*) FROM notification WHERE status='PROCESSING' AND provider_message_id IS NULL")).isEqualTo(2);
            assertThat(count("SELECT COUNT(*) FROM delivery_attempt WHERE outcome='STARTED'")).isEqualTo(2);
            assertThat(completed.count()).isZero();
            assertThat(CampaignCompletionExperiment.snapshot(meters, 5)).containsEntry("commits", 0L);

            try (var retry = factory.open()) {
                for (int index = 0; index < tokens.size(); index++) {
                    retry.accepted(successes.get(index), tokens.get(index), Timer.start(meters));
                }
            }
            assertThat(count("SELECT COUNT(*) FROM delivery_attempt WHERE outcome='PROVIDER_ACCEPTED'")).isEqualTo(2);
            for (var success : successes) {
                assertThat(service.get(success.notificationId()).providerMessageId()).isEqualTo(success.providerMessageId());
            }
            assertThat(completed.count()).isEqualTo(2);
            assertThat(CampaignCompletionExperiment.snapshot(meters, 5)).containsEntry("rows", 2L);
            assertThat(mock.getBean(MockPushEndpoint.class).stats().requests()).isEqualTo(2);
        }
    }

    @Test
    void failedGroupedTargetsHandOffToIndividualRetriesThatCommitOneAtATime() throws Exception {
        app.close();
        app = application(false, "--app.campaign.batch.grouped-outbox=true");
        assertThat(run(requested(2)).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        relay(template()).relay();
        try (var mock = startHttpMock(":memory:", Mode.SERVER_ERROR);
             var worker = application(true, "--spring.profiles.active=campaign-flexible", "--app.provider.mode=HTTP",
                     "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort())) {
            await().atMost(Duration.ofSeconds(20)).until(() -> committedOffset(worker) == 1);
            assertThat(count("SELECT COUNT(*) FROM notification WHERE status='WAITING_RETRY'")).isEqualTo(2);
            assertThat(count("SELECT COUNT(*) FROM outbox_event WHERE event_type='NOTIFICATION_RETRY'")).isEqualTo(2);
            var meters = worker.getBean(MeterRegistry.class);
            assertThat(meters.get("notification.pipeline.stage").tag("stage", "worker.complete").timer().count()).isEqualTo(2);
            assertThat(CampaignCompletionExperiment.snapshot(meters, 5)).containsEntry("commits", 0L);
        }
        try (var mock = startHttpMock(":memory:");
             var worker = application(true, "--spring.profiles.active=campaign-flexible", "--app.provider.mode=HTTP",
                     "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort())) {
            var relay = relay(template());
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                relay.relay();
                return committedOffset(worker) == 3;
            });
            assertThat(count("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'")).isEqualTo(2);
            assertThat(count("SELECT COUNT(*) FROM delivery_attempt WHERE attempt_no=2 AND outcome='PROVIDER_ACCEPTED'")).isEqualTo(2);
            var meters = worker.getBean(MeterRegistry.class);
            assertThat(meters.get("notification.pipeline.stage").tag("stage", "worker.complete").timer().count()).isEqualTo(2);
            assertThat(CampaignCompletionExperiment.snapshot(meters, 5)).containsEntry("commits", 0L);
            assertThat(mock.getBean(MockPushEndpoint.class).stats().accepted()).isEqualTo(2);
        }
    }

    private Process startCompletionWorker(String endpoint, Path log, Path marker) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", System.getProperty("notificationLab.testClasspath"),
                (marker == null ? NotificationLabApplication.class : CampaignCompletionExperiment.class).getName()));
        command.addAll(arguments(true));
        command.addAll(List.of("--spring.profiles.active=campaign-flexible", "--spring.main.web-application-type=none",
                "--app.campaign.batch.enabled=false", "--app.provider.mode=HTTP", "--app.provider.http.base-url=" + endpoint));
        if (marker != null) command.add("--experiment.hold-five-before-commit=" + marker);
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword());
        return builder.start();
    }

    private static void stopCompletionWorker(Process worker) throws InterruptedException {
        worker.destroyForcibly();
        assertThat(worker.waitFor(10, TimeUnit.SECONDS)).isTrue();
    }

    private static String workerExitDetails(Path log) throws java.io.IOException {
        return Files.readAllLines(log).stream().filter(line -> line.contains("Exception") || line.contains("Error:")
                || line.contains("Could not") || line.contains("Parameter ") || line.contains("Reason:"))
                .limit(8).collect(java.util.stream.Collectors.joining("\n"));
    }

    private int count(String sql) { return db().queryForObject(sql, Integer.class); }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deadlineFlushWhileNextClaimIsBlockedPreservesTheCommitBoundary(boolean failWrite) throws Exception {
        app.close();
        app = application(false, "--app.campaign.batch.grouped-outbox=true");
        assertThat(run(requested(2)).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        relay(template()).relay();
        root("""
                CREATE TRIGGER slow_second_claim BEFORE UPDATE ON notification FOR EACH ROW
                BEGIN
                    IF NEW.status='PROCESSING' AND NEW.recipient_id='synthetic-v1-2' THEN DO SLEEP(3); END IF;
                END
                """);
        if (failWrite) root("""
                CREATE TRIGGER fail_result BEFORE UPDATE ON delivery_attempt FOR EACH ROW
                BEGIN
                    IF NEW.outcome='PROVIDER_ACCEPTED' THEN SIGNAL SQLSTATE '45000'; END IF;
                END
                """);
        try (var mock = startHttpMock(":memory:");
             var worker = application(true, "--spring.profiles.active=campaign-flexible", "--app.provider.mode=HTTP",
                     "--app.provider.http.base-url=http://127.0.0.1:" + mock.getWebServer().getPort())) {
            var meters = worker.getBean(MeterRegistry.class);
            if (failWrite) {
                var listener = worker.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
                await().atMost(Duration.ofSeconds(20)).until(() -> !listener.isRunning());
                assertThat(CampaignCompletionExperiment.snapshot(meters, 5)).containsEntry("rows", 0L).containsEntry("deadlineTriggers", 1L);
                assertThat(count("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'")).isZero();
                assertThat(committedOffset(worker)).isZero();
                assertThat(mock.getBean(MockPushEndpoint.class).stats().requests()).isOne();
                return;
            }
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(count("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'")).isOne();
                assertThat(CampaignCompletionExperiment.snapshot(meters, 5)).containsEntry("rows", 1L).containsEntry("deadlineTriggers", 1L);
                assertThat(committedOffset(worker)).isZero();
            });
            await().atMost(Duration.ofSeconds(12)).until(() -> committedOffset(worker) == 1);
            assertThat(CampaignCompletionExperiment.snapshot(meters, 5)).containsEntry("rows", 2L).containsEntry("commits", 2L);
        } finally {
            root("DROP TRIGGER slow_second_claim");
            if (failWrite) root("DROP TRIGGER fail_result");
        }
    }

    @Test
    void slowChunkYieldsBetweenTargetsAndFinishesWithoutRepeatingWork() throws Exception {
        app.close();
        app = application(false, "--app.campaign.batch.grouped-outbox=true");
        var campaign = requested(2);
        assertThat(run(campaign).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        relay(template()).relay();
        root("""
                CREATE TRIGGER slow_completion BEFORE UPDATE ON notification FOR EACH ROW
                BEGIN
                    IF NEW.status='PROVIDER_ACCEPTED' AND NEW.recipient_id='synthetic-v1-1' THEN
                        DO SLEEP(4.2);
                    END IF;
                END
                """);
        try (var worker = application(true, "--spring.kafka.consumer.properties.max.poll.interval.ms=8000")) {
            await().atMost(Duration.ofSeconds(22)).until(() -> committedOffset(worker) == 1);
            assertThat(db().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(2);
            assertThat(db().queryForList("SELECT status FROM notification", String.class))
                    .containsExactly("PROVIDER_ACCEPTED", "PROVIDER_ACCEPTED");
            assertThat(worker.getBean(MeterRegistry.class).counter("notification.worker.campaign.chunk.pauses").count())
                    .isEqualTo(1);
        } finally { root("DROP TRIGGER slow_completion"); }
    }

    @Test
    void failedSecondChunkRollsBackAndNewContextRestartsSameInstanceAtCommittedKey() throws Exception {
        Campaign campaign = requested(7);
        root("CREATE TRIGGER fail_target BEFORE INSERT ON campaign_target FOR EACH ROW BEGIN IF NEW.target_ordinal = 5 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'secret-campaign-payload'; END IF; END");
        JobExecution failed;
        try { failed = run(campaign); } finally { root("DROP TRIGGER fail_target"); }
        assertThat(failed.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertRows(3);
        assertThat(checkpoint(failed)).isEqualTo(3);
        assertThat(failed.getExitStatus().getExitDescription()).doesNotContain("secret-campaign-payload");
        assertThat(failed.getStepExecutions()).allSatisfy(step ->
                assertThat(step.getExitStatus().getExitDescription()).doesNotContain("secret-campaign-payload"));
        app.close(); app = application(false);
        var resumed = run(campaign);
        assertThat(resumed.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(resumed.getJobInstance().getInstanceId()).isEqualTo(failed.getJobInstance().getInstanceId());
        assertThat(resumed.getId()).isNotEqualTo(failed.getId());
        assertThat(resumed.getStepExecutions()).allSatisfy(step -> assertThat(step.getReadCount()).isEqualTo(4));
        assertThat(checkpoint(resumed)).isEqualTo(7);
        assertRows(7);
        assertThat(run(campaign).getId()).isEqualTo(resumed.getId());
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE", Integer.class)).isEqualTo(1);
        assertThat(endOffset()).isZero();
        assertThat(db().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PENDING'", Integer.class)).isEqualTo(7);
        assertIdentities(campaign);
        System.out.println("CAMPAIGN_BATCH_RESTART checkpoint=3 resumedRead=4 finalTargets=7 instances=1 executions=2 kafkaRecords=0");
    }

    @Test
    void checkpointStorageFailureRollsBackBusinessRowsWithTheChunk() throws Exception {
        Campaign campaign = requested(7);
        root("CREATE TRIGGER fail_checkpoint BEFORE UPDATE ON BATCH_STEP_EXECUTION_CONTEXT FOR EACH ROW BEGIN IF EXISTS(SELECT 1 FROM campaign_target WHERE target_ordinal = 4) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'checkpoint failure'; END IF; END");
        JobExecution failed;
        try { failed = run(campaign); } finally { root("DROP TRIGGER fail_checkpoint"); }
        assertThat(failed.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertRows(3);
        assertThat(checkpoint(failed)).isEqualTo(3);
        assertThat(run(campaign).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertRows(7);
    }

    @Test
    void repositoryRejectsSecondApplicationWhileSameInstanceIsRunning() throws Exception {
        Campaign campaign = requested(6);
        var second = application(false);
        root("CREATE TRIGGER slow_target BEFORE INSERT ON campaign_target FOR EACH ROW DO SLEEP(0.2)");
        try (var executor = Executors.newSingleThreadExecutor()) {
            var firstRun = executor.submit(() -> run(campaign));
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION WHERE STATUS='STARTED'", Integer.class) == 1);
            assertThatThrownBy(() -> second.getBean(CampaignBatchService.class).execute(campaign.campaignId()))
                    .isInstanceOf(JobExecutionAlreadyRunningException.class);
            assertThat(firstRun.get(10, TimeUnit.SECONDS).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        } finally { root("DROP TRIGGER slow_target"); }
        assertRows(6);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class)).isEqualTo(1);
    }

    @Test
    void ackUncertaintyDuplicatesKafkaRecordButNotDurableWorkOrProviderAttempt() throws Exception {
        Campaign campaign = requested(2);
        var execution = run(campaign);
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(checkpoint(execution)).isEqualTo(2);
        assertRows(2);
        var real = template();
        var uncertainAck = new CompletableFuture<SendResult<String, String>>();
        var brokerAck = new CompletableFuture<SendResult<String, String>>();
        var gated = new KafkaTemplate<String, String>(real.getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String name, String key, String value) {
                real.send(name, key, value).whenComplete((result, error) -> {
                    if (error != null) brokerAck.completeExceptionally(error); else brokerAck.complete(result);
                });
                return uncertainAck;
            }
        };
        try (var executor = Executors.newSingleThreadExecutor()) {
            var relay = executor.submit(() -> relay(gated).relay());
            brokerAck.get(5, TimeUnit.SECONDS);
            assertThat(endOffset()).isEqualTo(1);
            assertThat(db().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED'", Integer.class)).isZero();
            assertThat(db().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PROCESSING'", Integer.class)).isEqualTo(1);
            uncertainAck.completeExceptionally(new java.util.concurrent.TimeoutException("test ACK uncertainty"));
            relay.get(5, TimeUnit.SECONDS);
        } finally { uncertainAck.completeExceptionally(new IllegalStateException("test cleanup")); }
        db().update("UPDATE outbox_event SET available_at=UTC_TIMESTAMP(6)");
        relay(real).relay();
        assertThat(endOffset()).isEqualTo(3);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED'", Integer.class)).isEqualTo(2);
        var worker = application(true);
        await().atMost(Duration.ofSeconds(20)).until(() ->
                db().queryForObject("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'", Integer.class) == 2
                        && committedOffset(worker) == 3);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(2);
        var tx = new TransactionTemplate(app.getBean(PlatformTransactionManager.class));
        tx.executeWithoutResult(status -> new CampaignTargetWriter(campaign, db(), 3)
                .write(new Chunk<>(SyntheticCampaignAudience.read(campaign, 0, 2).items())));
        assertRows(2);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(2);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED'", Integer.class)).isEqualTo(2);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM delivery_attempt", Integer.class)).isEqualTo(2);
        assertThat(endOffset()).isEqualTo(3);
        System.out.println("CAMPAIGN_BATCH_ACK targets=2 kafkaRecords=3 published=2 providerAttempts=2 replayReset=0");
    }

    @Test
    void explicitCommandRunsInSeparateJvmAndCompletedReplayDoesNotExpandAgain() throws Exception {
        Campaign campaign = requested(4);
        assertThat(command(campaign.campaignId())).isZero();
        var created = db().queryForObject("SELECT CREATE_TIME FROM BATCH_JOB_EXECUTION", java.time.LocalDateTime.class);
        assertThat(Duration.between(created.toInstant(java.time.ZoneOffset.UTC), java.time.Instant.now()).abs())
                .isLessThan(Duration.ofMinutes(1));
        assertRows(4);
        assertThat(command(campaign.campaignId())).isZero();
        assertRows(4);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class)).isEqualTo(1);
        assertThat(endOffset()).isZero();
    }

    @ParameterizedTest
    @CsvSource({"APP_CAMPAIGN_BATCH_GROUPED_OUTBOX,true", "SPRING_PROFILES_ACTIVE,campaign-flexible"})
    void groupedOutboxAppliesOnlyToTheSelectedCampaignCommand(String option, String value) throws Exception {
        Campaign grouped = requested(101);
        Campaign individual = requested(4);

        assertThat(command(grouped.campaignId(), 0, Map.of(option, value))).isZero();
        assertThat(db().queryForObject("SELECT COUNT(*) FROM campaign_target WHERE campaign_id=?", Integer.class,
                individual.campaignId().toString())).isZero();
        assertThat(command(individual.campaignId())).isZero();

        String referencesPerEvent = "SELECT COUNT(*) FROM campaign_target WHERE campaign_id=? GROUP BY event_id ORDER BY COUNT(*) DESC";
        assertThat(db().queryForList(referencesPerEvent, Integer.class, grouped.campaignId().toString())).containsExactly(100, 1);
        assertThat(db().queryForList(referencesPerEvent, Integer.class, individual.campaignId().toString())).containsExactly(1, 1, 1, 1);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE event_type=?", Integer.class,
                CampaignChunk.EVENT_TYPE)).isEqualTo(2);
        assertThat(endOffset()).isZero();
    }

    @Test
    void draftAndUnknownCampaignsCannotCreateBatchExecutions() throws Exception {
        Campaign draft = app.getBean(CampaignService.class).create(new CampaignRequest("draft", "title", "body", 2), UUID.randomUUID().toString()).campaign();
        assertThatThrownBy(() -> run(draft)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> app.getBean(CampaignBatchService.class).execute(UUID.randomUUID()))
                .isInstanceOf(dev.jinhyeon.notificationlab.campaign.CampaignNotFoundException.class);
        assertThat(command(draft.campaignId(), 1)).isEqualTo(1);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE", Integer.class)).isZero();
        assertRows(0);
    }

    @Test
    void killedJvmRecoversOnlyCommittedChunkInSameJobInstance() throws Exception {
        Campaign campaign = requested(7);
        root("CREATE TRIGGER slow_chunk BEFORE INSERT ON campaign_target FOR EACH ROW BEGIN IF NEW.target_ordinal = 4 THEN DO SLEEP(8); END IF; END");
        Path output = Files.createTempFile("campaign-kill-", ".log");
        Process process = startCommand(campaign.campaignId(), output);
        try {
            awaitFirstChunk();
            long firstId = db().queryForObject("SELECT JOB_EXECUTION_ID FROM BATCH_JOB_EXECUTION", Long.class);
            process.destroyForcibly();
            assertThat(process.waitFor(5, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(20)).until(() -> owner(campaign) == null);
            root("DROP TRIGGER slow_chunk");
            assertRows(3);
            assertThat(db().queryForObject("SELECT STATUS FROM BATCH_JOB_EXECUTION", String.class)).isEqualTo("STARTED");
            assertThat(command(campaign.campaignId())).isZero();
            assertRows(7);
            assertIdentities(campaign);
            var old = app.getBean(JobExplorer.class).getJobExecution(firstId);
            assertThat(old.getStatus()).isEqualTo(BatchStatus.FAILED);
            assertThat(checkpoint(old)).isEqualTo(3);
            assertThat(old.getExitStatus().getExitDescription()).contains("exclusive MySQL session");
            var resumed = app.getBean(JobExplorer.class).getJobExecution(run(campaign).getId());
            assertThat(resumed.getStepExecutions()).hasSize(1);
            assertThat(resumed.getJobInstance().getId()).isEqualTo(old.getJobInstance().getId());
            assertThat(resumed.getStepExecutions()).allSatisfy(step -> assertThat(step.getReadCount()).isEqualTo(4));
            assertThat(checkpoint(resumed)).isEqualTo(7);
            assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class)).isEqualTo(2);
            assertThat(endOffset()).isZero();
            System.out.println("CAMPAIGN_BATCH_KILL checkpoint=3 resumedRead=4 finalTargets=7 instances=1 executions=2");
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
            Files.deleteIfExists(output);
        }
    }

    @Test
    void pausedOldJvmCannotWriteBusinessOrMetadataAfterItsMysqlSessionIsKilled() throws Exception {
        Campaign campaign = requested(7);
        root("CREATE TRIGGER slow_chunk BEFORE INSERT ON campaign_target FOR EACH ROW BEGIN IF NEW.target_ordinal = 4 THEN DO SLEEP(15); END IF; END");
        Path output = Files.createTempFile("campaign-stale-owner-", ".log");
        Process process = startCommand(campaign.campaignId(), output);
        try {
            awaitFirstChunk();
            long ownerId = owner(campaign);
            signal(process, "-STOP");
            // Paused does not mean dead. A live MySQL session must block takeover, irrespective of age.
            assertThatThrownBy(() -> run(campaign)).isInstanceOf(JobExecutionAlreadyRunningException.class);
            root("KILL CONNECTION " + ownerId);
            await().atMost(Duration.ofSeconds(10)).until(() -> owner(campaign) == null);
            root("DROP TRIGGER slow_chunk");
            var resumed = run(campaign);
            assertThat(resumed.getStatus()).isEqualTo(BatchStatus.COMPLETED);
            assertThat(checkpoint(resumed)).isEqualTo(7);
            var business = new java.util.LinkedHashMap<String, List<Map<String, Object>>>();
            for (String table : List.of("notification", "delivery_task", "outbox_event", "campaign_target", "BATCH_JOB_EXECUTION_CONTEXT")) {
                business.put(table, snapshot(table));
            }
            var jobs = db().queryForList("SELECT * FROM BATCH_JOB_EXECUTION ORDER BY JOB_EXECUTION_ID");
            var steps = db().queryForList("SELECT * FROM BATCH_STEP_EXECUTION ORDER BY STEP_EXECUTION_ID");
            var contextsBefore = db().queryForList("SELECT * FROM BATCH_STEP_EXECUTION_CONTEXT ORDER BY STEP_EXECUTION_ID");
            signal(process, "-CONT");
            assertThat(process.waitFor(40, TimeUnit.SECONDS)).isTrue();
            assertThat(process.exitValue()).isEqualTo(1);
            assertRows(7);
            assertIdentities(campaign);
            assertThat(db().queryForList("SELECT * FROM BATCH_JOB_EXECUTION ORDER BY JOB_EXECUTION_ID")).isEqualTo(jobs);
            assertThat(db().queryForList("SELECT * FROM BATCH_STEP_EXECUTION ORDER BY STEP_EXECUTION_ID")).isEqualTo(steps);
            assertThat(db().queryForList("SELECT * FROM BATCH_STEP_EXECUTION_CONTEXT ORDER BY STEP_EXECUTION_ID")).isEqualTo(contextsBefore);
            for (var snapshot : business.entrySet()) {
                assertThat(snapshot(snapshot.getKey())).as(snapshot.getKey())
                        .containsExactlyInAnyOrderElementsOf(snapshot.getValue());
            }
            assertThat(owner(campaign)).isNull();
            System.out.println("CAMPAIGN_BATCH_FENCE pausedOwnerRejected=true staleExit=1 targets=7 metadataUnchanged=true");
        } finally {
            if (process.isAlive()) { signal(process, "-CONT"); process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
            Files.deleteIfExists(output);
        }
    }

    @Test
    void legacyUncertainExecutionIsNeverReclassifiedWithoutOwnershipEvidence() throws Exception {
        var campaign = requested(2);
        var repository = app.getBean(org.springframework.batch.core.repository.JobRepository.class);
        var parameters = new org.springframework.batch.core.JobParametersBuilder()
                .addString("campaignId", campaign.campaignId().toString()).toJobParameters();
        var legacy = repository.createJobExecution(CampaignBatchService.JOB_NAME, parameters);
        for (var status : List.of(BatchStatus.STARTING, BatchStatus.UNKNOWN)) {
            legacy.setStatus(status);
            repository.update(legacy);
            assertThatThrownBy(() -> run(campaign)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Legacy execution ownership");
            assertThat(db().queryForObject("SELECT STATUS FROM BATCH_JOB_EXECUTION", String.class)).isEqualTo(status.name());
            assertThat(owner(campaign)).isNull();
        }
        assertRows(0);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class)).isEqualTo(1);
    }

    @Test
    void unknownExecutionRecoveryIsAtomicAndPreservesCommittedCheckpoint() throws Exception {
        var campaign = requested(7);
        root("CREATE TRIGGER fail_target BEFORE INSERT ON campaign_target FOR EACH ROW BEGIN IF NEW.target_ordinal = 5 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test failure'; END IF; END");
        var failed = run(campaign);
        root("DROP TRIGGER fail_target");
        assertRows(3);
        // Model an uncertain commit result recorded by the framework after loss of the original session.
        db().update("UPDATE BATCH_JOB_EXECUTION SET STATUS='UNKNOWN', END_TIME=NULL");
        db().update("UPDATE BATCH_STEP_EXECUTION SET STATUS='UNKNOWN', END_TIME=NULL");
        var before = db().queryForList("SELECT * FROM BATCH_STEP_EXECUTION");
        root("CREATE TRIGGER fail_recovery BEFORE UPDATE ON BATCH_JOB_EXECUTION FOR EACH ROW BEGIN IF NEW.STATUS = 'FAILED' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'recovery storage failure'; END IF; END");
        try {
            assertThatThrownBy(() -> run(campaign)).isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThat(db().queryForList("SELECT * FROM BATCH_STEP_EXECUTION")).isEqualTo(before);
            assertThat(db().queryForObject("SELECT STATUS FROM BATCH_JOB_EXECUTION", String.class)).isEqualTo("UNKNOWN");
            assertRows(3);
            assertThat(owner(campaign)).isNull();
        } finally { root("DROP TRIGGER fail_recovery"); }
        var resumed = run(campaign);
        assertThat(resumed.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(resumed.getJobInstance().getId()).isEqualTo(failed.getJobInstance().getId());
        assertThat(resumed.getStepExecutions()).allSatisfy(step -> assertThat(step.getReadCount()).isEqualTo(4));
        assertRows(7);
    }

    @Test
    void reconnectingConnectionIsRejectedBeforeCreatingExecution() throws Exception {
        var properties = new org.springframework.boot.autoconfigure.jdbc.DataSourceProperties();
        properties.setUrl(url + "&autoReconnect=true");
        properties.setUsername(MYSQL.getUsername());
        properties.setPassword(MYSQL.getPassword());
        var campaign = requested(1);
        assertThatThrownBy(() -> CampaignBatchSession.open(properties, db().getDataSource(), campaign.campaignId()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("without reconnect");
        assertThat(owner(campaign)).isNull();
        assertRows(0);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class)).isZero();
    }

    @Test
    void executionParameterFailureCannotCommitPartialInstanceOrExecution() throws Exception {
        var campaign = requested(2);
        root("CREATE TRIGGER fail_parameters BEFORE INSERT ON BATCH_JOB_EXECUTION_PARAMS FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'parameter storage failure'");
        try {
            assertThatThrownBy(() -> run(campaign)).isInstanceOf(org.springframework.dao.DataAccessException.class);
            for (String table : List.of("BATCH_JOB_INSTANCE", "BATCH_JOB_EXECUTION", "BATCH_JOB_EXECUTION_PARAMS", "BATCH_JOB_EXECUTION_CONTEXT")) {
                assertThat(db().queryForObject("SELECT COUNT(*) FROM " + table, Integer.class)).as(table).isZero();
            }
            assertRows(0);
            assertThat(owner(campaign)).isNull();
        } finally { root("DROP TRIGGER fail_parameters"); }
        assertThat(run(campaign).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertRows(2);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE", Integer.class)).isEqualTo(1);
    }

    @Test
    void differentCampaignSessionsCanRunConcurrentlyWithoutSharingOwnership() throws Exception {
        var first = requested(6);
        var second = requested(6);
        var other = application(false);
        root("CREATE TRIGGER slow_target BEFORE INSERT ON campaign_target FOR EACH ROW DO SLEEP(0.2)");
        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstRun = executor.submit(() -> run(first));
            await().atMost(Duration.ofSeconds(5)).until(() -> owner(first) != null);
            var secondRun = executor.submit(() -> other.getBean(CampaignBatchService.class).execute(second.campaignId()));
            assertThat(firstRun.get(15, TimeUnit.SECONDS).getStatus()).isEqualTo(BatchStatus.COMPLETED);
            assertThat(secondRun.get(15, TimeUnit.SECONDS).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        } finally { root("DROP TRIGGER slow_target"); }
        assertRows(12);
        assertIdentities(first);
        assertIdentities(second);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE", Integer.class)).isEqualTo(2);
        assertThat(owner(first)).isNull();
        assertThat(owner(second)).isNull();
    }

    private List<Map<String, Object>> snapshot(String table) {
        var rows = db().queryForList("SELECT * FROM " + table);
        // JDBC returns a fresh byte[] for VARBINARY on each read; compare its bytes, not object identity.
        rows.forEach(row -> row.replaceAll((key, value) -> value instanceof byte[] bytes
                ? java.util.HexFormat.of().formatHex(bytes) : value));
        return rows;
    }

    private void awaitFirstChunk() {
        await().atMost(Duration.ofSeconds(40)).until(() ->
                db().queryForObject("SELECT COUNT(*) FROM campaign_target", Integer.class) == 3);
    }

    private Long owner(Campaign campaign) {
        return db().queryForObject("SELECT IS_USED_LOCK(?)", Long.class,
                CampaignBatchSession.lockName(db().queryForObject("SELECT DATABASE()", String.class), campaign.campaignId()));
    }

    private void signal(Process process, String signal) throws Exception {
        var command = new ProcessBuilder("kill", signal, Long.toString(process.pid())).start();
        assertThat(command.waitFor(5, TimeUnit.SECONDS)).isTrue();
        assertThat(command.exitValue()).isZero();
    }

    private int command(UUID id) throws Exception { return command(id, 0); }

    private int command(UUID id, int expectedExit) throws Exception {
        return command(id, expectedExit, Map.of());
    }

    private int command(UUID id, int expectedExit, Map<String, String> environment) throws Exception {
        boolean expectedOutcome = false;
        Path output = Files.createTempFile("campaign-command-", ".log");
        Process process = startCommand(id, output, environment);
        try {
            assertThat(process.waitFor(40, TimeUnit.SECONDS)).as("campaign command exited; log %s", output).isTrue();
            assertThat(Files.readString(output)).contains("CAMPAIGN_BATCH");
            expectedOutcome = process.exitValue() == expectedExit;
            return process.exitValue();
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
            if (expectedOutcome) Files.deleteIfExists(output);
            else {
                Path reports = Files.createDirectories(Path.of("build/reports/campaign-command-failures"));
                Path preserved = reports.resolve(output.getFileName());
                Files.move(output, preserved);
                System.out.println("CAMPAIGN_COMMAND_FAILURE log=" + preserved);
            }
        }
    }

    private Process startCommand(UUID id, Path output) throws Exception {
        return startCommand(id, output, Map.of());
    }

    private Process startCommand(UUID id, Path output, Map<String, String> environment) throws Exception {
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("notificationLab.testClasspath"), CampaignBatchCommand.class.getName(), id.toString());
        builder.environment().putAll(Map.of("SPRING_DATASOURCE_URL", url, "SPRING_DATASOURCE_USERNAME", MYSQL.getUsername(),
                "SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword(), "SPRING_KAFKA_BOOTSTRAP_SERVERS", KAFKA.getBootstrapServers(),
                "APP_ASYNC_TOPIC", topic, "APP_ASYNC_GROUP", topic, "LOGGING_LEVEL_ROOT", "WARN", "SPRING_MAIN_BANNER_MODE", "off", "APP_CAMPAIGN_BATCH_CHUNK_SIZE", "3"));
        builder.environment().remove("APP_CAMPAIGN_BATCH_GROUPED_OUTBOX");
        builder.environment().remove("SPRING_PROFILES_ACTIVE");
        builder.environment().putAll(environment);
        builder.redirectErrorStream(true).redirectOutput(output.toFile());
        return builder.start();
    }

    private Campaign requested(int count) {
        var service = app.getBean(CampaignService.class);
        var campaign = service.create(new CampaignRequest("batch", "title", "synthetic body", count), UUID.randomUUID().toString()).campaign();
        return service.start(campaign.campaignId()).campaign();
    }
    private JobExecution run(Campaign campaign) throws Exception { return app.getBean(CampaignBatchService.class).execute(campaign.campaignId()); }
    private JdbcTemplate db() { return app.getBean(JdbcTemplate.class); }
    private long checkpoint(JobExecution execution) {
        return app.getBean(JobExplorer.class).getJobExecution(execution.getId()).getStepExecutions().iterator().next()
                .getExecutionContext().getLong(CampaignTargetReader.CHECKPOINT);
    }
    private void assertRows(int count) {
        for (String table : List.of("notification", "delivery_task", "outbox_event", "campaign_target")) {
            assertThat(db().queryForObject("SELECT COUNT(*) FROM " + table, Integer.class)).as(table).isEqualTo(count);
        }
    }
    private void assertIdentities(Campaign campaign) {
        for (int ordinal = 1; ordinal <= campaign.targetCount(); ordinal++) {
            var ids = CampaignTargetIds.of(campaign.campaignId(), ordinal);
            assertThat(db().queryForMap("SELECT notification_id,task_id,event_id FROM campaign_target WHERE campaign_id=? AND target_ordinal=?",
                    campaign.campaignId().toString(), ordinal)).containsAllEntriesOf(Map.of("notification_id", ids.notificationId().toString(),
                    "task_id", ids.taskId().toString(), "event_id", ids.eventId().toString()));
        }
    }
    private void root(String sql) throws Exception {
        try (var connection = DriverManager.getConnection(url, "root", MYSQL.getPassword()); var statement = connection.createStatement()) { statement.execute(sql); }
    }
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> template() { return (KafkaTemplate<String, String>) app.getBean(KafkaTemplate.class); }
    private OutboxRelay relay(KafkaTemplate<String, String> kafka) {
        return new OutboxRelay(app.getBean(OutboxStore.class), kafka, app.getBean(AsyncProperties.class),
                app.getBean(MeterRegistry.class), app.getBean(TrafficRouting.class));
    }
    private long endOffset() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            String name = app.getBean(TrafficRouting.class).topic();
            var ends = new java.util.HashMap<TopicPartition, OffsetSpec>();
            admin.describeTopics(List.of(name)).allTopicNames().get(5, TimeUnit.SECONDS).get(name).partitions()
                    .forEach(partition -> ends.put(new TopicPartition(name, partition.partition()), OffsetSpec.latest()));
            return admin.listOffsets(ends).all().get(5, TimeUnit.SECONDS).values().stream().mapToLong(info -> info.offset()).sum();
        }
    }
    private long committedOffset(ConfigurableApplicationContext context) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            return admin.listConsumerGroupOffsets(context.getBean(TrafficRouting.class).group()).partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS).values().stream().mapToLong(offset -> offset.offset()).sum();
        }
    }
    @AfterEach
    void close() {
        for (int i = contexts.size() - 1; i >= 0; i--) contexts.get(i).close();
    }
}
