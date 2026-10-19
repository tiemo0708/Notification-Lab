package dev.jinhyeon.notificationlab.campaign.batch;

import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.campaign.Campaign;
import dev.jinhyeon.notificationlab.campaign.CampaignRequest;
import dev.jinhyeon.notificationlab.campaign.CampaignService;
import dev.jinhyeon.notificationlab.campaign.SyntheticCampaignAudience;
import dev.jinhyeon.notificationlab.config.AsyncProperties;
import dev.jinhyeon.notificationlab.config.TrafficRouting;
import dev.jinhyeon.notificationlab.outbox.OutboxRelay;
import dev.jinhyeon.notificationlab.outbox.OutboxStore;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.item.Chunk;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
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

    private ConfigurableApplicationContext application(boolean worker) {
        var context = new SpringApplicationBuilder(NotificationLabApplication.class).web(WebApplicationType.NONE).run(
                "--app.campaign.batch.enabled=true", "--app.campaign.batch.chunk-size=3",
                "--app.traffic.lane=CAMPAIGN", "--app.traffic.split-enabled=true",
                "--app.rate-limit.client-enabled=true", "--app.async.worker-enabled=" + worker,
                "--app.async.relay-enabled=false", "--app.mock.delay-ms=0",
                "--spring.datasource.url=" + url, "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(), "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + topic,
                "--app.rate-limit.port=" + REDIS.getMappedPort(6379), "--app.rate-limit.key=" + quotaKey,
                "--app.rate-limit.rate-per-second=100", "--app.rate-limit.burst=100",
                "--app.traffic.campaign-rate=50", "--app.traffic.campaign-burst=50",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        contexts.add(context);
        return context;
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

    @Test
    void draftAndUnknownCampaignsCannotCreateBatchExecutions() throws Exception {
        Campaign draft = app.getBean(CampaignService.class).create(new CampaignRequest("draft", "title", "body", 2), UUID.randomUUID().toString()).campaign();
        assertThatThrownBy(() -> run(draft)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> app.getBean(CampaignBatchService.class).execute(UUID.randomUUID()))
                .isInstanceOf(dev.jinhyeon.notificationlab.campaign.CampaignNotFoundException.class);
        assertThat(command(draft.campaignId())).isEqualTo(1);
        assertThat(db().queryForObject("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE", Integer.class)).isZero();
        assertRows(0);
    }

    private int command(UUID id) throws Exception {
        Path output = Files.createTempFile("campaign-command-", ".log");
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("notificationLab.testClasspath"), CampaignBatchCommand.class.getName(), id.toString());
        builder.environment().putAll(Map.of("SPRING_DATASOURCE_URL", url, "SPRING_DATASOURCE_USERNAME", MYSQL.getUsername(),
                "SPRING_DATASOURCE_PASSWORD", MYSQL.getPassword(), "SPRING_KAFKA_BOOTSTRAP_SERVERS", KAFKA.getBootstrapServers(),
                "APP_ASYNC_TOPIC", topic, "APP_ASYNC_GROUP", topic, "LOGGING_LEVEL_ROOT", "WARN", "SPRING_MAIN_BANNER_MODE", "off"));
        builder.redirectErrorStream(true).redirectOutput(output.toFile());
        Process process = builder.start();
        try {
            assertThat(process.waitFor(40, TimeUnit.SECONDS)).as("campaign command exited; log %s", output).isTrue();
            assertThat(Files.readString(output)).contains("CAMPAIGN_BATCH");
            return process.exitValue();
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
            Files.deleteIfExists(output);
        }
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
