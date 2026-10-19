package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.campaign.CampaignRequest;
import dev.jinhyeon.notificationlab.campaign.CampaignService;
import dev.jinhyeon.notificationlab.config.TrafficRouting;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in experiment. No HTTP/provider workload: measures durable Outbox handoff only. */
@Tag("campaign-load")
class CampaignLoadTest {
    private static final int CHUNK = 100;
    private static final long MYSQL_MEMORY = 2L * 1024 * 1024 * 1024;

    // Materialize the bounded keyset page before primary-key lookups; no whole-dataset JOIN/sort.
    private static final String VERIFY_PAGE = """
                    SELECT /*+ NO_BNL(n, t, o) */ c.target_ordinal, c.notification_id, c.task_id, c.event_id,
                           n.recipient_id, t.target_id, t.notification_id AS task_notification,
                           o.notification_id AS event_notification, o.payload->>'$.taskId' AS payload_task
                    FROM (SELECT target_ordinal, notification_id, task_id, event_id FROM campaign_target
                          WHERE campaign_id=? AND target_ordinal>? ORDER BY target_ordinal LIMIT 1000) c
                    STRAIGHT_JOIN notification n FORCE INDEX (PRIMARY) ON n.notification_id=c.notification_id
                    STRAIGHT_JOIN delivery_task t FORCE INDEX (PRIMARY) ON t.task_id=c.task_id STRAIGHT_JOIN outbox_event o FORCE INDEX (PRIMARY) ON o.event_id=c.event_id
                    ORDER BY c.target_ordinal
                    """;

    @Test
    @Timeout(value = 110, unit = TimeUnit.MINUTES)
    void measuresDurableHandoffAndReconcilesEveryTarget() throws Exception {
        int targets = Integer.getInteger("campaign.load.targets", 100_000);
        if (targets < 1 || targets > 1_000_000) throw new IllegalArgumentException("Targets must be 1..1000000");
        Path output = Path.of(System.getProperty("campaign.load.output", "build/reports/campaign-load"));
        Files.createDirectories(output);
        var report = new LinkedHashMap<String, Object>();
        report.put("startedAtUtc", Instant.now().toString());
        report.put("scope", "campaign DB Outbox handoff; no provider delivery");
        report.put("targets", targets);
        report.put("chunkSize", CHUNK);
        report.put("writerConcurrency", 1);
        report.put("relayEnabled", false);
        report.put("workerEnabled", false);
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"));
        report.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        report.put("mysql", "8.4.8");
        report.put("mysqlMemoryLimitBytes", MYSQL_MEMORY);
        report.put("mysqlCpuLimit", 2);
        report.put("kafka", "4.0.2");
        report.put("sampleIntervalSeconds", 5);
        report.put("verificationQueryVersion", "bounded-primary-key-page-v3");
        var originalZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try (var mysql = new MySQLContainer<>("mysql:8.4.8")
                .withDatabaseName("notification_lab").withUsername("notification_test").withPassword(UUID.randomUUID().toString())
                .withCommand("--default-time-zone=+00:00", "--innodb-buffer-pool-size=536870912")
                .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
                .withUrlParam("connectTimeout", "3000").withUrlParam("socketTimeout", "60000")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(MYSQL_MEMORY).withNanoCPUs(2_000_000_000L)
                        .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port ->
                                new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
             var kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.2"))
                     .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
                     .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(1024L * 1024 * 1024).withNanoCPUs(1_000_000_000L)
                             .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port ->
                                     new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()))) {
            mysql.start();
            kafka.start();
            try (var app = new SpringApplicationBuilder(NotificationLabApplication.class).web(WebApplicationType.NONE).run(
                    "--app.campaign.batch.enabled=true", "--app.campaign.batch.chunk-size=" + CHUNK,
                    "--app.traffic.lane=CAMPAIGN", "--app.traffic.split-enabled=true", "--app.rate-limit.client-enabled=true",
                    "--app.async.worker-enabled=false", "--app.async.relay-enabled=false",
                    "--spring.datasource.url=" + mysql.getJdbcUrl(), "--spring.datasource.username=" + mysql.getUsername(),
                    "--spring.datasource.password=" + mysql.getPassword(), "--spring.datasource.hikari.maximum-pool-size=4",
                    "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                    "--logging.level.root=WARN", "--spring.main.banner-mode=off")) {
                var jdbc = app.getBean(JdbcTemplate.class);
                report.put("mysqlSettings", jdbc.queryForMap("SELECT @@innodb_buffer_pool_size AS bufferPoolBytes, "
                        + "@@innodb_flush_log_at_trx_commit AS flushLogAtCommit, @@sync_binlog AS syncBinlog"));
                var campaigns = app.getBean(CampaignService.class);
                var campaign = campaigns.create(new CampaignRequest("load fixture", "title", "synthetic body", targets),
                        UUID.randomUUID().toString()).campaign();
                campaigns.start(campaign.campaignId());
                var samples = new ArrayList<Map<String, Object>>();
                var monitor = Executors.newSingleThreadScheduledExecutor();
                long started = System.nanoTime();
                report.put("dbStatusBefore", dbStatus(jdbc));
                samples.add(sample(jdbc, mysql, started, campaign.campaignId()));
                monitor.scheduleWithFixedDelay(() -> {
                    try {
                        var value = sample(jdbc, mysql, started, campaign.campaignId());
                        samples.add(value); // Read only after executor termination.
                        System.out.printf("CAMPAIGN_LOAD_PROGRESS targets=%d committed=%s elapsedSeconds=%s%n",
                                targets, value.get("committedOrdinal"), value.get("elapsedSeconds"));
                    } catch (Exception failure) {
                        samples.add(Map.of("sampleErrorType", failure.getClass().getSimpleName()));
                    }
                }, 5, 5, TimeUnit.SECONDS);
                org.springframework.batch.core.JobExecution execution;
                double elapsed;
                try {
                    execution = app.getBean(CampaignBatchService.class).execute(campaign.campaignId());
                    elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
                } finally {
                    monitor.shutdown();
                    if (!monitor.awaitTermination(70, TimeUnit.SECONDS)) {
                        monitor.shutdownNow();
                        throw new IllegalStateException("Resource sampler did not terminate");
                    }
                    report.put("samples", samples);
                }
                samples.add(sample(jdbc, mysql, started, campaign.campaignId()));
                report.put("elapsedSeconds", elapsed);
                report.put("targetsPerSecond", targets / elapsed);
                report.put("dbStatusAfter", dbStatus(jdbc));
                report.put("batchStatus", execution.getStatus().name());
                assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
                var step = app.getBean(JobExplorer.class).getJobExecution(execution.getId()).getStepExecutions().iterator().next();
                assertThat(step.getReadCount()).isEqualTo(targets);
                assertThat(step.getWriteCount()).isEqualTo(targets);
                assertThat(step.getExecutionContext().getLong(CampaignTargetReader.CHECKPOINT)).isEqualTo(targets);
                report.put("checkpoint", targets);
                report.put("commitCount", step.getCommitCount());
                report.put("rollbackCount", step.getRollbackCount());
                var verificationPlan = jdbc.queryForList("EXPLAIN " + VERIFY_PAGE, campaign.campaignId().toString(), 0);
                report.put("verificationPlan", verificationPlan);
                verificationPlan.stream().filter(row -> List.of("n", "t", "o").contains(row.get("table")))
                        .forEach(row -> {
                            assertThat(row.get("type")).as("verification PK lookup: " + row.get("table")).isEqualTo("eq_ref");
                            assertThat(row.get("key")).isEqualTo("PRIMARY");
                        });
                long verifyStarted = System.nanoTime();
                assertCounts(jdbc, targets);
                report.put("mappingSha256", verifyEveryMapping(jdbc, campaign.campaignId(), targets));
                report.put("verificationSeconds", (System.nanoTime() - verifyStarted) / 1_000_000_000.0);
                long replayStarted = System.nanoTime();
                assertThat(app.getBean(CampaignBatchService.class).execute(campaign.campaignId()).getId()).isEqualTo(execution.getId());
                report.put("completedReplaySeconds", (System.nanoTime() - replayStarted) / 1_000_000_000.0);
                assertCounts(jdbc, targets);
                assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Long.class)).isEqualTo(1);
                assertThat(kafkaRecords(kafka, app.getBean(TrafficRouting.class).topic())).isZero();
                report.put("kafkaRecords", 0);
                report.put("providerAttempts", 0);
                report.put("verifiedTargets", targets);
                report.put("result", "PASS");
                System.out.printf("CAMPAIGN_LOAD_RESULT targets=%d elapsedSeconds=%.3f targetsPerSecond=%.3f checkpoint=%d kafkaRecords=0%n",
                        targets, elapsed, targets / elapsed, targets);
            }
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL");
            report.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            TimeZone.setDefault(originalZone);
            report.put("finishedAtUtc", Instant.now().toString());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.resolve("handoff-" + targets + ".json").toFile(), report);
        }
    }

    private Map<String, Object> sample(JdbcTemplate jdbc, MySQLContainer<?> mysql, long start, UUID campaignId) throws Exception {
        var sample = new LinkedHashMap<String, Object>();
        sample.put("elapsedSeconds", (System.nanoTime() - start) / 1_000_000_000.0);
        sample.put("heapUsedBytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        sample.put("processCpuNanos", ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime());
        sample.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
        sample.put("gcCount", ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(gc -> gc.getCollectionCount()).sum());
        sample.put("gcMillis", ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(gc -> gc.getCollectionTime()).sum());
        sample.put("committedOrdinal", jdbc.queryForObject("SELECT COALESCE(MAX(target_ordinal),0) FROM campaign_target WHERE campaign_id=?", Long.class, campaignId.toString()));
        var rss = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(ProcessHandle.current().pid())).start();
        if (rss.waitFor(2, TimeUnit.SECONDS) && rss.exitValue() == 0) {
            sample.put("processRssBytes", Long.parseLong(new String(rss.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip()) * 1024);
        } else { rss.destroyForcibly(); sample.put("rssUnavailable", true); }
        var cgroup = mysql.execInContainer("cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/cpu.stat");
        if (cgroup.getExitCode() == 0) {
            var lines = cgroup.getStdout().lines().toList();
            sample.put("mysqlCgroupMemoryBytes", Long.parseLong(lines.getFirst()));
            lines.stream().filter(line -> line.startsWith("usage_usec ")).findFirst()
                    .ifPresent(line -> sample.put("mysqlCgroupCpuMicros", Long.parseLong(line.substring(11).strip())));
        } else { sample.put("cgroupUnavailable", true); }
        return sample;
    }

    private Map<String, String> dbStatus(JdbcTemplate jdbc) {
        var status = new LinkedHashMap<String, String>();
        jdbc.query("SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Innodb_rows_inserted','Innodb_data_written','Innodb_os_log_written')",
                (org.springframework.jdbc.core.RowCallbackHandler) rs -> status.put(rs.getString(1), rs.getString(2)));
        return status;
    }

    private void assertCounts(JdbcTemplate jdbc, int targets) {
        for (String table : List.of("notification", "delivery_task", "outbox_event", "campaign_target")) {
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class)).as(table).isEqualTo(targets);
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM notification WHERE status='QUEUED' AND traffic_class='CAMPAIGN'", Long.class)).isEqualTo(targets);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PENDING' AND traffic_class='CAMPAIGN'", Long.class)).isEqualTo(targets);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM delivery_attempt", Long.class)).isZero();
    }

    private String verifyEveryMapping(JdbcTemplate jdbc, UUID id, int targets) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        for (int after = 0; after < targets;) {
            var rows = jdbc.queryForList(VERIFY_PAGE, id.toString(), after);
            assertThat(rows).hasSize(Math.min(1000, targets - after));
            for (var row : rows) {
                int ordinal = ((Number) row.get("target_ordinal")).intValue();
                assertThat(ordinal).isEqualTo(++after);
                var ids = CampaignTargetIds.of(id, ordinal);
                assertThat(row.get("notification_id")).isEqualTo(ids.notificationId().toString());
                assertThat(row.get("task_id")).isEqualTo(ids.taskId().toString());
                assertThat(row.get("event_id")).isEqualTo(ids.eventId().toString());
                assertThat(row.get("recipient_id")).isEqualTo("synthetic-v1-" + ordinal);
                assertThat((byte[]) row.get("target_id")).isEqualTo(("synthetic-v1-" + ordinal).getBytes(StandardCharsets.UTF_8));
                assertThat(row.get("task_notification")).isEqualTo(row.get("notification_id"));
                assertThat(row.get("event_notification")).isEqualTo(row.get("notification_id"));
                assertThat(row.get("payload_task")).isEqualTo(row.get("task_id"));
                digest.update((ordinal + ":" + ids.notificationId() + ":" + ids.taskId() + ":" + ids.eventId() + "\n").getBytes(StandardCharsets.UTF_8));
            }
            if (after % 100_000 == 0 || after == targets) {
                System.out.printf("CAMPAIGN_LOAD_VERIFY verified=%d targets=%d%n", after, targets);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private long kafkaRecords(KafkaContainer kafka, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var offsets = new LinkedHashMap<TopicPartition, OffsetSpec>();
            admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic).partitions()
                    .forEach(partition -> offsets.put(new TopicPartition(topic, partition.partition()), OffsetSpec.latest()));
            return admin.listOffsets(offsets).all().get(10, TimeUnit.SECONDS).values().stream().mapToLong(info -> info.offset()).sum();
        }
    }
}
