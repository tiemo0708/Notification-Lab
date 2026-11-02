package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariDataSource;
import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import dev.jinhyeon.notificationlab.config.TrafficProperties;
import dev.jinhyeon.notificationlab.notification.JdbcNotificationStore;
import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import dev.jinhyeon.notificationlab.worker.DeliveryTaskStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/** Diagnostic observation, not a throughput benchmark or a reproduction of the million-run failure. */
@Tag("outbox-query-probe")
class OutboxQueryProbeTest {
    // Deliberately mirrors PipelineMetrics.refresh; check it against production when changing that query.
    private static final String METRICS = """
            SELECT COUNT(*), COALESCE(SUM(status = 'FAILED'), 0),
                COALESCE(TIMESTAMPDIFF(MICROSECOND, MIN(created_at), UTC_TIMESTAMP(6)) / 1000000.0, 0)
            FROM outbox_event WHERE status <> 'PUBLISHED' AND traffic_class = ?
            """;
    private final Map<String, Object> report = new LinkedHashMap<>();

    @Test
    void observesProductionClaimsAndMetricsWithoutChangingIndexesOrTimeouts() throws Exception {
        report.put("startedAtUtc", Instant.now().toString());
        report.put("scope", "SQL only; synthetic 10k/100k campaign backlog; no Kafka, Worker, HTTP or provider");
        report.put("conditions", Map.of("mysql", "8.4.8", "cpu", 2, "memoryMiB", 2048,
                "bufferPoolMiB", 512, "poolPerLane", 4, "iterationsPerOperation", 20,
                "concurrentAdmissions", 40, "concurrentCampaignClaims", 100, "concurrentMetricsSamples", 10));
        report.put("diagnosticTiming", Map.of("metricsIntervalMillis", 200, "admissionIntervalMillis", 50,
                "jdbcQueryTimeoutSeconds", 30, "connectionTimeoutMillis", 3000,
                "futureWaitSeconds", 60, "junitMinutes", 8, "taskMinutes", 10));
        report.put("limitations", List.of("one run; warm caches; fixed operation order",
                "SQL UUID() seeding differs from campaign Batch IDs and timestamp distribution",
                "manual transactions preserve store boundaries; no Kafka ACK or provider acceptance",
                "metrics sampled more frequently than production; no Worker DB transactions",
                "timings observe this host only; do not explain the previous 1m timeout"));
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        try (var mysql = new MySQLContainer<>("mysql:8.4.8").withDatabaseName("notification_lab")
                .withUsername("notification_test").withPassword(UUID.randomUUID().toString())
                .withCommand("--default-time-zone=+00:00", "--innodb-buffer-pool-size=536870912")
                .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
                .withUrlParam("connectTimeout", "3000").withUrlParam("socketTimeout", "60000")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(2048L * 1024 * 1024)
                        .withMemorySwap(2048L * 1024 * 1024).withNanoCPUs(2_000_000_000L)
                        .withPortBindings(new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), new ExposedPort(3306))))) {
            mysql.start();
            Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()).load().migrate();
            try (var campaignPool = pool(mysql); var realtimePool = pool(mysql)) {
                var campaign = new CapturingJdbc(campaignPool);
                var realtime = new CapturingJdbc(realtimePool);
                var campaignTx = new TransactionTemplate(new JdbcTransactionManager(campaignPool));
                var realtimeTx = new TransactionTemplate(new JdbcTransactionManager(realtimePool));
                var campaignStore = new OutboxStore(campaign, traffic(TrafficProperties.Lane.CAMPAIGN));
                var realtimeStore = new OutboxStore(realtime, traffic(TrafficProperties.Lane.REALTIME));
                var submissions = new SubmissionService(new JdbcNotificationStore(realtime), realtime, Clock.systemUTC(),
                        new DeliveryTaskStore(realtime, new DeliveryProperties(3, 1000, 60000)), traffic(TrafficProperties.Lane.REALTIME));
                // SQL bulk seeding excludes Batch execution cost; IDs and payloads never leave the isolated DB.
                campaign.execute("CREATE TABLE diagnostic_digits (n INT PRIMARY KEY)");
                campaign.execute("INSERT INTO diagnostic_digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)");
                campaign.execute("CREATE TABLE diagnostic_seed (ordinal INT PRIMARY KEY, nid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin, tid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin, eid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin)");
                var phases = new ArrayList<Map<String, Object>>();
                report.put("phases", phases);
                int seeded = 0;
                for (int size : List.of(10_000, 100_000)) {
                    long seedStart = System.nanoTime();
                    while (seeded < size) {
                        int base = seeded;
                        campaignTx.executeWithoutResult(tx -> seed(campaign, base));
                        seeded += 1000;
                    }
                    var phase = new LinkedHashMap<String, Object>();
                    phases.add(phase);
                    phase.put("campaignRows", size);
                    phase.put("additionalSeedMillis", millis(seedStart));
                    phase.put("startingStatuses", campaign.queryForList("SELECT status, COUNT(*) AS rows_count FROM outbox_event WHERE traffic_class='CAMPAIGN' GROUP BY status"));
                    assertThat(campaign.queryForObject("SELECT COUNT(*) FROM notification WHERE traffic_class='CAMPAIGN'", Integer.class)).isEqualTo(size);
                    for (var lane : List.of(TrafficProperties.Lane.CAMPAIGN, TrafficProperties.Lane.REALTIME)) {
                        var jdbc = lane == TrafficProperties.Lane.CAMPAIGN ? campaign : realtime;
                        var store = lane == TrafficProperties.Lane.CAMPAIGN ? campaignStore : realtimeStore;
                        var tx = lane == TrafficProperties.Lane.CAMPAIGN ? campaignTx : realtimeTx;
                        var metrics = new double[20];
                        var claims = new double[20];
                        Map<String, Object> aggregate = null;
                        for (int i = 0; i < 20; i++) {
                            long start = System.nanoTime();
                            aggregate = jdbc.queryForMap(METRICS, lane.name());
                            metrics[i] = millis(start);
                            start = System.nanoTime();
                            claimAndMarkPublished(tx, store, lane == TrafficProperties.Lane.CAMPAIGN);
                            claims[i] = millis(start);
                        }
                        phase.put(lane.name(), Map.of("metricsMillis", percentiles(metrics), "claimAndMarkMillis", percentiles(claims),
                                "lastMetrics", aggregate, "metricsPlan", jdbc.queryForList("EXPLAIN " + METRICS, lane.name()),
                                "candidatePlans", jdbc.plans()));
                    }
                    var start = new CountDownLatch(1);
                    var admissionTimes = new double[40];
                    try (var executor = Executors.newFixedThreadPool(3)) {
                        var relay = executor.submit(() -> {
                            assertThat(start.await(3, TimeUnit.SECONDS)).isTrue();
                            for (int i = 0; i < 100; i++) claimAndMarkPublished(campaignTx, campaignStore, true);
                            return null;
                        });
                        var metrics = executor.submit(() -> {
                            assertThat(start.await(3, TimeUnit.SECONDS)).isTrue();
                            for (int i = 0; i < 10; i++) {
                                campaign.queryForMap(METRICS, "CAMPAIGN");
                                TimeUnit.MILLISECONDS.sleep(200);
                            }
                            return null;
                        });
                        var admissions = executor.submit(() -> {
                            assertThat(start.await(3, TimeUnit.SECONDS)).isTrue();
                            for (int i = 0; i < admissionTimes.length; i++) {
                                long begin = System.nanoTime();
                                realtimeTx.executeWithoutResult(tx -> submissions.accept(
                                        new NotificationRequest("synthetic-probe", "diagnostic", "synthetic body"), null));
                                admissionTimes[i] = millis(begin);
                                TimeUnit.MILLISECONDS.sleep(50);
                            }
                            return null;
                        });
                        start.countDown();
                        relay.get(60, TimeUnit.SECONDS);
                        metrics.get(60, TimeUnit.SECONDS);
                        admissions.get(60, TimeUnit.SECONDS);
                    }
                    phase.put("overlappingAdmissionDbMillis", percentiles(admissionTimes));
                    phase.put("realtimeRows", realtime.queryForObject("SELECT COUNT(*) FROM notification WHERE traffic_class='REALTIME'", Integer.class));
                    assertThat(realtime.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE traffic_class='REALTIME'", Integer.class)).isEqualTo(size == 10_000 ? 40 : 80);
                    System.out.printf("OUTBOX_QUERY_PROBE rows=%d admissionMaxMillis=%.3f%n", size, Arrays.stream(admissionTimes).max().orElseThrow());
                }
                report.put("result", "PASS");
            }
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            report.put("finishedAtUtc", Instant.now().toString());
            Path output = Path.of("build/reports/outbox-query-probe/result.json");
            Files.createDirectories(output.getParent());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
        }
    }

    private static void seed(JdbcTemplate jdbc, int base) {
        jdbc.update("INSERT INTO diagnostic_seed SELECT ? + a.n + 10*b.n + 100*c.n + 1, UUID(), UUID(), UUID() FROM diagnostic_digits a CROSS JOIN diagnostic_digits b CROSS JOIN diagnostic_digits c", base);
        jdbc.update("INSERT INTO notification (notification_id,recipient_id,title,body,status,traffic_class,created_at) SELECT nid,'synthetic-target','CAMPAIGN','synthetic body','QUEUED','CAMPAIGN',UTC_TIMESTAMP(6) FROM diagnostic_seed WHERE ordinal>? AND ordinal<=?", base, base + 1000);
        jdbc.update("INSERT INTO delivery_task (task_id,notification_id,target_id,max_attempts,created_at) SELECT tid,nid,CONVERT('synthetic-target' USING binary),3,UTC_TIMESTAMP(6) FROM diagnostic_seed WHERE ordinal>? AND ordinal<=?", base, base + 1000);
        jdbc.update("INSERT INTO outbox_event (event_id,notification_id,event_type,payload,traffic_class,created_at,available_at) SELECT eid,nid,'NOTIFICATION_REQUESTED',JSON_OBJECT('schemaVersion',2,'eventId',eid,'notificationId',nid,'taskId',tid),'CAMPAIGN',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6) FROM diagnostic_seed WHERE ordinal>? AND ordinal<=?", base, base + 1000);
    }

    private static void claimAndMarkPublished(TransactionTemplate tx, OutboxStore store, boolean expected) {
        // Explicit templates supply the same two transaction boundaries as production's Spring proxies.
        var claim = tx.execute(status -> store.claim(10));
        assertThat(claim).isNotNull();
        if (expected) assertThat(claim).isPresent();
        claim.ifPresent(value -> {
            Boolean published = tx.execute(status -> store.published(value));
            assertThat(published).isTrue();
        });
        // No Kafka ACK is asserted here: this isolated DB probe must never be used as delivery evidence.
    }

    private static TrafficProperties traffic(TrafficProperties.Lane lane) {
        return new TrafficProperties(lane, true, 200, 20);
    }

    private static HikariDataSource pool(MySQLContainer<?> mysql) {
        var pool = new HikariDataSource();
        pool.setJdbcUrl(mysql.getJdbcUrl()); pool.setUsername(mysql.getUsername()); pool.setPassword(mysql.getPassword());
        pool.setMaximumPoolSize(4); pool.setMinimumIdle(1); pool.setConnectionTimeout(3000);
        return pool;
    }

    private static double millis(long start) { return (System.nanoTime() - start) / 1_000_000.0; }

    private static Map<String, Double> percentiles(double[] samples) {
        var sorted = samples.clone(); Arrays.sort(sorted);
        return Map.of("p50", sorted[(int) Math.ceil(sorted.length * .50) - 1],
                "p95", sorted[(int) Math.ceil(sorted.length * .95) - 1], "max", sorted[sorted.length - 1]);
    }

    private static class CapturingJdbc extends JdbcTemplate {
        private record Query(String sql, Object[] args) { }
        private final Map<String, Query> queries = new LinkedHashMap<>();
        CapturingJdbc(HikariDataSource pool) { super(pool); setQueryTimeout(30); }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
            if (sql.contains("FOR UPDATE SKIP LOCKED") && !queries.containsKey(args[1].toString())) {
                queries.put(args[1].toString(), new Query(sql, args.clone()));
            }
            return super.query(sql, mapper, args);
        }

        Map<String, Object> plans() {
            var plans = new LinkedHashMap<String, Object>();
            queries.forEach((status, query) -> plans.put(status, queryForList("EXPLAIN " + query.sql(), query.args())));
            queries.clear();
            return plans;
        }
    }
}
