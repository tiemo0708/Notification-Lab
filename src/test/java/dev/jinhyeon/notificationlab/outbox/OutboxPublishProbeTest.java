package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.claimIsolation;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.createSeedTables;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.mysql;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.pool;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.seed;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.traffic;
import static org.assertj.core.api.Assertions.assertThat;

/** Held SQL-only observations, separate from timed HTTP delivery. No Kafka ACK claim. */
@Tag("outbox-publish-probe")
class OutboxPublishProbeTest {
    @Test
    void observesPublishPlansAndCrossLaneProgressWithAHundredThousandPendingRows() throws Exception {
        var report = new LinkedHashMap<String, Object>();
        report.put("startedAtUtc", Instant.now().toString());
        report.put("diagnosticsVersion", "separate-explain-point-updates-and-legacy-control-v3");
        report.put("scope", "SQL-only; 100000 campaign +100 realtime synthetic rows; publication transactions deliberately held; no HTTP/Kafka/Worker");
        report.put("conditions", Map.of("mysql", "8.4.8", "cpu", 2, "memoryMiB", 2048,
                "bufferPoolMiB", 512, "holderPoolMax", 4, "probePoolMax", 4, "observerPoolMax", 1));
        report.put("bounds", Map.of("queryTimeoutSeconds", 5, "probeLockTimeoutSeconds", 3,
                "heldObservationMillis", 1000, "latchSeconds", 3, "futureSeconds", 8, "executorShutdownSeconds", 5));
        var cases = new ArrayList<Map<String, Object>>();
        report.put("cases", cases);
        try (var mysql = mysql()) {
            mysql.start();
            Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()).load().migrate();
            try (var source = pool(mysql); var probeSource = pool(mysql); var observerSource = pool(mysql)) {
                probeSource.setConnectionInitSql("SET SESSION innodb_lock_wait_timeout=3");
                observerSource.setMaximumPoolSize(1);
                observerSource.setUsername("root");
                var jdbc = new PublishingJdbc(source);
                var probe = jdbc(probeSource);
                var observer = jdbc(observerSource);
                jdbc.explaining = observer;
                var tx = tx(source, -1);
                var claimTx = tx(source, claimIsolation());
                var probeTx = tx(probeSource, -1);
                var probeClaimTx = tx(probeSource, claimIsolation());
                createSeedTables(jdbc);
                for (int base = 0; base < 100_000; base += 1000) {
                    int start = base;
                    tx.executeWithoutResult(status -> seed(jdbc, start));
                }
                var realtime = traffic(TrafficProperties.Lane.REALTIME);
                var submissions = new SubmissionService(new JdbcNotificationStore(probe), probe, Clock.systemUTC(),
                        new DeliveryTaskStore(probe, new DeliveryProperties(3, 1000, 60000)), realtime);
                for (int i = 0; i < 100; i++) probeTx.executeWithoutResult(status -> submissions.accept(request(), null));
                assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event", Integer.class)).isEqualTo(100100);
                var campaignStore = new OutboxStore(jdbc, traffic(TrafficProperties.Lane.CAMPAIGN));
                var realtimeStore = new OutboxStore(probe, realtime);
                var claims = new ArrayList<OutboxStore.Claim>();
                for (int i = 0; i < 10; i++) claims.add(claimTx.execute(status -> campaignStore.claim(10).orElseThrow()));
                var realtimeClaim = probeClaimTx.execute(status -> realtimeStore.claim(10).orElseThrow());
                var before = snapshot(jdbc);
                report.put("initialData", before);
                report.put("statisticsSettings", jdbc.queryForList("SHOW VARIABLES WHERE Variable_name IN ('innodb_stats_auto_recalc','innodb_stats_persistent')"));
                for (String strategy : List.of("POINT_SINGLE", "POINT_BATCH", "LEGACY_OR_CONTROL")) {
                    int size = strategy.equals("POINT_SINGLE") ? 1 : 10;
                    boolean legacy = strategy.equals("LEGACY_OR_CONTROL");
                    var result = new LinkedHashMap<String, Object>();
                    result.put("batchSize", size);
                    result.put("strategy", strategy);
                    cases.add(result);
                    var started = new CountDownLatch(3);
                    var connections = List.of(new AtomicLong(), new AtomicLong(), new AtomicLong());
                    var futures = new ArrayList<Future<Map<String, Object>>>();
                    var executor = new ThreadPoolExecutor(3, 3, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(3), Thread.ofPlatform().daemon(true).factory());
                    try {
                        tx.executeWithoutResult(status -> {
                            long holder = thread(observer, jdbc.queryForObject("SELECT CONNECTION_ID()", Long.class));
                            result.put("holderIsolation", jdbc.queryForObject("SELECT @@transaction_isolation", String.class));
                            jdbc.plans.clear();
                            jdbc.capture = true;
                            int changed;
                            try { changed = legacy ? legacyPublish(jdbc, claims)
                                    : size == 1 ? (campaignStore.published(claims.getFirst()) ? 1 : 0) : campaignStore.published(claims); }
                            finally { jdbc.capture = false; }
                            assertThat(changed).isEqualTo(size);
                            result.put("changedRows", changed);
                            result.put("explainBeforeUpdates", List.copyOf(jdbc.plans));
                            result.put("holderLocks", observer.queryForList("SELECT INDEX_NAME,LOCK_TYPE,LOCK_MODE,LOCK_STATUS,COUNT(*) AS record_count, SUM(CASE WHEN LOCK_DATA='supremum pseudo-record' THEN 1 ELSE 0 END) AS supremum_count, SUM(CASE WHEN LOCK_DATA IS NULL THEN 1 ELSE 0 END) AS unknown_data_count FROM performance_schema.data_locks WHERE THREAD_ID=? AND OBJECT_SCHEMA='notification_lab' AND OBJECT_NAME='outbox_event' GROUP BY INDEX_NAME,LOCK_TYPE,LOCK_MODE,LOCK_STATUS ORDER BY INDEX_NAME,LOCK_MODE", holder));
                            futures.add(executor.submit(() -> probe(probeTx, probe, connections.get(0), started, () -> realtimeStore.published(realtimeClaim) ? 1 : 0)));
                            futures.add(executor.submit(() -> probe(probeClaimTx, probe, connections.get(1), started, () -> realtimeStore.claim(10).isPresent() ? 1 : 0)));
                            futures.add(executor.submit(() -> probe(probeTx, probe, connections.get(2), started, () -> { submissions.accept(request(), null); return 1; })));
                            try {
                                assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
                                var threads = connections.stream().map(c -> thread(observer, c.get())).toList();
                                int edges = 0;
                                long begin = System.nanoTime();
                                do {
                                    edges = Math.max(edges, observer.queryForObject("SELECT COUNT(*) FROM performance_schema.data_lock_waits WHERE BLOCKING_THREAD_ID=? AND REQUESTING_THREAD_ID IN (?,?,?)", Integer.class, holder, threads.get(0), threads.get(1), threads.get(2)));
                                    if (futures.stream().allMatch(Future::isDone)) break;
                                    TimeUnit.MILLISECONDS.sleep(50);
                                } while (millis(begin) < 1000);
                                result.put("maxObservedWaitEdgesToHolder", edges);
                                result.put("observationMillis", millis(begin));
                                result.put("completedWhileHeld", futures.stream().map(Future::isDone).toList());
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("Probe interrupted");
                            } finally { status.setRollbackOnly(); }
                        });
                        var outcomes = new ArrayList<Map<String, Object>>();
                        for (var future : futures) outcomes.add(future.get(8, TimeUnit.SECONDS));
                        result.put("probeOrder", List.of("realtime.publish", "realtime.claim", "realtime.admission"));
                        result.put("outcomes", outcomes);
                        if (!legacy) assertThat(outcomes).allMatch(row -> Integer.valueOf(1).equals(row.get("observedRowsOrPresence")));
                        @SuppressWarnings("unchecked")
                        var locks = (List<Map<String, Object>>) result.get("holderLocks");
                        long primaryRecords = locks.stream().filter(row -> "PRIMARY".equals(row.get("INDEX_NAME"))
                                        && "RECORD".equals(row.get("LOCK_TYPE")))
                                .mapToLong(row -> ((Number) row.get("record_count")).longValue()).sum();
                        long supremumRecords = locks.stream().filter(row -> "PRIMARY".equals(row.get("INDEX_NAME"))
                                        && "RECORD".equals(row.get("LOCK_TYPE")))
                                .mapToLong(row -> ((Number) row.get("supremum_count")).longValue()).sum();
                        long unknownRecords = locks.stream().filter(row -> "PRIMARY".equals(row.get("INDEX_NAME"))
                                        && "RECORD".equals(row.get("LOCK_TYPE")))
                                .mapToLong(row -> ((Number) row.get("unknown_data_count")).longValue()).sum();
                        result.put("primaryRecordLocks", primaryRecords);
                        result.put("primarySupremumLocks", supremumRecords);
                        result.put("primaryRealRecordLocks", primaryRecords - supremumRecords);
                        if (!legacy) {
                            assertThat(unknownRecords).as("must classify every primary lock without exporting keys").isZero();
                            assertThat(primaryRecords - supremumRecords).as("publication must not lock unrelated primary records").isBetween(1L, (long) size);
                        }
                        System.out.printf("OUTBOX_PUBLISH_PROBE batch=%d completedWhileHeld=%s maxWaitEdges=%s%n", size, result.get("completedWhileHeld"), result.get("maxObservedWaitEdgesToHolder"));
                    } finally {
                        executor.shutdownNow();
                        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                    }
                }
                var after = snapshot(jdbc);
                assertThat(after).isEqualTo(before);
                report.put("finalData", after);
                report.put("result", "PASS");
            }
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            report.put("finishedAtUtc", Instant.now().toString());
            Path path = Path.of("build/reports/outbox-publish-probe/result.json");
            Files.createDirectories(path.getParent());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(path.toFile(), report);
        }
    }

    // Exact previous production predicate, retained only as a SQL diagnostic control.
    private static int legacyPublish(JdbcTemplate jdbc, List<OutboxStore.Claim> claims) {
        var parameters = new ArrayList<Object>();
        var predicates = new ArrayList<String>();
        for (var claim : claims) {
            predicates.add("(event_id = ? AND lease_token = ?)");
            parameters.add(claim.eventId()); parameters.add(claim.token());
        }
        return jdbc.update("UPDATE outbox_event SET status = 'PUBLISHED', published_at = UTC_TIMESTAMP(6), lease_token = NULL "
                + "WHERE status = 'PROCESSING' AND (" + String.join(" OR ", predicates) + ")", parameters.toArray());
    }

    private static class PublishingJdbc extends JdbcTemplate {
        boolean capture;
        JdbcTemplate explaining;
        final List<List<Map<String, Object>>> plans = new ArrayList<>();
        PublishingJdbc(HikariDataSource source) { super(source); setQueryTimeout(5); }
        @Override public int update(String sql, Object... args) {
            if (capture) plans.add(explaining.queryForList("EXPLAIN " + sql, args));
            return super.update(sql, args);
        }
    }
    private static Map<String, Object> probe(TransactionTemplate tx, JdbcTemplate jdbc, AtomicLong connection,
            CountDownLatch started, java.util.function.IntSupplier work) {
        long begin = System.nanoTime();
        int count = tx.execute(status -> {
            connection.set(jdbc.queryForObject("SELECT CONNECTION_ID()", Long.class));
            started.countDown();
            try { return work.getAsInt(); } finally { status.setRollbackOnly(); }
        });
        return Map.of("result", "ROLLED_BACK", "observedRowsOrPresence", count, "elapsedMillis", millis(begin));
    }
    private static TransactionTemplate tx(HikariDataSource source, int isolation) {
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        tx.setIsolationLevel(isolation);
        return tx;
    }
    private static JdbcTemplate jdbc(HikariDataSource source) { var jdbc = new JdbcTemplate(source); jdbc.setQueryTimeout(5); return jdbc; }
    private static long thread(JdbcTemplate observer, long connection) { return observer.queryForObject("SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID=?", Long.class, connection); }
    private static List<Map<String, Object>> snapshot(JdbcTemplate jdbc) { return jdbc.queryForList("SELECT traffic_class,status,COUNT(*) AS row_count FROM outbox_event GROUP BY traffic_class,status ORDER BY traffic_class,status"); }
    private static NotificationRequest request() { return new NotificationRequest("synthetic-probe", "diagnostic", "synthetic body"); }
    private static double millis(long begin) { return (System.nanoTime() - begin) / 1_000_000.0; }
}
