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
import java.util.concurrent.atomic.AtomicBoolean;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.captureCandidates;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.claimIsolation;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.createSeedTables;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.handlers;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.mysql;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.pool;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.seed;
import static dev.jinhyeon.notificationlab.outbox.OutboxQueryProbeTest.traffic;
import static org.assertj.core.api.Assertions.assertThat;

/** Test-only held transactions and index controls; never a provider-delivery experiment. */
@Tag("outbox-lock-probe")
class OutboxLockProbeTest {
    private final Map<String, Object> report = new LinkedHashMap<>();

    @Test
    void observesNaturalPlansAndSeparatesCrossLaneRecordWaitsFromNewAdmissions() throws Exception {
        report.put("startedAtUtc", Instant.now().toString());
        report.put("diagnosticsVersion", "production-select-and-claim-locks-v4");
        report.put("scope", "SQL-only automatic statistics ON; current production SELECT/isolation and diagnostic index controls; no HTTP/Kafka/Worker");
        report.put("conditions", Map.of("mysql", "8.4.8", "cpu", 2, "memoryMiB", 2048,
                "bufferPoolMiB", 512, "applicationPoolMax", 4, "writerPoolMax", 1,
                "observerPoolMax", 1, "naturalObservationSeconds", 20, "heldObservationMillis", 1000));
        report.put("bounds", Map.of("queryTimeoutSeconds", 10, "observerQueryTimeoutSeconds", 5,
                "writerLockTimeoutSeconds", 3, "startedLatchSeconds", 3, "futureSeconds", 8,
                "executorShutdownSeconds", 5, "junitMinutes", 8, "taskMinutes", 10));
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("limitations", List.of("one isolated 100000 campaign + 100 realtime dataset; SQL UUID() seeding",
                "held realtime SELECT extends production timing without its lease UPDATE; concurrent campaign claim updates then rolls back",
                "natural optimizer statistics may change at any time; hints are diagnostic controls only",
                "record-lock counts are index records, not unique business rows; waits are transient snapshots",
                "observed SQL waits do not reproduce the previous HTTP response timeout"));
        try (var mysql = mysql()) {
            mysql.start();
            Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()).load().migrate();
            try (var source = pool(mysql); var writerSource = pool(mysql); var observerSource = pool(mysql)) {
                writerSource.setMaximumPoolSize(1);
                writerSource.setConnectionInitSql("SET SESSION innodb_lock_wait_timeout=3");
                observerSource.setMaximumPoolSize(1);
                observerSource.setUsername("root"); // Only the isolated container's generated credential.
                var jdbc = jdbc(source, 10);
                var writer = jdbc(writerSource, 10);
                var observer = jdbc(observerSource, 5);
                var tx = new TransactionTemplate(new JdbcTransactionManager(source));
                var claimTx = new TransactionTemplate(new JdbcTransactionManager(source));
                claimTx.setIsolationLevel(claimIsolation());
                report.put("declaredClaimIsolation", claimIsolation());
                var writerTx = new TransactionTemplate(new JdbcTransactionManager(writerSource));
                createSeedTables(jdbc);
                report.put("statisticsSettings", jdbc.queryForList("SHOW VARIABLES WHERE Variable_name IN ('innodb_stats_auto_recalc','innodb_stats_persistent')"));
                assertThat(jdbc.queryForObject("SELECT @@innodb_stats_auto_recalc", Integer.class)).isEqualTo(1);
                assertThat(jdbc.queryForObject("SELECT @@innodb_stats_persistent", Integer.class)).isEqualTo(1);
                String options = jdbc.queryForObject("SELECT CREATE_OPTIONS FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='outbox_event'", String.class);
                report.put("tableCreateOptions", options);
                assertThat(options).doesNotContain("stats_auto_recalc=0");
                String isolation = jdbc.queryForObject("SELECT @@transaction_isolation", String.class);
                report.put("serverDefaultIsolation", isolation);
                assertThat(isolation).isEqualTo("REPEATABLE-READ");
                for (int base = 0; base < 100_000; base += 1000) {
                    int start = base;
                    tx.executeWithoutResult(status -> seed(jdbc, start));
                }
                var submissions = new SubmissionService(new JdbcNotificationStore(jdbc), jdbc, Clock.systemUTC(),
                        new DeliveryTaskStore(jdbc, new DeliveryProperties(3, 1000, 60000)), traffic(TrafficProperties.Lane.REALTIME));
                for (int i = 0; i < 100; i++) tx.executeWithoutResult(status -> submissions.accept(request(), null));
                assertData(jdbc);
                var initialData = snapshot(jdbc);
                report.put("initialData", initialData);
                // Keep synthetic identifiers in memory only. Never export lock data, thread/connection IDs or SQL text with IDs.
                String campaignEvent = jdbc.queryForObject("SELECT eid FROM diagnostic_seed WHERE ordinal=1", String.class);
                var original = captureCandidates(TrafficProperties.Lane.REALTIME).get("PENDING");
                var cases = new ArrayList<Map<String, Object>>();
                report.put("cases", cases);
                long loaded = System.nanoTime();
                lockCase(cases, "CURRENT_IMMEDIATE", original, jdbc, claimTx, tx, writer, writerTx, observer, submissions, campaignEvent);
                var timeline = new ArrayList<Map<String, Object>>();
                report.put("planTimeline", timeline);
                for (int i = 0; i < 10; i++) {
                    TimeUnit.SECONDS.sleep(2);
                    timeline.add(Map.of("secondsAfterLoad", millis(loaded) / 1000,
                            "explain", jdbc.queryForList("EXPLAIN " + original.sql(), original.args())));
                }
                lockCase(cases, "CURRENT_AFTER_WINDOW", original, jdbc, claimTx, tx, writer, writerTx, observer, submissions, campaignEvent);
                for (String index : List.of("idx_outbox_status_available", "idx_outbox_traffic_status_available")) {
                    var controlled = new OutboxQueryProbeTest.SqlQuery(original.sql()
                            .replace(" FORCE INDEX (idx_outbox_traffic_status_available)", "").replace("FROM outbox_event",
                            "FROM outbox_event FORCE INDEX (" + index + ")"), original.args());
                    lockCase(cases, "CONTROL_" + index, controlled, jdbc, claimTx, tx, writer, writerTx, observer, submissions, campaignEvent);
                }
                assertData(jdbc);
                var finalData = snapshot(jdbc);
                assertThat(finalData).isEqualTo(initialData);
                report.put("finalData", finalData);
                report.put("result", "PASS");
            }
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            report.put("finishedAtUtc", Instant.now().toString());
            Path output = Path.of("build/reports/outbox-lock-probe/result.json");
            Files.createDirectories(output.getParent());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
        }
    }

    private static void lockCase(List<Map<String, Object>> cases, String name, OutboxQueryProbeTest.SqlQuery query,
            JdbcTemplate jdbc, TransactionTemplate tx, TransactionTemplate admissionTx, JdbcTemplate writer, TransactionTemplate writerTx,
            JdbcTemplate observer, SubmissionService submissions, String campaignEvent) throws Exception {
        var result = new LinkedHashMap<String, Object>();
        result.put("case", name);
        cases.add(result); // Preserve partial observations if a later probe fails.
        var started = new CountDownLatch(3);
        var writerConnection = new AtomicLong();
        var admissionConnection = new AtomicLong();
        var campaignConnection = new AtomicLong();
        var campaignFound = new AtomicBoolean();
        var holderThread = new AtomicLong();
        var executor = new ThreadPoolExecutor(3, 3, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(3),
                Thread.ofPlatform().daemon(true).factory());
        var futures = new ArrayList<Future<Map<String, Object>>>();
        try {
            tx.executeWithoutResult(status -> {
                long connection = jdbc.queryForObject("SELECT CONNECTION_ID()", Long.class);
                holderThread.set(thread(observer, connection));
                result.put("holderIsolation", jdbc.queryForObject("SELECT @@transaction_isolation", String.class));
                result.put("explainBeforeSelect", jdbc.queryForList("EXPLAIN " + query.sql(), query.args()));
                var before = handlers(jdbc);
                long selectStart = System.nanoTime();
                int returned = jdbc.queryForList(query.sql(), query.args()).size();
                result.put("selectMillis", millis(selectStart));
                assertThat(returned).isEqualTo(1);
                result.put("returnedRows", returned);
                var after = handlers(jdbc);
                var delta = new LinkedHashMap<String, Long>();
                before.forEach((counter, count) -> delta.put(counter, after.get(counter) - count));
                result.put("sessionHandlerDelta", delta);
                var locks = observer.queryForList("""
                        SELECT INDEX_NAME,LOCK_TYPE,LOCK_MODE,LOCK_STATUS,COUNT(*) AS record_count
                        FROM performance_schema.data_locks WHERE THREAD_ID=? AND OBJECT_SCHEMA='notification_lab'
                            AND OBJECT_NAME='outbox_event'
                        GROUP BY INDEX_NAME,LOCK_TYPE,LOCK_MODE,LOCK_STATUS ORDER BY INDEX_NAME,LOCK_MODE
                        """, holderThread.get());
                result.put("holderLocks", locks);
                assertThat(locks).anyMatch(row -> "RECORD".equals(row.get("LOCK_TYPE")));
                futures.add(executor.submit(() -> rollbackProbe(writerTx, writer, writerConnection, started, () ->
                        writer.update("UPDATE outbox_event SET available_at=available_at WHERE event_id=?", campaignEvent))));
                futures.add(executor.submit(() -> rollbackProbe(admissionTx, jdbc, admissionConnection, started, () ->
                        submissions.accept(request(), null))));
                var campaignStore = new OutboxStore(jdbc, traffic(TrafficProperties.Lane.CAMPAIGN));
                futures.add(executor.submit(() -> rollbackProbe(tx, jdbc, campaignConnection, started, () ->
                        campaignFound.set(campaignStore.claim(10).isPresent()))));
                try {
                    assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
                    long writerThread = thread(observer, writerConnection.get());
                    long admissionThread = thread(observer, admissionConnection.get());
                    long campaignThread = thread(observer, campaignConnection.get());
                    boolean writerWait = false;
                    boolean admissionWait = false;
                    boolean claimWait = false;
                    var claimWaitModes = new ArrayList<Map<String, Object>>();
                    long start = System.nanoTime();
                    do {
                        writerWait |= waiting(observer, writerThread, holderThread.get());
                        admissionWait |= waiting(observer, admissionThread, holderThread.get());
                        if (waiting(observer, campaignThread, holderThread.get())) {
                            claimWait = true;
                            if (claimWaitModes.isEmpty()) claimWaitModes.addAll(observer.queryForList("""
                                    SELECT d.INDEX_NAME,d.LOCK_TYPE,d.LOCK_MODE,COUNT(*) AS record_count
                                    FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks d
                                      ON d.ENGINE=w.ENGINE AND d.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID
                                    WHERE w.REQUESTING_THREAD_ID=? AND w.BLOCKING_THREAD_ID=?
                                    GROUP BY d.INDEX_NAME,d.LOCK_TYPE,d.LOCK_MODE
                                    """, campaignThread, holderThread.get()));
                        }
                        if (futures.stream().allMatch(Future::isDone)) break;
                        TimeUnit.MILLISECONDS.sleep(50);
                    } while (millis(start) < 1000);
                    result.put("observationMillis", millis(start));
                    result.put("campaignUpdateWaitObserved", writerWait);
                    result.put("newRealtimeAdmissionWaitObserved", admissionWait);
                    result.put("campaignClaimWaitObserved", claimWait);
                    result.put("campaignClaimWaitModes", claimWaitModes);
                    result.put("campaignUpdateCompletedWhileHeld", futures.get(0).isDone());
                    result.put("newRealtimeAdmissionCompletedWhileHeld", futures.get(1).isDone());
                    result.put("campaignClaimCompletedWhileHeld", futures.get(2).isDone());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Diagnostic interrupted");
                } finally { status.setRollbackOnly(); }
            });
            result.put("campaignUpdate", futures.get(0).get(8, TimeUnit.SECONDS));
            result.put("newRealtimeAdmission", futures.get(1).get(8, TimeUnit.SECONDS));
            result.put("campaignClaim", futures.get(2).get(8, TimeUnit.SECONDS));
            result.put("campaignClaimFound", campaignFound.get());
            assertThat(observer.queryForObject("SELECT COUNT(*) FROM performance_schema.data_locks WHERE THREAD_ID=? AND OBJECT_SCHEMA='notification_lab' AND OBJECT_NAME='outbox_event'", Integer.class, holderThread.get())).isZero();
            System.out.printf("OUTBOX_LOCK_PROBE case=%s campaignWait=%s admissionWait=%s%n", name,
                    result.get("campaignUpdateWaitObserved"), result.get("newRealtimeAdmissionWaitObserved"));
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static Map<String, Object> rollbackProbe(TransactionTemplate tx, JdbcTemplate jdbc,
            AtomicLong connection, CountDownLatch started, Runnable work) {
        long start = System.nanoTime();
        tx.executeWithoutResult(status -> {
            connection.set(jdbc.queryForObject("SELECT CONNECTION_ID()", Long.class));
            started.countDown();
            try { work.run(); } finally { status.setRollbackOnly(); }
        });
        return Map.of("result", "ROLLED_BACK", "elapsedMillis", millis(start));
    }

    private static boolean waiting(JdbcTemplate observer, long requesting, long blocking) {
        return observer.queryForObject("SELECT COUNT(*) FROM performance_schema.data_lock_waits WHERE REQUESTING_THREAD_ID=? AND BLOCKING_THREAD_ID=?", Integer.class, requesting, blocking) > 0;
    }

    private static long thread(JdbcTemplate observer, long connection) {
        return observer.queryForObject("SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID=?", Long.class, connection);
    }

    private static void assertData(JdbcTemplate jdbc) {
        for (String table : List.of("notification", "delivery_task", "outbox_event")) {
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class)).isEqualTo(100_100);
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PENDING' AND attempt_count=0 AND lease_token IS NULL", Integer.class)).isEqualTo(100_100);
    }

    private static List<Map<String, Object>> snapshot(JdbcTemplate jdbc) {
        return jdbc.queryForList("SELECT traffic_class,status,COUNT(*) AS row_count FROM outbox_event GROUP BY traffic_class,status ORDER BY traffic_class,status");
    }

    private static NotificationRequest request() { return new NotificationRequest("synthetic-probe", "diagnostic", "synthetic body"); }
    private static double millis(long start) { return (System.nanoTime() - start) / 1_000_000.0; }
    private static JdbcTemplate jdbc(HikariDataSource source, int seconds) {
        var jdbc = new JdbcTemplate(source); jdbc.setQueryTimeout(seconds); return jdbc;
    }
}
