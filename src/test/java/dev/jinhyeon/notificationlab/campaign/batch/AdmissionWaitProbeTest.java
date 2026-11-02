package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariDataSource;
import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import dev.jinhyeon.notificationlab.config.TrafficProperties;
import dev.jinhyeon.notificationlab.notification.JdbcNotificationStore;
import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import dev.jinhyeon.notificationlab.outbox.SubmissionService;
import dev.jinhyeon.notificationlab.worker.DeliveryTaskStore;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.MySQLContainer;

import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Part;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Three bounded controls around real transactional production beans; no HTTP/Kafka/provider load. */
@Tag("api-wait-probe")
class AdmissionWaitProbeTest {
    private static final int HOLD_MILLIS = 200;
    private final Map<String, Object> report = new LinkedHashMap<>();

    @Test
    @Timeout(120)
    void distinguishesDatabaseInsertWaitFromCallerDelayAfterSuccessfulNativeCommit() throws Exception {
        var cpu = ManagementFactory.getThreadMXBean();
        assertThat(cpu.isCurrentThreadCpuTimeSupported()).isTrue();
        boolean cpuWasEnabled = cpu.isThreadCpuTimeEnabled();
        if (!cpuWasEnabled) cpu.setThreadCpuTimeEnabled(true);
        report.put("startedAtUtc", Instant.now().toString());
        report.put("scope", "SQL-only real Spring transactional SubmissionService; normal, held gap lock, synthetic post-commit caller pause; no HTTP/Kafka/provider");
        report.put("conditions", Map.of("mysql", "8.4.8", "mysqlCpu", 2, "mysqlMemoryMiB", 2048,
                "mysqlBufferPoolMiB", 512, "applicationPoolMax", 4, "externalConnectionsMax", 2,
                "warmupSubmissions", 3, "cases", 3, "holdMillis", HOLD_MILLIS));
        report.put("bounds", Map.of("lockWaitSeconds", 5, "queryTimeoutSeconds", 5, "observerQuerySeconds", 2,
                "observationSeconds", 2, "gateSeconds", 3, "futureSeconds", 8, "executorQueue", 1));
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("limitations", List.of("synthetic controls calibrate evidence, not the cause of previous 100k/million latency",
                "wall minus thread CPU includes all off-CPU intervals, not pure scheduler or database time",
                "post-commit latch is not a physical network fault or an actual OS scheduling event",
                "wait edges are transient snapshots; none observed does not prove absence throughout execution"));
        var cases = new ArrayList<Map<String, Object>>();
        report.put("cases", cases);
        try (var mysql = mysql()) {
            mysql.start();
            Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()).load().migrate();
            try (var pool = pool(mysql);
                 var observerConnection = DriverManager.getConnection(mysql.getJdbcUrl(), "root", mysql.getPassword());
                 var blocker = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
                var observer = new JdbcTemplate(new SingleConnectionDataSource(observerConnection, true));
                observer.setQueryTimeout(2);
                var nativeGate = new AtomicReference<CommitGate>();
                var capture = new ThreadLocal<Capture>();
                var timing = new DeliveryTiming((part, nanos, failed) -> {
                    var active = capture.get();
                    if (active == null) throw new IllegalStateException("Missing SQL calibration capture");
                    active.record(part, nanos, failed);
                });
                try (var context = context(timing.dataSource(new CommitDelayDataSource(pool, nativeGate)))) {
                    var submissions = context.getBean(SubmissionService.class);
                    for (int i = 0; i < 3; i++) submissions.accept(request(), null);
                    assertRows(observer, 3);
                    assertThat(waitEdges(observer)).isZero();
                    var executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
                            Thread.ofPlatform().daemon(true).factory());
                    try {
                        for (String scenario : List.of("BASELINE", "OUTBOX_GAP_LOCK", "POST_COMMIT_CALLER_PAUSE")) {
                            var row = new LinkedHashMap<String, Object>(); row.put("scenario", scenario); cases.add(row);
                            int before = count(observer, "notification");
                            var gate = new CommitGate();
                            try {
                                if (scenario.equals("OUTBOX_GAP_LOCK")) {
                                    blocker.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                                    blocker.setAutoCommit(false);
                                    try (var statement = blocker.prepareStatement("SELECT event_id FROM outbox_event FORCE INDEX (PRIMARY) WHERE event_id >= '' FOR UPDATE")) {
                                        statement.setQueryTimeout(5);
                                        try (var result = statement.executeQuery()) { while (result.next()) { /* No identifiers retained. */ } }
                                    }
                                }
                                if (scenario.equals("POST_COMMIT_CALLER_PAUSE")) nativeGate.set(gate);
                                var future = executor.submit(() -> {
                                    var measured = new Capture(); capture.set(measured);
                                    long cpuStart = cpu.getCurrentThreadCpuTime(); long start = System.nanoTime();
                                    try {
                                        timing.scope(DeliveryTiming.Phase.API_SUBMIT, () -> submissions.accept(request(), null));
                                        measured.wallMillis = (System.nanoTime() - start) / 1_000_000.0;
                                        measured.cpuMillis = (cpu.getCurrentThreadCpuTime() - cpuStart) / 1_000_000.0;
                                        return measured;
                                    } finally { capture.remove(); }
                                });
                                if (scenario.equals("OUTBOX_GAP_LOCK")) {
                                    await().pollInterval(Duration.ofMillis(25)).atMost(Duration.ofSeconds(2))
                                            .until(() -> outboxInsertWaits(observer) > 0);
                                    row.put("observedOutboxWaitEdges", outboxInsertWaits(observer));
                                    assertThat(future.isDone()).isFalse(); assertRows(observer, before);
                                    row.put("uncommittedRowsInvisible", true);
                                    TimeUnit.MILLISECONDS.sleep(HOLD_MILLIS);
                                    blocker.rollback(); blocker.setAutoCommit(true);
                                } else if (scenario.equals("POST_COMMIT_CALLER_PAUSE")) {
                                    assertThat(gate.nativeReturned.await(3, TimeUnit.SECONDS)).isTrue();
                                    assertThat(future.isDone()).isFalse(); assertRows(observer, before + 1);
                                    row.put("nativeCommitReturned", true); row.put("rowsVisibleWhileCallerBlocked", true);
                                    row.put("waitEdgesWhileCallerBlocked", waitEdges(observer));
                                    assertThat(waitEdges(observer)).isZero();
                                    TimeUnit.MILLISECONDS.sleep(HOLD_MILLIS); gate.release.countDown();
                                }
                                var measured = future.get(8, TimeUnit.SECONDS);
                                measured.verify(); row.putAll(measured.summary());
                                if (!scenario.equals("BASELINE")) assertThat(measured.wallMillis - measured.cpuMillis).isGreaterThan(100);
                                if (scenario.equals("OUTBOX_GAP_LOCK")) assertThat(measured.executions[3] / 1_000_000.0).isGreaterThan(150);
                                if (scenario.equals("POST_COMMIT_CALLER_PAUSE")) assertThat(measured.nanos[Part.COMMIT.ordinal()] / 1_000_000.0).isGreaterThan(150);
                                assertRows(observer, before + 1); assertThat(waitEdges(observer)).isZero();
                                row.put("result", "PASS");
                            } finally {
                                gate.release.countDown(); nativeGate.set(null);
                                if (!blocker.getAutoCommit()) { blocker.rollback(); blocker.setAutoCommit(true); }
                            }
                        }
                    } finally {
                        executor.shutdownNow(); assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
                    }
                    assertRows(observer, 6);
                    assertThat(count(observer, "delivery_attempt")).isZero();
                    report.put("acceptedSubmissions", 6); report.put("providerCalls", 0);
                }
            }
            report.put("result", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName()); throw failure;
        } finally {
            if (!cpuWasEnabled) cpu.setThreadCpuTimeEnabled(false);
            report.put("finishedAtUtc", Instant.now().toString());
            Path output = Path.of("build/reports/api-wait-probe/result.json"); Files.createDirectories(output.getParent());
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
        }
    }

    private static AnnotationConfigApplicationContext context(DataSource source) {
        var context = new AnnotationConfigApplicationContext();
        context.register(TransactionConfiguration.class, SubmissionService.class, JdbcNotificationStore.class, DeliveryTaskStore.class);
        context.registerBean(DataSource.class, () -> source);
        context.registerBean(JdbcTemplate.class, () -> { var jdbc = new JdbcTemplate(source); jdbc.setQueryTimeout(5); return jdbc; });
        context.registerBean("transactionManager", JdbcTransactionManager.class, () -> new JdbcTransactionManager(source));
        context.registerBean(Clock.class, Clock::systemUTC);
        context.registerBean(DeliveryProperties.class, () -> new DeliveryProperties(3, 1000, 60000));
        context.registerBean(TrafficProperties.class, () -> new TrafficProperties(TrafficProperties.Lane.REALTIME, false, 200, 20));
        context.refresh(); return context;
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableTransactionManagement(proxyTargetClass = true)
    static class TransactionConfiguration { }

    private static final class Capture {
        final long[] nanos = new long[Part.values().length];
        final long[] counts = new long[nanos.length];
        final long[] executions = new long[4];
        boolean failed;
        double wallMillis;
        double cpuMillis;
        void record(Part part, long elapsed, boolean failure) {
            if (part == Part.EXECUTE && counts[part.ordinal()] < 4) executions[(int) counts[part.ordinal()]] = elapsed;
            nanos[part.ordinal()] += elapsed; counts[part.ordinal()]++; failed |= failure;
        }
        void verify() {
            assertThat(failed).isFalse(); assertThat(cpuMillis).isGreaterThanOrEqualTo(0);
            for (var part : List.of(Part.STORE_CALL, Part.ACQUIRE, Part.COMMIT, Part.RELEASE)) assertThat(counts[part.ordinal()]).isEqualTo(1);
            assertThat(counts[Part.EXECUTE.ordinal()]).isEqualTo(4); assertThat(counts[Part.ROLLBACK.ordinal()]).isZero();
        }
        Map<String, Object> summary() {
            var parts = new LinkedHashMap<String, Object>();
            for (var part : Part.values()) parts.put(part.name(), Map.of("count", counts[part.ordinal()], "millis", nanos[part.ordinal()] / 1_000_000.0));
            return Map.of("wallMillis", wallMillis, "threadCpuMillis", cpuMillis,
                    "wallMinusCpuMillis", wallMillis - cpuMillis, "jdbc", parts,
                    "sqlExecutionMillis", Arrays.stream(executions).mapToDouble(n -> n / 1_000_000.0).boxed().toList());
        }
    }

    private static final class CommitGate {
        final CountDownLatch nativeReturned = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
    }

    private static final class CommitDelayDataSource extends DelegatingDataSource {
        private final AtomicReference<CommitGate> gate;
        CommitDelayDataSource(DataSource target, AtomicReference<CommitGate> gate) { super(target); this.gate = gate; }
        @Override public Connection getConnection() throws SQLException { return connection(obtainTargetDataSource().getConnection()); }
        @Override public Connection getConnection(String username, String password) throws SQLException { return connection(obtainTargetDataSource().getConnection(username, password)); }
        private Connection connection(Connection target) {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                if (method.getDeclaringClass() == Object.class) {
                    return switch (method.getName()) {
                        case "equals" -> proxy == args[0];
                        case "hashCode" -> System.identityHashCode(proxy);
                        default -> "Commit calibration connection";
                    };
                }
                Object result;
                try { result = method.invoke(target, args); }
                catch (InvocationTargetException failure) { throw failure.getCause(); }
                if (method.getName().equals("commit")) {
                    var armed = gate.getAndSet(null);
                    if (armed != null) {
                        armed.nativeReturned.countDown();
                        try {
                            if (!armed.release.await(3, TimeUnit.SECONDS)) throw new SQLException("Calibration release timed out");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt(); throw new SQLException("Calibration interrupted");
                        }
                    }
                }
                return result;
            });
        }
    }

    private static int outboxInsertWaits(JdbcTemplate observer) {
        return observer.queryForObject("""
                SELECT COUNT(*) FROM performance_schema.data_lock_waits w
                JOIN performance_schema.data_locks d ON d.ENGINE=w.ENGINE AND d.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID
                WHERE d.OBJECT_SCHEMA='notification_lab' AND d.OBJECT_NAME='outbox_event' AND d.LOCK_STATUS='WAITING'
                """, Integer.class);
    }
    private static int waitEdges(JdbcTemplate observer) { return observer.queryForObject("SELECT COUNT(*) FROM performance_schema.data_lock_waits", Integer.class); }
    private static int count(JdbcTemplate observer, String table) { return observer.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class); }
    private static void assertRows(JdbcTemplate observer, int expected) {
        for (String table : List.of("notification", "delivery_task", "outbox_event")) assertThat(count(observer, table)).as(table).isEqualTo(expected);
    }
    private static NotificationRequest request() { return new NotificationRequest("synthetic", "SQL calibration", "synthetic"); }
    private static HikariDataSource pool(MySQLContainer<?> mysql) {
        var pool = new HikariDataSource(); pool.setJdbcUrl(mysql.getJdbcUrl()); pool.setUsername(mysql.getUsername()); pool.setPassword(mysql.getPassword());
        pool.setMaximumPoolSize(4); pool.setMinimumIdle(1); pool.setConnectionTimeout(2000); pool.setValidationTimeout(1000);
        pool.setConnectionInitSql("SET SESSION innodb_lock_wait_timeout=5"); return pool;
    }
    private static MySQLContainer<?> mysql() {
        return new MySQLContainer<>("mysql:8.4.8").withDatabaseName("notification_lab").withUsername("notification_test")
                .withPassword(UUID.randomUUID().toString()).withCommand("--default-time-zone=+00:00", "--innodb-buffer-pool-size=536870912")
                .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "8000")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(2048L * 1024 * 1024).withNanoCPUs(2_000_000_000L)
                        .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port ->
                                new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    }
}
