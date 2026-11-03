package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariDataSource;
import dev.jinhyeon.notificationlab.api.ApiExceptionHandler;
import dev.jinhyeon.notificationlab.api.NotificationController;
import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import dev.jinhyeon.notificationlab.config.TrafficProperties;
import dev.jinhyeon.notificationlab.notification.JdbcNotificationStore;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.outbox.IdempotentSubmissionService;
import dev.jinhyeon.notificationlab.outbox.SubmissionService;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import dev.jinhyeon.notificationlab.worker.DeliveryTaskStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.InetAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.apache.coyote.AbstractProtocol;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.filter.OncePerRequestFilter;
import org.testcontainers.containers.MySQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Admission-only production MVC and transaction path, with deterministic test JDBC pauses. No Kafka/provider delivery. */
@Tag("api-failure-persistence")
class ApiFailurePersistenceProbeTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private enum Point { BEFORE_COMMIT, AFTER_RESTORE }

    @Test
    @Timeout(180)
    void savesTheFirstHttpFailureBeforeTeardownAndKeepsItAfterLateRealDatabaseCompletion() throws Exception {
        Path directory = Files.createTempDirectory(Files.createDirectories(Path.of("build/reports/api-failure-persistence").toAbsolutePath()), "run-");
        var report = new LinkedHashMap<String, Object>(); report.put("startedAtUtc", Instant.now().toString());
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("scope", "controlled production HTTP/MySQL admission only; test pauses, not natural failure reproduction; no Kafka/relay/worker/provider");
        report.put("conditions", Map.of("mysql", "8.4.8", "mysqlCpu", 2, "mysqlMemoryMiB", 2048, "mysqlBufferPoolMiB", 512,
                "poolMax", 4, "serverThreadsMax", 4, "warmupRequestsPerCase", 40, "fixtureTimeoutMillis", 1000, "gateMaxSeconds", 10));
        var cases = new ArrayList<Map<String, Object>>(); report.put("cases", cases);
        try (var mysql = mysql()) {
            mysql.start();
            Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()).load().migrate();
            try (var connection = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
                var observer = new JdbcTemplate(new SingleConnectionDataSource(connection, true)); observer.setQueryTimeout(2);
                for (var point : Point.values()) {
                    var row = new LinkedHashMap<String, Object>(); row.put("point", point.name()); cases.add(row);
                    int before = count(observer, "notification");
                    var window = new ApiFailureWindow(100); var resources = new Resources(pool(mysql), window);
                    var gate = new Gate(point); var reportFile = directory.resolve(point.name() + ".json");
                    var store = new FailureSnapshotStore(window, reportFile);
                    try (var pool = resources.pool;
                         var app = new SpringApplicationBuilder(ProbeApplication.class).web(WebApplicationType.SERVLET)
                                 .initializers(context -> context.getBeanFactory().registerSingleton("probeResources", resources))
                                 .run("--spring.config.name=notification-failure-probe", "--server.address=127.0.0.1", "--server.port=0",
                                         "--server.tomcat.threads.max=4", "--server.tomcat.threads.min-spare=1", "--server.tomcat.accept-count=4",
                                         "--server.tomcat.max-connections=16", "--logging.level.root=WARN", "--spring.main.banner-mode=off");
                         var http = HttpClient.newHttpClient()) {
                        URI uri = URI.create("http://127.0.0.1:" + ((ServletWebServerApplicationContext) app).getWebServer().getPort() + "/notifications");
                        try {
                            var protocol = (AbstractProtocol<?>) ((TomcatWebServer) ((ServletWebServerApplicationContext) app).getWebServer()).getTomcat().getConnector().getProtocolHandler();
                            assertThat(protocol.getMaxThreads()).isEqualTo(4); assertThat(protocol.getMaxConnections()).isEqualTo(16);
                            row.put("actualServerThreadsMax", protocol.getMaxThreads()); row.put("actualServerConnectionsMax", protocol.getMaxConnections());
                            for (int i = 0; i < 40; i++) {
                                window.open(ApiFailureWindow.Window.BASELINE); long start = System.nanoTime();
                                var response = CampaignDeliveryDiagnostics.sendProbe(http, request(uri, 5000), row, "BASELINE", i + 1, i);
                                assertThat(response.statusCode()).isEqualTo(202);
                                window.complete(202, (System.nanoTime() - start) / 1_000_000.0);
                            }
                            assertRows(observer, before + 40);
                            resources.armed.set(gate); window.open(ApiFailureWindow.Window.MIXED);
                            assertThatThrownBy(() -> {
                                try { CampaignDeliveryDiagnostics.sendProbe(http, request(uri, 1000), row, "MIXED", 41, 40); }
                                catch (HttpTimeoutException timeout) { throw store.preserve(timeout, ApiFailureWindow.Stop.HTTP_TIMEOUT, row); }
                            }).isInstanceOf(HttpTimeoutException.class);
                            assertThat(gate.reached.getCount()).isZero();
                            assertThat(app.isActive()).isTrue(); assertThat(pool.isClosed()).isFalse();
                            assertThat(gate.finished.getCount()).isEqualTo(1); assertThat(gate.release.getCount()).isEqualTo(1);
                            assertThat(reportFile).exists(); byte[] first = Files.readAllBytes(reportFile);
                            var frozen = MAPPER.readTree(first);
                            assertThat(frozen.path("stopReason").asText()).isEqualTo("HTTP_TIMEOUT");
                            assertThat(frozen.path("activeServerOutcome").asText()).isEqualTo("UNKNOWN");
                            assertThat(frozen.path("recent").size()).isEqualTo(32);
                            assertThat(frozen.path("completed").asInt()).isEqualTo(40); assertThat(frozen.path("evicted").asInt()).isEqualTo(8);
                            assertThat(frozen.path("active").path("ordinal").asInt()).isEqualTo(41);
                            assertThat(frozen.path("active").path("responseObserved").asBoolean()).isFalse();
                            assertThat(frozen.path("active").path("activePart").asText()).isEqualTo(point == Point.BEFORE_COMMIT ? "COMMIT" : "AUTOCOMMIT_STATE");
                            assertThat(frozen.path("active").path("commitReturned").asBoolean()).isEqualTo(point == Point.AFTER_RESTORE);
                            int committed = before + 40 + (point == Point.AFTER_RESTORE ? 1 : 0); assertRows(observer, committed);
                            row.put("committedRowsWhileBlocked", committed); row.put("baselineRowsBeforeCase", before);
                            row.put("savedBeforeTeardown", true); row.put("savedWhileServerBlocked", true);
                            row.put("snapshotPublishedObservedAtUtc", Instant.now().toString());
                            row.put("snapshotFile", reportFile.getFileName().toString()); row.put("snapshotSha256", sha(first));
                            row.put("snapshotBytes", first.length); assertThat(first.length).isLessThanOrEqualTo(FailureSnapshotStore.MAX_BYTES);
                            gate.release.countDown();
                            assertThat(gate.finished.await(3, TimeUnit.SECONDS)).isTrue();
                            await().atMost(Duration.ofSeconds(3)).until(() -> count(observer, "notification") == before + 41);
                            assertRows(observer, before + 41);
                            assertThat(store.save(ApiFailureWindow.Stop.INFRA_FAILURE).bytes()).isEqualTo(first.length);
                            window.complete(202, 2000); assertThat(Files.readAllBytes(reportFile)).isEqualTo(first);
                            assertThat(resources.requests.get()).isEqualTo(41); assertThat(resources.providerCalls.get()).isZero();
                            assertThat(count(observer, "delivery_attempt")).isZero();
                            row.put("serverRequests", resources.requests.get()); row.put("providerCalls", 0);
                            row.put("committedRowsAfterRelease", before + 41); row.put("lateCompletionPreservedFile", true);
                            row.put("result", "EXPECTED_TIMEOUT_VERIFIED");
                        } finally { gate.release.countDown(); }
                    } finally { gate.release.countDown(); }
                    assertThat(resources.pool.isClosed()).isTrue();
                    assertThat(Files.exists(reportFile)).isTrue(); row.put("fileStillPresentAfterAppClose", true);
                }
                assertRows(observer, 82); assertThat(count(observer, "delivery_attempt")).isZero();
                assertThat(observer.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PENDING'", Integer.class)).isEqualTo(82);
                report.put("acceptedDatabaseSubmissions", 82); report.put("observedHttp202", 80); report.put("httpTimeouts", 2); report.put("providerCalls", 0);
            }
            report.put("result", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName()); throw failure;
        } finally {
            report.put("finishedAtUtc", Instant.now().toString());
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("result.json").toFile(), report);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    @ImportAutoConfiguration({ServletWebServerFactoryAutoConfiguration.class, DispatcherServletAutoConfiguration.class,
            WebMvcAutoConfiguration.class, JacksonAutoConfiguration.class, ValidationAutoConfiguration.class})
    @EnableTransactionManagement(proxyTargetClass = true)
    @Import({NotificationController.class, ApiExceptionHandler.class, NotificationService.class, SubmissionService.class,
            IdempotentSubmissionService.class, JdbcNotificationStore.class, DeliveryTaskStore.class})
    static class ProbeApplication {
        @Bean ServletWebServerFactory serverFactory() throws Exception {
            var factory = new TomcatServletWebServerFactory(0); factory.setAddress(InetAddress.getByName("127.0.0.1"));
            factory.addConnectorCustomizers(connector -> {
                var protocol = (AbstractProtocol<?>) connector.getProtocolHandler();
                protocol.setMaxThreads(4); protocol.setMinSpareThreads(1); protocol.setMaxConnections(16); protocol.setAcceptCount(4);
            });
            return factory;
        }
        @Bean DeliveryTiming timing(Resources resources) { return new DeliveryTiming(resources.window); }
        @Bean(destroyMethod = "close") DataSource dataSource(Resources resources, DeliveryTiming timing) {
            return timing.dataSource(new PausingDataSource(resources));
        }
        @Bean JdbcTemplate jdbc(DataSource source) { var jdbc = new JdbcTemplate(source); jdbc.setQueryTimeout(5); return jdbc; }
        @Bean JdbcTransactionManager transactionManager(DataSource source) { return new JdbcTransactionManager(source); }
        @Bean Clock clock() { return Clock.systemUTC(); }
        @Bean DeliveryProperties delivery() { return new DeliveryProperties(3, 1000, 60000); }
        @Bean TrafficProperties traffic() { return new TrafficProperties(TrafficProperties.Lane.REALTIME, false, 200, 20); }
        @Bean SimpleMeterRegistry registry() { return new SimpleMeterRegistry(); }
        @Bean PushProvider provider(Resources resources) { return request -> { resources.providerCalls.incrementAndGet(); throw new AssertionError("No provider delivery in admission control"); }; }
        @Bean SmartInitializingSingleton submissionTiming(DeliveryTiming timing, SubmissionService submission) { return () -> timing.attachSubmission(submission); }
        @Bean FilterRegistrationBean<OncePerRequestFilter> gateScope(Resources resources) {
            return new FilterRegistrationBean<>(new OncePerRequestFilter() {
                @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
                    var gate = resources.armed.get(); resources.current.set(gate); resources.requests.incrementAndGet();
                    try { chain.doFilter(request, response); }
                    finally { resources.current.remove(); if (gate != null) gate.finished.countDown(); }
                }
            });
        }
    }
    private static final class Resources {
        final HikariDataSource pool;
        final ApiFailureWindow window;
        final ThreadLocal<Gate> current = new ThreadLocal<>();
        final AtomicReference<Gate> armed = new AtomicReference<>();
        final AtomicInteger requests = new AtomicInteger();
        final AtomicInteger providerCalls = new AtomicInteger();
        Resources(HikariDataSource pool, ApiFailureWindow window) { this.pool = pool; this.window = window; }
    }
    private static final class Gate {
        final Point point;
        final AtomicBoolean claimed = new AtomicBoolean();
        final CountDownLatch reached = new CountDownLatch(1), release = new CountDownLatch(1), finished = new CountDownLatch(1);
        Gate(Point point) { this.point = point; }
        boolean matches(String name, Object[] args) {
            return point == Point.BEFORE_COMMIT ? name.equals("commit") : name.equals("setAutoCommit") && Boolean.TRUE.equals(args[0]);
        }
        void pause() throws SQLException {
            reached.countDown();
            try { if (!release.await(10, TimeUnit.SECONDS)) throw new SQLException("Control release timed out"); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new SQLException("Control interrupted"); }
        }
    }
    private static final class PausingDataSource extends DelegatingDataSource implements AutoCloseable {
        final Resources resources;
        PausingDataSource(Resources resources) { super(resources.pool); this.resources = resources; }
        @Override public Connection getConnection() throws SQLException { return wrap(obtainTargetDataSource().getConnection()); }
        @Override public Connection getConnection(String username, String password) throws SQLException { return wrap(obtainTargetDataSource().getConnection(username, password)); }
        Connection wrap(Connection target) {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                if (method.getDeclaringClass() == Object.class) return switch (method.getName()) {
                    case "equals" -> proxy == args[0]; case "hashCode" -> System.identityHashCode(proxy); default -> "Controlled admission connection";
                };
                var gate = resources.current.get(); boolean armed = gate != null && gate.matches(method.getName(), args) && gate.claimed.compareAndSet(false, true);
                if (armed && gate.point == Point.BEFORE_COMMIT) gate.pause();
                Object result;
                try { result = method.invoke(target, args); } catch (InvocationTargetException failure) { throw failure.getCause(); }
                if (armed && gate.point == Point.AFTER_RESTORE) gate.pause();
                return result;
            });
        }
        @Override public void close() { resources.pool.close(); }
    }
    private static HttpRequest request(URI uri, int timeoutMillis) {
        return HttpRequest.newBuilder(uri).timeout(Duration.ofMillis(timeoutMillis)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"recipientId\":\"synthetic\",\"title\":\"control\",\"body\":\"synthetic\"}")).build();
    }
    private static int count(JdbcTemplate jdbc, String table) { return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class); }
    private static void assertRows(JdbcTemplate jdbc, int expected) {
        for (String table : List.of("notification", "delivery_task", "outbox_event")) assertThat(count(jdbc, table)).as(table).isEqualTo(expected);
    }
    private static String sha(byte[] bytes) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
    private static HikariDataSource pool(MySQLContainer<?> mysql) {
        var pool = new HikariDataSource(); pool.setJdbcUrl(mysql.getJdbcUrl()); pool.setUsername(mysql.getUsername()); pool.setPassword(mysql.getPassword());
        pool.setMaximumPoolSize(4); pool.setMinimumIdle(1); pool.setConnectionTimeout(2000); pool.setValidationTimeout(1000);
        pool.setConnectionInitSql("SET SESSION innodb_lock_wait_timeout=5"); return pool;
    }
    private static MySQLContainer<?> mysql() {
        return new MySQLContainer<>("mysql:8.4.8").withDatabaseName("notification_lab").withUsername("notification_test").withPassword(UUID.randomUUID().toString())
                .withCommand("--default-time-zone=+00:00", "--innodb-buffer-pool-size=536870912")
                .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
                .withUrlParam("connectTimeout", "2000").withUrlParam("socketTimeout", "8000")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(2048L * 1024 * 1024).withNanoCPUs(2_000_000_000L)
                        .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port -> new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    }
}
