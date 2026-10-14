package dev.jinhyeon.notificationlab.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariDataSource;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.config.MockProperties;
import dev.jinhyeon.notificationlab.provider.MockPushProvider;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import dev.jinhyeon.notificationlab.provider.PushRequest;
import dev.jinhyeon.notificationlab.provider.PushResult;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Mandatory real-MySQL tests: Docker absence must fail, not silently skip this suite. */
@Testcontainers
class MySqlNotificationTest {
    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4.8")
            .withDatabaseName("notification_lab")
            .withUsername("notification_test")
            .withPassword(UUID.randomUUID().toString())
            .withCommand("--default-time-zone=+00:00")
            .withUrlParam("connectionTimeZone", "UTC")
            .withUrlParam("forceConnectionTimeZoneToSession", "true")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), new ExposedPort(3306))));

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private static final String PAYLOAD = """
            {"recipientId":"test-user","title":"테스트 🌱","body":"재시작 후 조회"}
            """;
    private static ServletWebServerApplicationContext app;

    @BeforeAll
    static void startApplication() {
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(
                NotificationLabApplication.class, ProviderProbeConfiguration.class).run(
                "--server.port=0", "--spring.profiles.active=sync", "--app.mock.delay-ms=0",
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword());
    }

    @AfterAll
    static void stopApplication() {
        if (app != null) app.close();
        HTTP.close();
    }

    @BeforeEach
    void clearRecords() {
        jdbc().update("DELETE FROM dead_letter_redrive");
        jdbc().update("DELETE FROM delivery_attempt");
        jdbc().update("DELETE FROM delivery_task");
        jdbc().update("DELETE FROM outbox_event");
        jdbc().update("DELETE FROM notification");
        probe().reset();
    }

    @Test
    void resultSurvivesApplicationRestartAndFlywayDoesNotReapplyMigration() throws Exception {
        HttpResponse<String> created = request("POST", "/notifications", PAYLOAD);
        assertThat(created.statusCode()).isEqualTo(201);
        JsonNode before = json(created);
        assertThat(before.path("status").asText()).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(before.path("title").asText()).isEqualTo("테스트 🌱");
        String location = created.headers().firstValue("Location").orElseThrow();
        assertThat(json(request("GET", location, null))).isEqualTo(before);

        app.close();
        startApplication();

        HttpResponse<String> after = request("GET", location, null);
        assertThat(after.statusCode()).isEqualTo(200);
        assertThat(json(after)).isEqualTo(before);
        assertThat(app.getBean(Flyway.class).info().applied()).hasSize(6);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event", Integer.class)).isZero();
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void laterMigrationsPreserveExistingVersionOneResults() throws Exception {
        String schema = "notification_upgrade_test";
        executeAsRoot("CREATE DATABASE " + schema);
        String upgradeUrl = MYSQL.getJdbcUrl().replace("/notification_lab", "/" + schema);
        try {
            Flyway.configure().dataSource(upgradeUrl, "root", MYSQL.getPassword())
                    .target("1").load().migrate();
            UUID id = UUID.randomUUID();
            try (var connection = DriverManager.getConnection(upgradeUrl, "root", MYSQL.getPassword());
                 var statement = connection.prepareStatement("""
                         INSERT INTO notification (notification_id, recipient_id, title, body, status,
                             provider_message_id, created_at, completed_at)
                         VALUES (?, 'synthetic', 'existing title', 'existing body', 'PROVIDER_ACCEPTED',
                             'existing-result', '2026-10-13 00:00:00.123456', '2026-10-13 00:00:01.123456')
                         """)) {
                statement.setString(1, id.toString());
                statement.executeUpdate();
            }
            Flyway.configure().dataSource(upgradeUrl, "root", MYSQL.getPassword()).load().migrate();
            try (var connection = DriverManager.getConnection(upgradeUrl, "root", MYSQL.getPassword());
                 var statement = connection.createStatement();
                 var rows = statement.executeQuery("SELECT * FROM notification")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("notification_id")).isEqualTo(id.toString());
                assertThat(rows.getString("status")).isEqualTo("PROVIDER_ACCEPTED");
                assertThat(rows.getString("body")).isEqualTo("existing body");
                assertThat(rows.getString("provider_message_id")).isEqualTo("existing-result");
                assertThat(rows.getString("completed_at")).isEqualTo("2026-10-13 00:00:01.123456");
                assertThat(rows.getString("worker_token")).isNull();
                assertThat(rows.next()).isFalse();
            }
        } finally {
            executeAsRoot("DROP DATABASE " + schema);
        }
    }

    @Test
    void deliveryMigrationPreservesOldLeasesAndDoesNotInventHistoricalAttempts() throws Exception {
        String schema = "delivery_upgrade_test";
        executeAsRoot("CREATE DATABASE " + schema);
        String url = MYSQL.getJdbcUrl().replace("/notification_lab", "/" + schema);
        try {
            Flyway.configure().dataSource(url, "root", MYSQL.getPassword()).target("3").load().migrate();
            var db = new JdbcTemplate(new org.springframework.jdbc.datasource.DriverManagerDataSource(
                    url, "root", MYSQL.getPassword()));
            String queued = UUID.randomUUID().toString();
            String active = UUID.randomUUID().toString();
            String completed = UUID.randomUUID().toString();
            String legacy = UUID.randomUUID().toString();
            String token = UUID.randomUUID().toString();
            for (String id : List.of(queued, active, completed, legacy)) {
                db.update("""
                        INSERT INTO notification (notification_id, recipient_id, title, body, status, created_at)
                        VALUES (?, 'synthetic', 'title', 'body', 'QUEUED', '2026-10-13 00:00:00.123456')
                        """, id);
                if (!id.equals(legacy)) db.update("""
                        INSERT INTO outbox_event (event_id, notification_id, event_type, payload, created_at, available_at)
                        VALUES (?, ?, 'NOTIFICATION_REQUESTED', JSON_OBJECT('schemaVersion', 1),
                            UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                        """, UUID.randomUUID().toString(), id);
            }
            db.update("UPDATE notification SET status = 'PROCESSING', worker_token = ?, "
                    + "worker_lease_until = '2026-10-14 00:00:30.123456' WHERE notification_id = ?", token, active);
            db.update("UPDATE notification SET status = 'PROCESSING' WHERE notification_id = ?", legacy);
            db.update("UPDATE notification SET status = 'PROVIDER_ACCEPTED', provider_message_id = 'old-result', "
                    + "completed_at = '2026-10-13 00:00:01.123456' WHERE notification_id = ?", completed);
            var before = db.queryForList("SELECT * FROM notification ORDER BY notification_id");
            var eventsBefore = db.queryForList("SELECT * FROM outbox_event ORDER BY event_id");
            Flyway.configure().dataSource(url, "root", MYSQL.getPassword()).target("4").load().migrate();
            assertThat(db.queryForList("SELECT * FROM notification ORDER BY notification_id")).isEqualTo(before);
            assertThat(db.queryForList("SELECT * FROM outbox_event ORDER BY event_id")).isEqualTo(eventsBefore);
            assertThat(db.queryForObject("SELECT COUNT(*) FROM delivery_task", Integer.class)).isEqualTo(3);
            assertThat(db.queryForList("SELECT notification_id FROM delivery_task", String.class))
                    .containsExactlyInAnyOrder(queued, active, completed);
            assertThat(db.queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?",
                    Integer.class, completed)).isZero();
            var attempts = db.queryForList("SELECT * FROM delivery_attempt");
            assertThat(attempts).hasSize(1);
            assertThat(attempts.getFirst()).containsEntry("lease_token", token).containsEntry("outcome", "STARTED")
                    .containsEntry("started_at", null).containsEntry("completed_at", null);
            assertThat(db.queryForObject("SELECT attempt_count FROM delivery_task WHERE notification_id = ?",
                    Integer.class, active)).isEqualTo(1);
            assertThat(db.queryForList("SELECT max_attempts FROM delivery_task", Integer.class)).containsOnly(3);
            // V5 must not silently reactivate an already terminal V4 transient failure.
            db.update("UPDATE notification SET status = 'RETRYABLE_FAILURE', provider_message_id = NULL, "
                    + "error_code = 'OLD_TRANSIENT' WHERE notification_id = ?", completed);
            var beforeRetryMigration = db.queryForList("SELECT * FROM notification ORDER BY notification_id");
            Flyway.configure().dataSource(url, "root", MYSQL.getPassword()).target("5").load().migrate();
            var outboxBeforeRedrive = db.queryForList("SELECT * FROM outbox_event ORDER BY event_id");
            var tasksBeforeRedrive = db.queryForList("SELECT * FROM delivery_task ORDER BY task_id");
            Flyway.configure().dataSource(url, "root", MYSQL.getPassword()).load().migrate();
            assertThat(db.queryForList("SELECT * FROM outbox_event ORDER BY event_id")).isEqualTo(outboxBeforeRedrive);
            assertThat(db.queryForList("SELECT * FROM delivery_task ORDER BY task_id"))
                    .usingRecursiveComparison().isEqualTo(tasksBeforeRedrive);
            assertThat(db.queryForObject("SELECT COUNT(*) FROM dead_letter_redrive", Integer.class)).isZero();
            assertThat(db.queryForList("SELECT * FROM notification ORDER BY notification_id")).isEqualTo(beforeRetryMigration);
            assertThat(db.queryForObject("SELECT COUNT(*) FROM outbox_event", Integer.class)).isEqualTo(3);
            assertThat(db.queryForList("SELECT retry_generation FROM outbox_event", Integer.class)).containsOnly(0);
            assertThat(db.queryForList("SELECT retry_generation FROM delivery_task", Integer.class)).containsOnly(0);
            assertThat(db.queryForObject("SELECT COUNT(*) FROM delivery_task WHERE next_attempt_at IS NOT NULL", Integer.class)).isZero();

        } finally {
            executeAsRoot("DROP DATABASE " + schema);
        }
    }

    @Test
    void initialWriteCommitsAndProviderRunsOutsideEvenAnExistingTransaction() {
        probe().beforeSend = () -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            assertThat(jdbc().queryForObject(
                    "SELECT COUNT(*) FROM notification WHERE status = 'PROCESSING'", Integer.class))
                    .isEqualTo(1);
        };
        transactions().executeWithoutResult(tx -> {
            Notification result = app.getBean(NotificationService.class)
                    .send(new NotificationRequest("test-user", "title", "body"));
            assertThat(result.status()).isEqualTo(NotificationStatus.PROVIDER_ACCEPTED);
            tx.setRollbackOnly();
        });
        // The caller transaction is suspended for send; both store writes committed separately.
        assertThat(jdbc().queryForObject(
                "SELECT COUNT(*) FROM notification WHERE status = 'PROVIDER_ACCEPTED'", Integer.class))
                .isEqualTo(1);
        assertThat(probe().attempts.get()).isEqualTo(1);
    }

    @Test
    void retryableFailureIsPersistedWithoutAutomaticRetry() throws Exception {
        probe().behavior = r -> new PushResult(PushResult.Outcome.RETRYABLE_FAILURE, null, "TEST_UNAVAILABLE");
        HttpResponse<String> response = request("POST", "/notifications", PAYLOAD);
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(json(response).path("status").asText()).isEqualTo("RETRYABLE_FAILURE");
        assertThat(json(request("GET", response.headers().firstValue("Location").orElseThrow(), null)))
                .isEqualTo(json(response));
        assertThat(probe().attempts.get()).isEqualTo(1);
    }

    @Test
    void permanentAndUnexpectedProviderFailuresArePersisted() throws Exception {
        probe().behavior = r -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "TEST_INVALID");
        HttpResponse<String> permanent = request("POST", "/notifications", PAYLOAD);
        assertThat(permanent.statusCode()).isEqualTo(201);
        assertThat(json(permanent).path("status").asText()).isEqualTo("PERMANENT_FAILURE");
        probe().behavior = r -> { throw new IllegalStateException("sensitive-test-detail"); };
        HttpResponse<String> unexpected = request("POST", "/notifications", PAYLOAD);
        assertThat(unexpected.statusCode()).isEqualTo(201);
        assertThat(json(unexpected).path("status").asText()).isEqualTo("INTERNAL_FAILURE");
        assertThat(unexpected.body()).doesNotContain("sensitive-test-detail");
        assertThat(jdbc().queryForObject(
                "SELECT COUNT(*) FROM notification WHERE status <> 'PROCESSING'", Integer.class)).isEqualTo(2);
    }

    @Test
    void databaseRejectsConcurrentDuplicateNotificationIds() throws Exception {
        Notification pending = pending();
        Callable<Boolean> insert = () -> {
            try {
                store().create(pending);
                return true;
            } catch (DuplicateKeyException expected) {
                return false;
            }
        };
        try (var executor = Executors.newFixedThreadPool(2)) {
            var results = executor.invokeAll(List.of(insert, insert));
            assertThat(List.of(results.get(0).get(), results.get(1).get()))
                    .containsExactlyInAnyOrder(true, false);
        }
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification", Integer.class)).isEqualTo(1);
    }

    @Test
    void failedTransactionRollsBackBothNotificationAndOutbox() {
        Notification pending = pending();
        assertThatThrownBy(() -> transactions().executeWithoutResult(tx -> {
            store().create(pending);
            insertEvent(pending.notificationId());
            insertEvent(pending.notificationId()); // violates unique (notification_id, event_type)
        })).isInstanceOf(DuplicateKeyException.class);
        assertThat(store().find(pending.notificationId())).isEmpty();
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event", Integer.class)).isZero();
    }

    @Test
    void completionRollsBackAndCannotOverwriteATerminalResult() {
        Notification pending = pending();
        store().create(pending);
        Notification completed = pending.completed(NotificationStatus.PROVIDER_ACCEPTED,
                "mock-test", null, pending.createdAt().plusSeconds(1));
        transactions().executeWithoutResult(tx -> {
            store().update(completed);
            tx.setRollbackOnly();
        });
        assertThat(store().find(pending.notificationId())).contains(pending);
        store().update(completed);
        assertThatThrownBy(() -> store().update(completed)).isInstanceOf(IllegalStateException.class);
        assertThat(store().find(pending.notificationId())).contains(completed);
    }

    @Test
    void foreignKeyAndUniqueEventConstraintAreEnforced() {
        assertThatThrownBy(() -> insertEvent(UUID.randomUUID())).isInstanceOf(DataIntegrityViolationException.class);
        Notification pending = pending();
        store().create(pending);
        insertEvent(pending.notificationId());
        assertThatThrownBy(() -> insertEvent(pending.notificationId())).isInstanceOf(DuplicateKeyException.class);
        assertThatThrownBy(() -> jdbc().update("DELETE FROM notification WHERE notification_id = ?",
                pending.notificationId().toString())).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void statusAndTimestampConstraintsRejectInconsistentRows() {
        Notification pending = pending();
        store().create(pending);
        assertMysqlError("UPDATE notification SET status = 'UNKNOWN'", 3819);
        assertMysqlError("UPDATE notification SET status = 'PROVIDER_ACCEPTED'", 3819);
        assertThatThrownBy(() -> jdbc().update("UPDATE notification SET body = NULL"))
                .isInstanceOf(DataIntegrityViolationException.class);
        insertEvent(pending.notificationId());
        assertMysqlError("UPDATE outbox_event SET attempt_count = -1", 3819);
        assertMysqlError("UPDATE outbox_event SET status = 'PUBLISHED'", 3819);
        assertMysqlError("UPDATE outbox_event SET payload = 'invalid-json'", 3140);
    }

    @Test
    void indexesMatchStatusAndTimeLookupOrder() {
        assertThat(indexColumns("notification", "idx_notification_status_created"))
                .containsExactly("status", "created_at", "notification_id");
        assertThat(indexColumns("outbox_event", "idx_outbox_status_available"))
                .containsExactly("status", "available_at", "event_id");
        assertThat(jdbc().queryForObject("SELECT VERSION()", String.class)).startsWith("8.4.8");
    }

    @Test
    void failedInitialWriteDoesNotCallProviderOrExposeDatabaseDetails() throws Exception {
        executeAsRoot("""
                CREATE TRIGGER test_fail_insert BEFORE INSERT ON notification FOR EACH ROW
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sensitive-test-detail'
                """);
        try {
            HttpResponse<String> response = request("POST", "/notifications", PAYLOAD);
            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.body()).doesNotContain("sensitive-test-detail", "test-user", "재시작 후 조회");
            assertThat(probe().attempts.get()).isZero();
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification", Integer.class)).isZero();
        } finally {
            executeAsRoot("DROP TRIGGER test_fail_insert");
        }
    }

    @Test
    void failedCompletionReturns503AndProcessingRemainsAfterRestart() throws Exception {
        executeAsRoot("""
                CREATE TRIGGER test_fail_update BEFORE UPDATE ON notification FOR EACH ROW
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sensitive-test-detail'
                """);
        UUID id;
        try {
            HttpResponse<String> response = request("POST", "/notifications", PAYLOAD);
            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.body()).doesNotContain("sensitive-test-detail", "test-user");
            assertThat(probe().attempts.get()).isEqualTo(1);
            id = UUID.fromString(jdbc().queryForObject("SELECT notification_id FROM notification", String.class));
            assertThat(store().find(id).orElseThrow().status()).isEqualTo(NotificationStatus.PROCESSING);
        } finally {
            executeAsRoot("DROP TRIGGER test_fail_update");
        }
        app.close();
        startApplication();
        assertThat(json(request("GET", "/notifications/" + id, null)).path("status").asText())
                .isEqualTo("PROCESSING");
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void unavailableConnectionPoolReturns503BeforeProviderInvocation() throws Exception {
        app.getBean(HikariDataSource.class).close();
        try {
            HttpResponse<String> response = request("POST", "/notifications", PAYLOAD);
            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(json(response).path("detail").asText())
                    .isEqualTo("Notification storage is temporarily unavailable");
            assertThat(probe().attempts.get()).isZero();
        } finally {
            app.close();
            startApplication();
        }
    }

    @Test
    void httpValidationHealthAndMetricsWorkWithMysql() throws Exception {
        assertThat(request("POST", "/notifications", "{}").statusCode()).isEqualTo(400);
        assertThat(request("GET", "/notifications/not-a-uuid", null).statusCode()).isEqualTo(400);
        assertThat(request("GET", "/notifications/" + UUID.randomUUID(), null).statusCode()).isEqualTo(404);
        assertThat(json(request("GET", "/actuator/health", null)).path("status").asText()).isEqualTo("UP");
        assertThat(request("GET", "/actuator/env", null).statusCode()).isEqualTo(404);
        assertThat(request("POST", "/notifications", PAYLOAD).statusCode()).isEqualTo(201);
        HttpResponse<String> metrics = request("GET", "/actuator/prometheus", null);
        assertThat(metrics.statusCode()).isEqualTo(200);
        assertThat(metrics.body()).contains("notification_provider_attempts_total")
                .doesNotContain("test-user", "재시작 후 조회");
    }

    private static void assertMysqlError(String sql, int errorCode) {
        // MySQL CHECK violations use HY000; Spring may classify them as uncategorized SQL errors.
        assertThatThrownBy(() -> jdbc().update(sql)).isInstanceOf(DataAccessException.class)
                .satisfies(error -> {
                    Throwable cause = ((DataAccessException) error).getMostSpecificCause();
                    assertThat(cause).isInstanceOf(SQLException.class);
                    assertThat(((SQLException) cause).getErrorCode()).isEqualTo(errorCode);
                });
    }

    private static void executeAsRoot(String sql) throws SQLException {
        // Only fault-injection DDL uses root in this disposable container; the app uses its own user.
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static Notification pending() {
        return new Notification(UUID.randomUUID(), "test-user", "title", "body", NotificationStatus.PROCESSING,
                null, null, Instant.parse("2026-10-13T01:00:00.123456Z"), null);
    }

    private static void insertEvent(UUID notificationId) {
        jdbc().update("""
                INSERT INTO outbox_event (event_id, notification_id, event_type, payload, created_at, available_at)
                VALUES (?, ?, 'NOTIFICATION_REQUESTED', '{}', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """, UUID.randomUUID().toString(), notificationId.toString());
    }

    private static List<String> indexColumns(String table, String index) {
        return jdbc().queryForList("""
                SELECT column_name FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
                ORDER BY seq_in_index
                """, String.class, table, index);
    }

    private static HttpResponse<String> request(String method, String path, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(
                "http://127.0.0.1:" + app.getWebServer().getPort() + path)).timeout(Duration.ofSeconds(10));
        if (body == null) builder.GET();
        else builder.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(body));
        return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode json(HttpResponse<String> response) throws Exception {
        return app.getBean(ObjectMapper.class).readTree(response.body());
    }

    private static JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private static NotificationStore store() { return app.getBean(NotificationStore.class); }
    private static ProviderProbe probe() { return app.getBean(ProviderProbe.class); }
    private static TransactionTemplate transactions() {
        return new TransactionTemplate(app.getBean(PlatformTransactionManager.class));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ProviderProbeConfiguration {
        @Bean
        @Primary
        ProviderProbe providerProbe(MockProperties properties) {
            return new ProviderProbe(new MockPushProvider(properties));
        }
    }

    static class ProviderProbe implements PushProvider {
        final AtomicInteger attempts = new AtomicInteger();
        final PushProvider delegate;
        volatile Runnable beforeSend;
        volatile Function<PushRequest, PushResult> behavior;

        ProviderProbe(PushProvider delegate) {
            this.delegate = delegate;
            reset();
        }

        void reset() {
            attempts.set(0);
            beforeSend = () -> {};
            behavior = delegate::send;
        }

        @Override
        public PushResult send(PushRequest request) {
            attempts.incrementAndGet();
            beforeSend.run();
            return behavior.apply(request);
        }
    }
}
