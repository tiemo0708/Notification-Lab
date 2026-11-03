package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Part;
import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Phase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryTimingTest {
    @Test
    void boundedFailureWindowReceivesRealSpringJdbcCallbacksWithoutExtraDriverCalls() throws Exception {
        var window = new ApiFailureWindow(2);
        var driver = new JdbcDriver();
        var timing = new DeliveryTiming(window);
        var source = timing.dataSource(driver);
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        window.open(ApiFailureWindow.Window.CONTROL);
        timing.scope(Phase.API_SUBMIT, () -> tx.execute(status -> {
            for (int i = 0; i < 4; i++) new JdbcTemplate(source).update("PRIVATE_SQL", "PRIVATE_ARG");
            return null;
        }));
        window.complete(202, 1);
        assertThat(window.snapshot()).containsEntry("completed", 1).containsEntry("activePresent", false);
        assertThat(driver.events).hasSize(15).endsWith("commit", "release");
        assertThat(new ObjectMapper().writeValueAsString(window.snapshot())).doesNotContain("PRIVATE_SQL", "PRIVATE_ARG", "threadId", "sessionId");
    }

    @Test
    void boundedWindowPreservesOriginalSqlFailureAndRecordsRollbackWithoutInventingAcceptance() throws Exception {
        var window = new ApiFailureWindow(2);
        var driver = new JdbcDriver(); driver.sqlFailure = new SQLException("PRIVATE_FAILURE", "45000");
        var timing = new DeliveryTiming(window);
        var source = timing.dataSource(driver);
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        window.open(ApiFailureWindow.Window.CONTROL);
        assertThatThrownBy(() -> timing.scope(Phase.API_SUBMIT, () -> tx.execute(status -> {
            new JdbcTemplate(source).update("PRIVATE_SQL", "PRIVATE_ARG"); return null;
        }))).hasRootCause(driver.sqlFailure);
        var frozen = window.freeze(ApiFailureWindow.Stop.SERVER_FAILURE);
        @SuppressWarnings("unchecked") var active = (Map<String, Object>) frozen.get("active");
        assertThat(active).containsEntry("failed", true).containsEntry("rollbackReturned", true)
                .containsEntry("commitReturned", false).containsEntry("responseObserved", false);
        assertThat(frozen).containsEntry("completed", 0).containsEntry("activeServerOutcome", "UNKNOWN");
        assertThat(driver.events).containsExactly("acquire", "prepare", "execute", "statement.close", "rollback", "release");
        assertThat(new ObjectMapper().writeValueAsString(frozen)).doesNotContain("PRIVATE_FAILURE", "PRIVATE_SQL", "PRIVATE_ARG");
    }

    @Test
    void pairsApiAcquisitionFourStatementsAndCommitAcrossTheTransactionBoundary() throws Exception {
        var api = new ApiRequestTiming(1, true);
        var driver = new JdbcDriver();
        var timing = new DeliveryTiming(api.observer());
        var source = timing.dataSource(driver);
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        var capture = api.open(ApiRequestTiming.Window.MIXED);
        api.serve(() -> timing.scope(Phase.API_SUBMIT, () -> tx.execute(status -> {
            for (int i = 0; i < 4; i++) new JdbcTemplate(source).update("PRIVATE_SQL", "PRIVATE_ARG");
            return null;
        })));
        api.complete(capture, 100, 202);
        assertCount(timing, Phase.API_SUBMIT, Part.STORE_CALL, 1, 0);
        assertCount(timing, Phase.API_SUBMIT, Part.ACQUIRE, 1, 0);
        assertCount(timing, Phase.API_SUBMIT, Part.EXECUTE, 4, 0);
        assertCount(timing, Phase.API_SUBMIT, Part.COMMIT, 1, 0);
        assertCount(timing, Phase.API_SUBMIT, Part.RELEASE, 1, 0);
        assertThat(driver.events).endsWith("commit", "release");
        assertThat(new ObjectMapper().writeValueAsString(api.snapshot())).doesNotContain("PRIVATE_SQL", "PRIVATE_ARG");
    }

    @Test
    void recordsRestorationAfterRollbackWithoutTurningItIntoACommit() throws Exception {
        var calls = new ApiConnectionTiming();
        var driver = new JdbcDriver();
        driver.sqlFailure = new SQLException("PRIVATE_SQL_FAILURE", "45000");
        var timing = new DeliveryTiming(connectionObserver(calls));
        var source = timing.dataSource(driver);
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        assertThatThrownBy(() -> timing.scope(Phase.API_SUBMIT, () -> tx.execute(status -> {
            new JdbcTemplate(source).update("PRIVATE_SQL", "PRIVATE_ARG"); return null;
        }))).hasRootCause(driver.sqlFailure);
        assertThat(calls.snapshot()).extracting(r -> r.get("method")).containsExactly("GET_AUTO_COMMIT", "SET_AUTO_COMMIT_FALSE", "SET_AUTO_COMMIT_TRUE");
        assertThat(calls.snapshot()).extracting(r -> r.get("boundary")).containsExactly("BEFORE_FIRST_SQL", "BEFORE_FIRST_SQL", "AFTER_ROLLBACK");
        assertCount(timing, Phase.API_SUBMIT, Part.COMMIT, 0, 0);
        assertCount(timing, Phase.API_SUBMIT, Part.ROLLBACK, 1, 0);
        assertThat(driver.events).endsWith("rollback", "release");
        assertThat(new ObjectMapper().writeValueAsString(calls.snapshot())).doesNotContain("PRIVATE_SQL", "PRIVATE_ARG", "PRIVATE_SQL_FAILURE");
    }

    @Test
    void preservesStateChangeFailureAndDoesNotInventSqlOrCompletionBoundaries() throws Exception {
        var calls = new ApiConnectionTiming();
        var driver = new JdbcDriver();
        driver.stateFailure = new SQLException("PRIVATE_STATE_FAILURE", "45000");
        var timing = new DeliveryTiming(connectionObserver(calls));
        var source = timing.dataSource(driver);
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        assertThatThrownBy(() -> timing.scope(Phase.API_SUBMIT, () -> tx.execute(status -> {
            throw new AssertionError("Body must not run");
        }))).hasRootCause(driver.stateFailure);
        assertThat(calls.snapshot()).hasSize(2);
        assertThat(calls.snapshot().get(1)).containsEntry("method", "SET_AUTO_COMMIT_FALSE")
                .containsEntry("boundary", "BEFORE_FIRST_SQL").containsEntry("failed", true);
        assertThat(driver.events).containsExactly("acquire", "release");
        assertCount(timing, Phase.API_SUBMIT, Part.AUTOCOMMIT_STATE, 2, 1);
        assertThat(new ObjectMapper().writeValueAsString(calls.snapshot())).doesNotContain("PRIVATE_STATE_FAILURE");
    }

    private static DeliveryTiming.ApiObserver connectionObserver(ApiConnectionTiming calls) {
        return new DeliveryTiming.ApiObserver() {
            @Override public void record(Part part, long nanos, boolean failed) { }
            @Override public boolean connectionCallsEnabled() { return true; }
            @Override public void connectionCall(ApiConnectionTiming.Method method, ApiConnectionTiming.Boundary boundary, long nanos, boolean failed) {
                calls.record(method, boundary, nanos, failed);
            }
        };
    }

    @Test
    void observesOneTransactionWithoutChangingCommitOrderOrRetainingSql() throws Exception {
        var driver = new JdbcDriver();
        var timing = new DeliveryTiming();
        var source = timing.dataSource(driver);
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        var jdbc = new JdbcTemplate(source);
        timing.scope(Phase.WORKER_COMPLETE, () -> tx.execute(status -> {
            assertThat(jdbc.update("PRIVATE_SQL", "PRIVATE_RECIPIENT")).isEqualTo(1);
            return null;
        }));
        assertThat(driver.events).containsExactly("acquire", "prepare", "execute", "statement.close", "commit", "release");
        assertCount(timing, Phase.WORKER_COMPLETE, Part.ACQUIRE, 1, 0);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.EXECUTE, 1, 0);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.COMMIT, 1, 0);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.ROLLBACK, 0, 0);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.RELEASE, 1, 0);
        assertThat(new ObjectMapper().writeValueAsString(timing.snapshot())).doesNotContain("PRIVATE_SQL", "PRIVATE_RECIPIENT");
        // The ThreadLocal scope was removed: unrelated SQL is not assigned to this store call.
        try (var connection = source.getConnection()) { assertThat(connection).isSameAs(driver.connection); }
        assertCount(timing, Phase.WORKER_COMPLETE, Part.ACQUIRE, 1, 0);
    }

    @Test
    void preservesOriginalSqlFailureAndRollbackInsteadOfCommitting() throws Exception {
        var driver = new JdbcDriver();
        var timing = new DeliveryTiming();
        var source = timing.dataSource(driver);
        var tx = new TransactionTemplate(new JdbcTransactionManager(source));
        SQLException original = new SQLException("PRIVATE_FAILURE", "45000");
        driver.sqlFailure = original;
        assertThatThrownBy(() -> timing.scope(Phase.WORKER_COMPLETE, () -> tx.execute(status -> {
            new JdbcTemplate(source).update("PRIVATE_SQL", "PRIVATE_ARG");
            return null;
        }))).hasRootCause(original);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.EXECUTE, 1, 1);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.STORE_CALL, 1, 1);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.ROLLBACK, 1, 0);
        assertCount(timing, Phase.WORKER_COMPLETE, Part.COMMIT, 0, 0);
        assertThat(driver.events).containsExactly("acquire", "prepare", "execute", "statement.close", "rollback", "release");
        assertThat(new ObjectMapper().writeValueAsString(timing.snapshot())).doesNotContain("PRIVATE_FAILURE", "PRIVATE_SQL");
    }

    @Test
    void doesNotRecordUnfinishedCallsOrInitiateConsumerCommits() throws Exception {
        var timing = new DeliveryTiming();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var called = new ArrayList<Object[]>();
        var offsets = Map.of(new TopicPartition("synthetic", 0), new OffsetAndMetadata(9));
        try (var executor = Executors.newSingleThreadExecutor()) {
            Consumer<String, String> consumer = timing.consumer(fakeConsumer((object, method, args) -> {
                if (method.getName().equals("commitSync")) {
                    called.add(args);
                    entered.countDown();
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("Commit was not released");
                }
                return null;
            }));
            assertThat(called).isEmpty();
            var future = executor.submit(() -> consumer.commitSync(offsets, Duration.ofSeconds(2)));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(timing::snapshot).isInstanceOf(IllegalStateException.class);
            } finally { release.countDown(); }
            future.get(5, TimeUnit.SECONDS);
            assertThat(called).hasSize(1);
            assertThat(called.getFirst()[0]).isSameAs(offsets);
            assertThat(called.getFirst()[1]).isEqualTo(Duration.ofSeconds(2));
            assertCount(timing, Phase.CONSUMER, Part.COMMIT_SYNC, 1, 0);
        }
    }

    @Test
    void preservesConsumerCommitFailureAndWrapperIdentity() {
        var timing = new DeliveryTiming();
        var original = new IllegalStateException("PRIVATE_COMMIT_ERROR");
        Consumer<String, String> consumer = timing.consumer(fakeConsumer((object, method, args) -> {
            throw original;
        }));
        assertThat(consumer.equals(consumer)).isTrue();
        assertThat(consumer.toString()).isEqualTo("Delivery timing wrapper");
        assertThatThrownBy(consumer::commitSync).isSameAs(original);
        assertCount(timing, Phase.CONSUMER, Part.COMMIT_SYNC, 1, 1);
        assertCount(timing, Phase.CONSUMER, Part.COMMIT_ASYNC_SUBMIT, 0, 0);
    }

    @Test
    void preservesPoolUnwrappingAndDoesNotExportPoolConfiguration() throws Exception {
        var timing = new DeliveryTiming();
        try (var pool = new HikariDataSource()) {
            pool.setUsername("PRIVATE_USER");
            pool.setPassword("PRIVATE_PASSWORD");
            var source = timing.dataSource(pool);
            assertThat(source.isWrapperFor(HikariDataSource.class)).isTrue();
            assertThat(source.unwrap(HikariDataSource.class)).isSameAs(pool);
            assertThat(new ObjectMapper().writeValueAsString(timing.snapshot())).doesNotContain("PRIVATE_USER", "PRIVATE_PASSWORD");
        }
    }

    @SuppressWarnings("unchecked")
    private static Consumer<String, String> fakeConsumer(java.lang.reflect.InvocationHandler handler) {
        return (Consumer<String, String>) proxy(Consumer.class, handler);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    @SuppressWarnings("unchecked")
    private static void assertCount(DeliveryTiming timing, Phase phase, Part part, long expected, long failures) {
        var phases = (Map<String, Map<String, Map<String, Object>>>) timing.snapshot().get("phases");
        assertThat(phases.get(phase.name()).get(part.name())).containsEntry("count", expected).containsEntry("failures", failures);
    }

    private static final class JdbcDriver extends AbstractDataSource {
        private final List<String> events = new ArrayList<>();
        private boolean autoCommit = true;
        private SQLException sqlFailure;
        private SQLException stateFailure;
        private final PreparedStatement statement = proxy(PreparedStatement.class, (object, method, args) -> {
            return switch (method.getName()) {
                case "executeUpdate" -> { events.add("execute"); if (sqlFailure != null) throw sqlFailure; yield 1; }
                case "close" -> { events.add("statement.close"); yield null; }
                case "getUpdateCount" -> 1;
                case "isClosed" -> false;
                default -> null;
            };
        });
        private final Connection connection = proxy(Connection.class, (object, method, args) -> {
            return switch (method.getName()) {
                case "getAutoCommit" -> autoCommit;
                case "setAutoCommit" -> {
                    if (Boolean.FALSE.equals(args[0]) && stateFailure != null) throw stateFailure;
                    autoCommit = (boolean) args[0]; yield null;
                }
                case "isReadOnly", "isClosed" -> false;
                case "prepareStatement" -> { events.add("prepare"); yield statement; }
                case "commit", "rollback" -> { events.add(method.getName()); yield null; }
                case "close" -> { events.add("release"); yield null; }
                default -> null;
            };
        });
        @Override public Connection getConnection() { events.add("acquire"); return connection; }
        @Override public Connection getConnection(String username, String password) { return getConnection(); }
    }
}
