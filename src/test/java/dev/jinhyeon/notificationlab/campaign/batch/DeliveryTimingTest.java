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
                case "setAutoCommit" -> { autoCommit = (boolean) args[0]; yield null; }
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
