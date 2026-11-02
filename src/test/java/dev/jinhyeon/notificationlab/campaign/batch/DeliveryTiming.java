package dev.jinhyeon.notificationlab.campaign.batch;

import com.zaxxer.hikari.HikariDataSource;
import dev.jinhyeon.notificationlab.outbox.OutboxStore;
import dev.jinhyeon.notificationlab.outbox.SubmissionService;
import dev.jinhyeon.notificationlab.worker.WorkerStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.sql.DataSource;
import org.aopalliance.intercept.MethodInterceptor;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/** Opt-in test instrumentation. Fixed counters only: no SQL, arguments, records or identifiers retained. */
final class DeliveryTiming {
    enum Phase {
        OUTBOX_CLAIM, OUTBOX_PUBLISHED, OUTBOX_FAILED, WORKER_INITIAL, WORKER_ADMITTED, WORKER_COMPLETE, CONSUMER, API_SUBMIT
    }
    enum Part { STORE_CALL, ACQUIRE, PREPARE, EXECUTE, COMMIT, ROLLBACK, AUTOCOMMIT_STATE, ISOLATION_STATE, READ_ONLY_STATE, CONNECTION_STATE, RELEASE, STATEMENT_CLOSE, COMMIT_SYNC, COMMIT_ASYNC_SUBMIT }
    @FunctionalInterface interface Action<T, E extends Throwable> { T run() throws E; }
    @FunctionalInterface interface ApiObserver { void record(Part part, long nanos, boolean failed); }

    private final ThreadLocal<Phase> current = new ThreadLocal<>();
    private final Stats[][] stats = new Stats[Phase.values().length][Part.values().length];
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger consumers = new AtomicInteger();
    private final List<HikariDataSource> pools = new ArrayList<>();
    private final ApiObserver apiObserver;

    DeliveryTiming() { this(null); }

    DeliveryTiming(ApiObserver apiObserver) {
        this.apiObserver = apiObserver;
        for (int p = 0; p < stats.length; p++) {
            for (int c = 0; c < stats[p].length; c++) stats[p][c] = new Stats();
        }
    }

    <T, E extends Throwable> T measure(Phase phase, Part part, Action<T, E> action) throws E {
        long start = System.nanoTime();
        active.incrementAndGet();
        boolean failed = true;
        try {
            T value = action.run();
            failed = false;
            return value;
        } finally {
            long nanos = System.nanoTime() - start;
            stats[phase.ordinal()][part.ordinal()].record(nanos, failed);
            active.decrementAndGet();
            if (phase == Phase.API_SUBMIT && apiObserver != null) apiObserver.record(part, nanos, failed);
        }
    }

    <T, E extends Throwable> T scope(Phase phase, Action<T, E> action) throws E {
        Phase previous = current.get();
        current.set(phase);
        try { return measure(phase, Part.STORE_CALL, action); }
        finally {
            if (previous == null) current.remove(); else current.set(previous);
        }
    }

    private Object decorate(Object bean) {
        if (bean instanceof HikariDataSource pool) {
            pools.add(pool);
            return dataSource(pool);
        }
        if (bean instanceof DefaultKafkaConsumerFactory<?, ?> factory) attach(factory);
        return bean;
    }

    private <K, V> void attach(DefaultKafkaConsumerFactory<K, V> factory) {
        factory.addPostProcessor(this::consumer);
    }

    void attachStores(OutboxStore outbox, WorkerStore worker) {
        attachStore(outbox, false);
        attachStore(worker, true);
    }

    void attachSubmission(SubmissionService submission) {
        if (!(submission instanceof Advised advised) || advised.isFrozen()) {
            throw new IllegalStateException("Expected mutable submission transaction proxy");
        }
        advised.addAdvice(0, (MethodInterceptor) invocation -> invocation.getMethod().getName().equals("accept")
                ? scope(Phase.API_SUBMIT, invocation::proceed) : invocation.proceed());
    }

    private void attachStore(Object store, boolean worker) {
        if (!(store instanceof Advised advised) || advised.isFrozen()) {
            throw new IllegalStateException("Expected mutable transactional store proxy");
        }
        // Outside the existing TransactionInterceptor: includes acquisition, commit and release.
        advised.addAdvice(0, (MethodInterceptor) invocation -> {
            Phase phase = switch (invocation.getMethod().getName()) {
                case "claim" -> worker ? (invocation.getMethod().getParameterCount() == 5 ? Phase.WORKER_INITIAL : Phase.WORKER_ADMITTED) : Phase.OUTBOX_CLAIM;
                case "published" -> worker ? null : Phase.OUTBOX_PUBLISHED;
                case "failed" -> worker ? null : Phase.OUTBOX_FAILED;
                case "complete" -> worker ? Phase.WORKER_COMPLETE : null;
                default -> null;
            };
            return phase == null ? invocation.proceed() : scope(phase, invocation::proceed);
        });
    }

    DataSource dataSource(DataSource target) {
        return new TimedDataSource(target);
    }

    private final class TimedDataSource extends DelegatingDataSource implements AutoCloseable {
        TimedDataSource(DataSource target) { super(target); }
        @Override public Connection getConnection() throws SQLException {
            return acquire(() -> obtainTargetDataSource().getConnection());
        }
        @Override public Connection getConnection(String username, String password) throws SQLException {
            return acquire(() -> obtainTargetDataSource().getConnection(username, password));
        }
        private Connection acquire(Action<Connection, SQLException> action) throws SQLException {
            Phase phase = current.get();
            if (phase == null) return action.run();
            Connection connection = measure(phase, Part.ACQUIRE, action);
            return wrapConnection(connection, phase);
        }
        @Override public void close() throws Exception {
            if (obtainTargetDataSource() instanceof AutoCloseable closeable) closeable.close();
        }
    }

    private Connection wrapConnection(Connection target, Phase phase) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) return objectMethod(proxy, method, args);
            String name = method.getName();
            Part part = switch (name) {
                case "prepareStatement", "prepareCall", "createStatement" -> Part.PREPARE;
                case "commit" -> Part.COMMIT;
                case "rollback" -> Part.ROLLBACK;
                case "close" -> Part.RELEASE;
                case "getAutoCommit", "setAutoCommit" -> Part.AUTOCOMMIT_STATE;
                case "getTransactionIsolation", "setTransactionIsolation" -> Part.ISOLATION_STATE;
                case "isReadOnly", "setReadOnly" -> Part.READ_ONLY_STATE;
                default -> Part.CONNECTION_STATE;
            };
            Object result = measure(phase, part, () -> invoke(target, method, args));
            return result instanceof Statement statement ? wrapStatement(statement, (Connection) proxy, phase) : result;
        });
    }

    private Statement wrapStatement(Statement target, Connection connection, Phase phase) {
        Class<?> type = target instanceof CallableStatement ? CallableStatement.class : target instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
        return (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) return objectMethod(proxy, method, args);
            if (method.getName().equals("getConnection")) return connection;
            Part part = switch (method.getName()) {
                case "execute", "executeQuery", "executeUpdate", "executeLargeUpdate", "executeBatch", "executeLargeBatch" -> Part.EXECUTE;
                case "close" -> Part.STATEMENT_CLOSE;
                default -> null;
            };
            return part == null ? invoke(target, method, args) : measure(phase, part, () -> invoke(target, method, args));
        });
    }

    @SuppressWarnings("unchecked")
    <K, V> Consumer<K, V> consumer(Consumer<K, V> target) {
        consumers.incrementAndGet();
        return (Consumer<K, V>) Proxy.newProxyInstance(Consumer.class.getClassLoader(), new Class<?>[] {Consumer.class}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) return objectMethod(proxy, method, args);
            Part part = switch (method.getName()) {
                case "commitSync" -> Part.COMMIT_SYNC;
                case "commitAsync" -> Part.COMMIT_ASYNC_SUBMIT;
                default -> null;
            };
            // Delegate the original overload and arguments once; do not initiate or retry commits.
            return part == null ? invoke(target, method, args) : measure(Phase.CONSUMER, part, () -> invoke(target, method, args));
        });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }

    private static Object objectMethod(Object proxy, Method method, Object[] args) {
        return switch (method.getName()) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> "Delivery timing wrapper";
            default -> throw new IllegalStateException("Unexpected Object method");
        };
    }

    Map<String, Object> snapshot() {
        if (active.get() != 0) throw new IllegalStateException("Timing calls still in flight");
        var phases = new LinkedHashMap<String, Object>();
        for (Phase phase : Phase.values()) {
            var parts = new LinkedHashMap<String, Object>();
            for (Part part : Part.values()) parts.put(part.name(), stats[phase.ordinal()][part.ordinal()].snapshot());
            phases.put(phase.name(), parts);
        }
        return Map.of("version", apiObserver == null ? "jdbc-consumer-boundaries-v2" : "jdbc-consumer-api-boundaries-v3", "scope", "live app lifecycle including baseline, idle and shutdown; preload excluded",
                "wrappedConsumers", consumers.get(), "pools", pools.size(), "poolsClosed", pools.stream().allMatch(HikariDataSource::isClosed),
                "activeCalls", active.get(), "phases", phases);
    }

    private static final class Stats {
        private final LongAdder count = new LongAdder();
        private final LongAdder failures = new LongAdder();
        private final LongAdder total = new LongAdder();
        private final AtomicLong max = new AtomicLong();
        void record(long nanos, boolean failed) {
            count.increment(); total.add(nanos); max.accumulateAndGet(nanos, Math::max);
            if (failed) failures.increment();
        }
        Map<String, Object> snapshot() {
            long n = count.sum(); double millis = total.sum() / 1_000_000.0;
            return Map.of("count", n, "failures", failures.sum(), "totalMillis", millis, "meanMillis", n == 0 ? 0 : millis / n, "maxMillis", max.get() / 1_000_000.0);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Wiring {
        @Bean static BeanPostProcessor deliveryTimingPostProcessor(DeliveryTiming timing) {
            return new BeanPostProcessor() {
                @Override public Object postProcessAfterInitialization(Object bean, String name) {
                    return timing.decorate(bean);
                }
            };
        }
        @Bean SmartInitializingSingleton deliveryTimingStores(DeliveryTiming timing, OutboxStore outbox, WorkerStore worker) {
            return () -> timing.attachStores(outbox, worker);
        }
    }
}
