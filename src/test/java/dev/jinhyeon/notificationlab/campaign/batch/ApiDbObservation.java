package dev.jinhyeon.notificationlab.campaign.batch;

import com.mysql.cj.jdbc.JdbcConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Part;

/** Observe only the held physical connection during one synchronous native call; never export IDs or SQL. */
final class ApiDbObservation implements DeliveryTiming.ApiIoObserver, AutoCloseable {
    static final int MAX_QUERIES = 512;
    static final int MAX_SAMPLES_PER_REQUEST = 8;
    enum Kind { INSERT, UPDATE, COMMIT, OTHER, NONE }
    record Observation(boolean sessionPresent, boolean statementActive, Kind kind, int rowWaitEdges) { }
    @FunctionalInterface interface Reader extends AutoCloseable {
        Observation read(long session) throws Exception;
        @Override default void close() throws Exception { }
    }
    private static final class Request {
        final String window;
        final List<Map<String, Object>> samples = new ArrayList<>();
        long session;
        int executions;
        int overflow;
        boolean failed;
        Request(String window) { this.window = window; }
    }
    static final class Candidate {
        final Request request;
        final Part part;
        final int ordinal;
        Candidate(Request request, Part part, int ordinal) { this.request = request; this.part = part; this.ordinal = ordinal; }
    }
    private final Object lock = new Object();
    private final Reader reader;
    private final DeliveryTiming.ApiObserver delegate;
    private final Supplier<String> window;
    private final boolean enabled;
    private final ThreadLocal<Request> current = new ThreadLocal<>();
    private final List<Request> requests = new ArrayList<>();
    private Candidate active;
    private int queries;
    private int errors;
    private int stale;
    private int lockSamples;
    private int openRequests;
    private long queryNanos;
    private long maxQueryNanos;
    private boolean closed;
    private ScheduledThreadPoolExecutor executor;
    private ScheduledFuture<?> periodic;

    ApiDbObservation(Reader reader, DeliveryTiming.ApiObserver delegate, Supplier<String> window, boolean enabled) {
        this.reader = reader; this.delegate = delegate; this.window = window; this.enabled = enabled;
    }

    static ApiDbObservation connect(Connection connection, DeliveryTiming.ApiObserver delegate,
                                    Supplier<String> window, boolean enabled) throws Exception {
        try {
            connection.setNetworkTimeout(Runnable::run, 2000);
            var jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            jdbc.setQueryTimeout(1);
            Integer consumers = jdbc.queryForObject("SELECT COUNT(*) FROM performance_schema.setup_consumers WHERE NAME='events_statements_current' AND ENABLED='YES'", Integer.class);
            Integer instruments = jdbc.queryForObject("SELECT COUNT(*) FROM performance_schema.setup_instruments WHERE NAME IN ('statement/sql/insert','statement/sql/update','statement/sql/commit') AND ENABLED='YES'", Integer.class);
            if (consumers == null || consumers != 1 || instruments == null || instruments != 3) throw new IllegalStateException("Statement observation unavailable");
            var reader = new Reader() {
                @Override public Observation read(long session) {
                    var rows = jdbc.query("""
                            SELECT e.EVENT_NAME, e.END_EVENT_ID,
                              (SELECT COUNT(*) FROM performance_schema.data_lock_waits w WHERE w.REQUESTING_THREAD_ID=t.THREAD_ID) AS edge_count
                            FROM performance_schema.threads t
                            LEFT JOIN performance_schema.events_statements_current e ON e.THREAD_ID=t.THREAD_ID
                            WHERE t.PROCESSLIST_ID=?
                            """, (rs, row) -> {
                        String name = rs.getString(1);
                        Kind kind = name == null ? Kind.NONE : switch (name) {
                            case "statement/sql/insert" -> Kind.INSERT;
                            case "statement/sql/update" -> Kind.UPDATE;
                            case "statement/sql/commit" -> Kind.COMMIT;
                            default -> Kind.OTHER;
                        };
                        return new Observation(true, name != null && rs.getObject(2) == null, kind, rs.getInt(3));
                    }, session);
                    if (rows.size() > 1) throw new IllegalStateException("Ambiguous physical session");
                    return rows.isEmpty() ? new Observation(false, false, Kind.NONE, 0) : rows.getFirst();
                }
                @Override public void close() throws SQLException { connection.close(); }
            };
            var observation = new ApiDbObservation(reader, delegate, window, enabled);
            if (enabled) observation.start();
            return observation;
        } catch (Exception failure) { connection.close(); throw failure; }
    }

    private void start() {
        executor = new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().daemon(true).factory());
        executor.setRemoveOnCancelPolicy(true);
        periodic = executor.scheduleWithFixedDelay(this::pollOnce, 20, 20, TimeUnit.MILLISECONDS);
    }

    @Override public void starting(Part part) {
        synchronized (lock) {
            if (closed) throw new IllegalStateException("Observation already closed");
            if (part == Part.STORE_CALL) {
                if (current.get() != null || openRequests != 0 || requests.size() >= 5000) throw new IllegalStateException("Observation request bound exceeded");
                String label = window.get();
                if (!List.of("BASELINE", "MIXED", "CONTROL").contains(label)) throw new IllegalArgumentException("Unknown observation window");
                current.set(new Request(label)); openRequests++;
            } else if (part == Part.EXECUTE || part == Part.COMMIT) {
                Request request = requireRequest();
                if (active != null || request.session <= 0) throw new IllegalStateException("Native call observation overlap");
                int ordinal = part == Part.EXECUTE ? ++request.executions : 0;
                if (ordinal > 4) throw new IllegalStateException("Unexpected admission statement count");
                active = new Candidate(request, part, ordinal);
            }
        }
    }

    @Override public void connection(Connection connection) throws SQLException {
        long session = connection.unwrap(JdbcConnection.class).getId(); // Driver's cached ID; no SELECT on the application connection.
        synchronized (lock) {
            var request = requireRequest();
            if (request.session != 0 || session <= 0) throw new IllegalStateException("Invalid physical connection boundary");
            request.session = session;
        }
    }

    @Override public void record(Part part, long nanos, boolean failed) {
        synchronized (lock) {
            Request request = requireRequest(); request.failed |= failed;
            if (part == Part.EXECUTE || part == Part.COMMIT) {
                if (active == null || active.request != request || active.part != part) throw new IllegalStateException("Native call boundary mismatch");
                active = null;
            }
            if (part == Part.STORE_CALL) {
                if (active != null) throw new IllegalStateException("Native call still active");
                requests.add(request); current.remove(); openRequests--;
            }
        }
        delegate.record(part, nanos, failed);
    }

    private Request requireRequest() {
        Request request = current.get();
        if (request == null) throw new IllegalStateException("Missing request observation scope");
        return request;
    }

    Candidate candidate() { synchronized (lock) { return active; } }

    void pollOnce() {
        Candidate candidate;
        synchronized (lock) {
            if (!enabled || closed || active == null || queries >= MAX_QUERIES || errors >= 3) return;
            candidate = active; queries++;
        }
        long start = System.nanoTime();
        try { accept(candidate, reader.read(candidate.request.session)); }
        catch (Exception failure) { synchronized (lock) { errors++; } } // Only a count; never retain a driver message or arguments.
        finally {
            long nanos = System.nanoTime() - start;
            synchronized (lock) { queryNanos += nanos; maxQueryNanos = Math.max(maxQueryNanos, nanos); }
        }
    }

    void accept(Candidate candidate, Observation observation) {
        synchronized (lock) {
            if (closed || active != candidate) { stale++; return; }
            var request = candidate.request;
            if (request.samples.size() >= MAX_SAMPLES_PER_REQUEST) { request.overflow++; return; }
            if (observation.rowWaitEdges() < 0) throw new IllegalArgumentException("Invalid wait count");
            request.samples.add(Map.of("part", candidate.part.name(), "sqlOrdinal", candidate.ordinal,
                    "sessionPresent", observation.sessionPresent(), "serverStatementActive", observation.statementActive(),
                    "statementKind", observation.kind().name(), "rowWaitEdges", observation.rowWaitEdges()));
            if (observation.rowWaitEdges() > 0) lockSamples++;
        }
    }

    boolean hasLockSample() { synchronized (lock) { return lockSamples > 0; } }

    Map<String, Object> snapshot() {
        synchronized (lock) {
            var rows = new ArrayList<Map<String, Object>>();
            for (var request : requests) rows.add(Map.of("window", request.window,
                    "status", !enabled ? "DISABLED" : request.samples.isEmpty() ? "NOT_OBSERVED"
                            : request.samples.stream().anyMatch(s -> Boolean.TRUE.equals(s.get("sessionPresent"))) ? "OBSERVED" : "SESSION_UNAVAILABLE",
                    "failed", request.failed, "samples", List.copyOf(request.samples), "samplesNotRetained", request.overflow));
            var result = new LinkedHashMap<String, Object>();
            result.put("version", "held-session-call-window-v1"); result.put("enabled", enabled); result.put("closed", closed);
            result.put("pollMillis", 20); result.put("maxQueries", MAX_QUERIES); result.put("maxSamplesPerRequest", MAX_SAMPLES_PER_REQUEST);
            result.put("queries", queries); result.put("errors", errors); result.put("staleDiscarded", stale);
            result.put("queryTotalMillis", queryNanos / 1_000_000.0); result.put("queryMaxMillis", maxQueryNanos / 1_000_000.0);
            result.put("samplerTerminated", executor == null || executor.isTerminated());
            result.put("queuedTasks", executor == null ? 0 : executor.getQueue().size());
            result.put("queryBudgetReached", queries >= MAX_QUERIES); result.put("lockSamples", lockSamples);
            result.put("openRequests", openRequests); result.put("rows", rows);
            return result;
        }
    }

    @Override public void close() throws Exception {
        synchronized (lock) { if (closed) return; closed = true; }
        if (periodic != null) periodic.cancel(false);
        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow(); reader.close();
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) throw new IllegalStateException("Observation worker did not terminate");
                return;
            }
        }
        reader.close();
    }
}
