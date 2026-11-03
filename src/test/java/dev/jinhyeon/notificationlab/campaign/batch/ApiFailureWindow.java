package dev.jinhyeon.notificationlab.campaign.batch;

import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Part;

/** Prototype for one sequential HTTP probe: bounded history, a live call slot, and an immutable first-stop snapshot. */
final class ApiFailureWindow implements DeliveryTiming.ApiIoObserver {
    static final int HISTORY = 32;
    enum Window { BASELINE, MIXED, CONTROL }
    enum Stop { HTTP_TIMEOUT, HTTP_IO_FAILURE, HTTP_STATUS, SERVER_FAILURE, PROBE_LIMIT, RUN_LIMIT, INFRA_FAILURE, PROTOCOL_FAILURE }
    private static final class Frame {
        final int ordinal;
        final Window window;
        final long opened;
        final long[] counts = new long[Part.values().length];
        final long[] nanos = new long[counts.length];
        final long[] executions = new long[4];
        final Part[] stack = new Part[2];
        final long[] starts = new long[2];
        final ApiConnectionTiming connection = new ApiConnectionTiming();
        int depth;
        boolean storeReturned;
        boolean failed;
        boolean commitReturned;
        boolean rollbackReturned;
        boolean responseObserved;
        int clientStatus;
        Double clientMillis;
        Frame(int ordinal, Window window, long opened) { this.ordinal = ordinal; this.window = window; this.opened = opened; }
    }
    private final int maxProbes;
    private final LongSupplier clock;
    private final ArrayDeque<Map<String, Object>> history = new ArrayDeque<>();
    private Frame active;
    private Thread owner;
    private int opened;
    private int completed;
    private Stop stop;
    private Map<String, Object> frozen;

    ApiFailureWindow(int maxProbes) { this(maxProbes, System::nanoTime); }
    ApiFailureWindow(int maxProbes, LongSupplier clock) {
        if (maxProbes < 1 || maxProbes > 150_000) throw new IllegalArgumentException("Probe bound must be 1..150000");
        this.maxProbes = maxProbes; this.clock = clock;
    }

    synchronized void open(Window window) {
        if (stop != null || active != null) throw new IllegalStateException("Stopped or overlapping probe");
        if (opened >= maxProbes) { freeze(Stop.PROBE_LIMIT); throw new IllegalStateException("Probe bound reached"); }
        if (window == null) throw new IllegalArgumentException("Missing probe window");
        active = new Frame(++opened, window, clock.getAsLong());
    }

    @Override public synchronized void starting(Part part) {
        if (stop != null) return; // A timed-out server may still finish; never change its execution or the frozen evidence.
        Frame frame = requireActive();
        if (part == Part.STORE_CALL && frame.depth == 0 && owner == null && !frame.storeReturned) owner = Thread.currentThread();
        requireOwner();
        if (frame.storeReturned || frame.depth >= 2 || (frame.depth == 0 && part != Part.STORE_CALL)
                || (frame.depth == 1 && part == Part.STORE_CALL)) throw new IllegalStateException("Unexpected call nesting");
        frame.stack[frame.depth] = part; frame.starts[frame.depth++] = clock.getAsLong();
    }

    @Override public synchronized void record(Part part, long nanos, boolean failed) {
        if (stop != null) return;
        Frame frame = requireActive(); requireOwner();
        if (nanos < 0 || frame.depth == 0 || frame.stack[frame.depth - 1] != part) throw new IllegalStateException("Call boundary mismatch");
        if (part == Part.EXECUTE) {
            if (frame.counts[part.ordinal()] >= 4) throw new IllegalStateException("Admission SQL bound exceeded");
            frame.executions[(int) frame.counts[part.ordinal()]] = nanos;
        }
        frame.depth--; frame.counts[part.ordinal()]++; frame.nanos[part.ordinal()] += nanos; frame.failed |= failed;
        if (part == Part.COMMIT && !failed) frame.commitReturned = true;
        if (part == Part.ROLLBACK && !failed) frame.rollbackReturned = true;
        if (part == Part.STORE_CALL) frame.storeReturned = true;
    }

    @Override public void connection(Connection connection) { /* No unwrap, IDs, or diagnostic JDBC calls. */ }
    @Override public boolean connectionCallsEnabled() { return true; }
    @Override public synchronized void connectionCall(ApiConnectionTiming.Method method, ApiConnectionTiming.Boundary boundary, long nanos, boolean failed) {
        if (stop != null) return;
        Frame frame = requireActive(); requireOwner();
        frame.connection.record(method, boundary, nanos, failed);
    }

    synchronized void complete(int status, double clientMillis) {
        if (stop != null) return;
        Frame frame = requireActive();
        if (status >= 100 && status <= 599) { frame.responseObserved = true; frame.clientStatus = status; }
        if (Double.isFinite(clientMillis) && clientMillis >= 0) frame.clientMillis = clientMillis;
        if (!frame.responseObserved || frame.clientMillis == null) {
            freeze(Stop.PROTOCOL_FAILURE); throw new IllegalStateException("Invalid observed HTTP response");
        }
        if (status != 202) { freeze(Stop.HTTP_STATUS); throw new IllegalStateException("Probe response is not accepted"); }
        try {
            if (!Double.isFinite(clientMillis) || clientMillis < 0 || !frame.storeReturned || frame.depth != 0 || frame.failed) {
                throw new IllegalStateException("Probe not successfully completed");
            }
            for (var part : List.of(Part.STORE_CALL, Part.ACQUIRE, Part.COMMIT, Part.RELEASE)) {
                if (frame.counts[part.ordinal()] != 1) throw new IllegalStateException("Unexpected transaction count");
            }
            if (frame.counts[Part.EXECUTE.ordinal()] != 4 || frame.counts[Part.ROLLBACK.ordinal()] != 0) {
                throw new IllegalStateException("Unexpected SQL or rollback count");
            }
            frame.connection.verifySuccessfulAdmission(frame.counts[Part.AUTOCOMMIT_STATE.ordinal()], frame.nanos[Part.AUTOCOMMIT_STATE.ordinal()]);
        } catch (RuntimeException invalid) { freeze(Stop.PROTOCOL_FAILURE); throw invalid; }
        var row = new LinkedHashMap<>(frame(frame, clock.getAsLong()));
        row.put("responseObserved", true); row.put("clientStatus", status); row.put("clientMillis", clientMillis);
        if (history.size() == HISTORY) history.removeFirst();
        history.addLast(Map.copyOf(row)); completed++; active = null; owner = null;
    }

    synchronized Map<String, Object> freeze(Stop reason) {
        if (frozen == null) {
            if (reason == null) throw new IllegalArgumentException("Missing stop reason");
            stop = reason;
            frozen = state(clock.getAsLong()); owner = null;
        }
        return frozen;
    }

    synchronized Map<String, Object> snapshot() { return frozen == null ? state(clock.getAsLong()) : frozen; }

    private Frame requireActive() {
        if (active == null) throw new IllegalStateException("Missing active probe");
        return active;
    }
    private void requireOwner() {
        if (owner != Thread.currentThread()) throw new IllegalStateException("Unexpected probe server thread");
    }
    private Map<String, Object> state(long now) {
        var result = new LinkedHashMap<String, Object>();
        result.put("version", "bounded-api-failure-window-prototype-v1");
        result.put("historyCapacity", HISTORY); result.put("maxProbes", maxProbes);
        result.put("opened", opened); result.put("completed", completed); result.put("evicted", Math.max(0, completed - HISTORY));
        result.put("stopped", stop != null); result.put("stopReason", stop == null ? "NONE" : stop.name());
        result.put("recent", List.copyOf(history)); result.put("activePresent", active != null);
        result.put("active", active == null ? Map.of() : frame(active, now));
        result.put("activeServerOutcome", active == null ? "NOT_APPLICABLE" : "UNKNOWN");
        return Map.copyOf(result);
    }
    private Map<String, Object> frame(Frame frame, long now) {
        var result = new LinkedHashMap<String, Object>();
        result.put("ordinal", frame.ordinal); result.put("window", frame.window.name());
        result.put("ageMillis", (now - frame.opened) / 1_000_000.0);
        result.put("storeReturned", frame.storeReturned); result.put("failed", frame.failed);
        result.put("commitReturned", frame.commitReturned); result.put("rollbackReturned", frame.rollbackReturned);
        result.put("responseObserved", frame.responseObserved);
        if (frame.responseObserved) result.put("clientStatus", frame.clientStatus);
        if (frame.clientMillis != null) result.put("clientMillis", frame.clientMillis);
        result.put("activePart", frame.depth == 0 ? (frame.storeReturned ? "STORE_RETURNED" : "STORE_NOT_ENTERED") : frame.stack[frame.depth - 1].name());
        result.put("activePartAgeMillis", frame.depth == 0 ? 0.0 : (now - frame.starts[frame.depth - 1]) / 1_000_000.0);
        var parts = new LinkedHashMap<String, Object>();
        for (var part : Part.values()) parts.put(part.name(), Map.of("count", frame.counts[part.ordinal()], "millis", frame.nanos[part.ordinal()] / 1_000_000.0));
        result.put("parts", Map.copyOf(parts));
        result.put("sqlExecutionMillis", Arrays.stream(frame.executions).mapToDouble(n -> n / 1_000_000.0).boxed().toList());
        result.put("connectionCalls", frame.connection.snapshot());
        return Map.copyOf(result);
    }
}
