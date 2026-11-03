package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.outbox.SubmissionService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

/** Single sequential probe in flight. Never reads HTTP content, SQL, arguments or record IDs. */
final class ApiRequestTiming {
    enum Window { BASELINE, MIXED }
    @FunctionalInterface interface ServerAction { void run() throws IOException, ServletException; }
    private final int capacity;
    private final boolean connectionCalls;
    private final AtomicReference<Capture> pending = new AtomicReference<>();
    private final ThreadLocal<Capture> serving = new ThreadLocal<>();
    private final List<Map<String, Object>> rows = new ArrayList<>();
    private final long originNanos = System.nanoTime();
    private double drainStartAfterMillis = -1;

    ApiRequestTiming(int capacity) {
        this(capacity, false);
    }

    ApiRequestTiming(int capacity, boolean connectionCalls) {
        if (capacity < 1 || capacity > 5000) throw new IllegalArgumentException("API timing capacity must be 1..5000");
        this.capacity = capacity;
        this.connectionCalls = connectionCalls;
    }

    static final class Capture {
        final Window window;
        final AtomicBoolean claimed = new AtomicBoolean();
        final CountDownLatch finished = new CountDownLatch(1);
        final long[] nanos = new long[DeliveryTiming.Part.values().length];
        final long[] counts = new long[nanos.length];
        final long[] executions = new long[4];
        final ApiConnectionTiming connection = new ApiConnectionTiming();
        long startedNanos;
        long serverNanos;
        boolean failed;
        Capture(Window window) { this.window = window; }
    }

    Capture open(Window window) {
        if (rows.size() >= capacity) throw new IllegalStateException("API timing capacity exceeded");
        var capture = new Capture(window);
        if (!pending.compareAndSet(null, capture)) throw new IllegalStateException("Only one API probe may be pending");
        return capture;
    }

    void serve(ServerAction action) throws IOException, ServletException {
        Capture capture = pending.get();
        if (capture == null || !capture.claimed.compareAndSet(false, true) || serving.get() != null) {
            throw new IllegalStateException("Unexpected or overlapping API probe");
        }
        serving.set(capture);
        long start = System.nanoTime();
        capture.startedNanos = start;
        boolean failed = true;
        try { action.run(); failed = false; }
        finally {
            capture.serverNanos = System.nanoTime() - start;
            capture.failed |= failed;
            serving.remove();
            capture.finished.countDown();
        }
    }

    void jdbc(DeliveryTiming.Part part, long nanos, boolean failed) {
        Capture capture = serving.get();
        if (capture == null) throw new IllegalStateException("API JDBC scope missing its servlet probe");
        if (part == DeliveryTiming.Part.EXECUTE && capture.counts[part.ordinal()] < capture.executions.length) {
            capture.executions[(int) capture.counts[part.ordinal()]] = nanos;
        }
        capture.nanos[part.ordinal()] += nanos;
        capture.counts[part.ordinal()]++;
        capture.failed |= failed;
    }

    DeliveryTiming.ApiObserver observer() {
        return new DeliveryTiming.ApiObserver() {
            @Override public void record(DeliveryTiming.Part part, long nanos, boolean failed) { jdbc(part, nanos, failed); }
            @Override public boolean connectionCallsEnabled() { return connectionCalls; }
            @Override public void connectionCall(ApiConnectionTiming.Method method, ApiConnectionTiming.Boundary boundary, long nanos, boolean failed) {
                Capture capture = serving.get();
                if (!connectionCalls || capture == null) throw new IllegalStateException("Missing connection call probe");
                capture.connection.record(method, boundary, nanos, failed);
            }
        };
    }

    void complete(Capture capture, double clientMillis, int status) throws InterruptedException {
        if (pending.get() != capture) throw new IllegalStateException("API probe does not match");
        try {
            if (!capture.finished.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("API filter completion not observed");
            if (!Double.isFinite(clientMillis) || clientMillis < 0) throw new IllegalArgumentException("Invalid API duration");
            if (status != 202 || capture.failed) throw new IllegalStateException("API probe did not complete successfully");
            for (var part : List.of(DeliveryTiming.Part.STORE_CALL, DeliveryTiming.Part.ACQUIRE,
                    DeliveryTiming.Part.COMMIT, DeliveryTiming.Part.RELEASE)) {
                if (capture.counts[part.ordinal()] != 1) throw new IllegalStateException("Unexpected API transaction boundary count");
            }
            if (capture.counts[DeliveryTiming.Part.EXECUTE.ordinal()] != 4
                    || capture.counts[DeliveryTiming.Part.ROLLBACK.ordinal()] != 0) {
                throw new IllegalStateException("Unexpected API SQL or rollback count");
            }
            var row = new LinkedHashMap<String, Object>();
            row.put("window", capture.window.name()); row.put("clientMillis", clientMillis);
            row.put("startAfterMillis", (capture.startedNanos - originNanos) / 1_000_000.0);
            row.put("serverMillis", capture.serverNanos / 1_000_000.0);
            row.put("sqlExecutionMillis", java.util.Arrays.stream(capture.executions).mapToDouble(n -> n / 1_000_000.0).boxed().toList());
            var parts = new LinkedHashMap<String, Object>();
            for (var part : DeliveryTiming.Part.values()) {
                parts.put(part.name(), Map.of("count", capture.counts[part.ordinal()], "millis", capture.nanos[part.ordinal()] / 1_000_000.0));
            }
            row.put("parts", parts);
            if (connectionCalls) {
                capture.connection.verifySuccessfulAdmission(capture.counts[DeliveryTiming.Part.AUTOCOMMIT_STATE.ordinal()],
                        capture.nanos[DeliveryTiming.Part.AUTOCOMMIT_STATE.ordinal()]);
                row.put("connectionCalls", capture.connection.snapshot());
            }
            rows.add(row);
        } finally { pending.compareAndSet(capture, null); }
    }

    void abort(Capture capture) { pending.compareAndSet(capture, null); }

    String currentWindow() {
        Capture capture = serving.get();
        if (capture == null) throw new IllegalStateException("No servlet timing scope");
        return capture.window.name();
    }

    void markDrainStart(long nanos) { drainStartAfterMillis = (nanos - originNanos) / 1_000_000.0; }

    Map<String, Object> snapshot() {
        return Map.of("version", connectionCalls ? "paired-api-jdbc-v2" : "paired-api-jdbc-v1", "capacity", capacity, "completed", rows.size(),
                "pending", pending.get() != null, "drainStartAfterMillis", drainStartAfterMillis, "rows", List.copyOf(rows));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Wiring {
        @Bean SmartInitializingSingleton apiSubmissionTiming(DeliveryTiming timing, SubmissionService submission) {
            return () -> timing.attachSubmission(submission);
        }
        @Bean FilterRegistrationBean<OncePerRequestFilter> apiTimingFilter(ApiRequestTiming timing) {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>(new OncePerRequestFilter() {
                @Override protected boolean shouldNotFilter(HttpServletRequest request) {
                    return !request.getMethod().equals("POST") || !request.getRequestURI().equals("/notifications");
                }
                @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                                         FilterChain chain) throws ServletException, IOException {
                    timing.serve(() -> chain.doFilter(request, response));
                }
            });
            registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
            return registration;
        }
    }
}
