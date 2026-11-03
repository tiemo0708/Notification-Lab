package dev.jinhyeon.notificationlab.campaign.batch;

import com.zaxxer.hikari.HikariDataSource;
import dev.jinhyeon.notificationlab.outbox.SubmissionService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.http.HttpTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.web.filter.OncePerRequestFilter;

/** Opt-in admission observation adapter; fault injection is restricted by the run profile. No production wiring, request contents, identifiers or diagnostic database queries. */
final class ApiFailurePilotSupport {
    enum Mode { NONE, TIMEOUT, STORAGE_FAILURE }
    static final String EARLIER_EVIDENCE = "earlier synthetic evidence retained";
    final ApiFailureWindow window;
    private final Mode mode;
    private final Path directory;
    private final Path file;
    private final FailureSnapshotStore store;
    private final ThreadLocal<Gate> requestGate = new ThreadLocal<>();
    private volatile Gate pending;
    private boolean injected;
    private boolean captured;
    private volatile HikariDataSource pool;

    ApiFailurePilotSupport(int maxProbes, Mode mode, Path directory) throws IOException {
        this.window = new ApiFailureWindow(maxProbes); this.mode = mode; this.directory = directory;
        Files.createDirectories(directory);
        file = directory.resolve("failure.json");
        if (mode == Mode.STORAGE_FAILURE) Files.writeString(file, EARLIER_EVIDENCE, java.nio.file.StandardOpenOption.CREATE_NEW);
        store = new FailureSnapshotStore(window, file);
    }
    static ApiFailurePilotSupport create(int maxProbes, Mode mode) throws IOException {
        return new ApiFailurePilotSupport(maxProbes, mode,
                Files.createTempDirectory(Files.createDirectories(Path.of("build/reports/api-failure-pilot").toAbsolutePath()), "run-"));
    }
    Path directory() { return directory; }
    void open(String phase) {
        window.open(ApiFailureWindow.Window.valueOf(phase));
        if (mode != Mode.NONE && phase.equals("MIXED") && !injected) { pending = new Gate(); injected = true; }
    }
    void release() { if (pending != null) pending.release.countDown(); }

    boolean capture(Throwable original, ApiFailureWindow.Stop reason, Map<String, Object> report) {
        if (captured) return false;
        captured = true;
        store.preserve(original, reason, report);
        var state = new LinkedHashMap<String, Object>();
        state.put("poolAvailable", pool != null);
        if (pool != null) state.put("poolClosed", pool.isClosed());
        state.put("gateReached", pending != null && pending.reached.getCount() == 0);
        state.put("gateReleased", pending != null && pending.release.getCount() == 0);
        state.put("gateAwaitActive", pending != null && pending.waiting.get());
        report.put("failureCaptureLive", Map.copyOf(state));
        return true;
    }
    static ApiFailureWindow.Stop classify(Throwable failure) {
        if (failure instanceof HttpTimeoutException) return ApiFailureWindow.Stop.HTTP_TIMEOUT;
        if (failure instanceof IOException) return ApiFailureWindow.Stop.HTTP_IO_FAILURE;
        if (failure instanceof InterruptedException) return ApiFailureWindow.Stop.RUN_LIMIT;
        if (failure instanceof AssertionError) return ApiFailureWindow.Stop.PROTOCOL_FAILURE;
        return ApiFailureWindow.Stop.SERVER_FAILURE;
    }
    Map<String, Object> control() {
        var state = new LinkedHashMap<String, Object>();
        state.put("mode", mode.name()); state.put("injected", injected); state.put("captured", captured);
        state.put("gateReached", pending != null && pending.reached.getCount() == 0);
        state.put("gateReleased", pending != null && pending.release.getCount() == 0);
        state.put("gateAwaitActive", pending != null && pending.waiting.get());
        state.put("fileExists", Files.exists(file));
        if (mode == Mode.STORAGE_FAILURE) {
            try { state.put("earlierEvidenceUnchanged", Files.readString(file).equals(EARLIER_EVIDENCE)); }
            catch (IOException unavailable) { state.put("earlierEvidenceReadErrorType", unavailable.getClass().getSimpleName()); }
        }
        return Map.copyOf(state);
    }
    DataSource dataSource(DataSource source) {
        if (source instanceof HikariDataSource hikari) pool = hikari;
        return new ControlledDataSource(source);
    }
    private final class ControlledDataSource extends DelegatingDataSource implements AutoCloseable {
        ControlledDataSource(DataSource source) { super(source); }
        @Override public Connection getConnection() throws SQLException { return wrap(obtainTargetDataSource().getConnection()); }
        @Override public Connection getConnection(String username, String password) throws SQLException { return wrap(obtainTargetDataSource().getConnection(username, password)); }
        private Connection wrap(Connection target) {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                if (method.getDeclaringClass() == Object.class) return switch (method.getName()) {
                    case "equals" -> proxy == args[0]; case "hashCode" -> System.identityHashCode(proxy); default -> "Controlled pilot connection";
                };
                Object value;
                try { value = method.invoke(target, args); } catch (InvocationTargetException failure) { throw failure.getCause(); }
                var gate = requestGate.get();
                if (gate != null && method.getName().equals("setAutoCommit") && Boolean.TRUE.equals(args[0]) && gate.claimed.compareAndSet(false, true)) gate.pause();
                return value;
            });
        }
        @Override public void close() throws Exception { if (obtainTargetDataSource() instanceof AutoCloseable closeable) closeable.close(); }
    }
    private static final class Gate {
        final AtomicBoolean claimed = new AtomicBoolean();
        final AtomicBoolean waiting = new AtomicBoolean();
        final CountDownLatch reached = new CountDownLatch(1), release = new CountDownLatch(1);
        void pause() throws SQLException {
            waiting.set(true); reached.countDown();
            try { if (!release.await(8, TimeUnit.SECONDS)) throw new SQLException("Pilot control release timed out"); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new SQLException("Pilot control interrupted"); }
            finally { waiting.set(false); }
        }
    }
    @TestConfiguration(proxyBeanMethods = false)
    static class Wiring {
        @Bean SmartInitializingSingleton failureSubmissionTiming(DeliveryTiming timing, SubmissionService submission) { return () -> timing.attachSubmission(submission); }
        @Bean FilterRegistrationBean<OncePerRequestFilter> failureRequestScope(ApiFailurePilotSupport support) {
            FilterRegistrationBean<OncePerRequestFilter> registration = new FilterRegistrationBean<>(new OncePerRequestFilter() {
                @Override protected boolean shouldNotFilter(HttpServletRequest request) { return !request.getMethod().equals("POST") || !request.getRequestURI().equals("/notifications"); }
                @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException {
                    support.requestGate.set(support.pending);
                    try { chain.doFilter(request, response); } finally { support.requestGate.remove(); }
                }
            });
            registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10); return registration;
        }
    }
}
