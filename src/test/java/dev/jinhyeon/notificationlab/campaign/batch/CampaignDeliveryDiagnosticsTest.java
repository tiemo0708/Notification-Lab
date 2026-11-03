package dev.jinhyeon.notificationlab.campaign.batch;

import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CampaignDeliveryDiagnosticsTest {
    private final CountDownLatch releaseResponse = new CountDownLatch(1);
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1), Thread.ofPlatform().daemon(true).factory());
    private HttpServer server;

    @Test
    void httpTimeoutFreezesTheSyntheticActiveCallAndLateResponseCannotReplaceTheEvidence() throws Exception {
        var window = new ApiFailureWindow(2);
        var armed = new AtomicBoolean();
        var requests = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var lateCompleted = new CountDownLatch(1);
        var lateReturned = new AtomicBoolean();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 1);
        server.setExecutor(executor);
        server.createContext("/notifications", exchange -> {
            boolean capture = armed.get(); requests.incrementAndGet();
            try {
                exchange.getRequestBody().readAllBytes();
                if (capture) {
                    window.starting(DeliveryTiming.Part.STORE_CALL);
                    window.starting(DeliveryTiming.Part.COMMIT); // Synthetic marker only: this HTTP fixture has no database.
                    entered.countDown();
                    if (!releaseResponse.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Test release timed out");
                    window.record(DeliveryTiming.Part.COMMIT, 1, false);
                    window.record(DeliveryTiming.Part.STORE_CALL, 1, false);
                    window.complete(202, 1); lateReturned.set(true);
                }
                byte[] body = "synthetic-private-response".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(202, body.length); exchange.getResponseBody().write(body);
            } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); if (capture) lateCompleted.countDown(); }
        });
        server.start();
        var uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/notifications");
        var report = new LinkedHashMap<String, Object>();
        try (var client = HttpClient.newHttpClient()) {
            assertThat(client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(202); // Warm up the transport before the timed fixture.
            window.open(ApiFailureWindow.Window.MIXED); armed.set(true);
            var request = HttpRequest.newBuilder(uri).timeout(Duration.ofMillis(500))
                    .POST(HttpRequest.BodyPublishers.ofString("synthetic-private-body")).build();
            try {
                assertThatThrownBy(() -> CampaignDeliveryDiagnostics.sendProbe(client, request, report, "MIXED", 1, 0))
                        .isInstanceOf(HttpTimeoutException.class);
                assertThat(entered.getCount()).isZero();
                var frozen = window.freeze(ApiFailureWindow.Stop.HTTP_TIMEOUT);
                assertThat(frozen).containsEntry("activeServerOutcome", "UNKNOWN").containsEntry("completed", 0);
                @SuppressWarnings("unchecked") var active = (Map<String, Object>) frozen.get("active");
                assertThat(active).containsEntry("activePart", "COMMIT").containsEntry("commitReturned", false);
                releaseResponse.countDown();
                assertThat(lateCompleted.await(2, TimeUnit.SECONDS)).isTrue(); assertThat(lateReturned).isTrue();
                assertThat(window.snapshot()).isSameAs(frozen); assertThat(requests.get()).isEqualTo(2);
                assertThat(frozen.toString() + report).doesNotContain(uri.toString(), "synthetic-private-response", "synthetic-private-body");
            } finally { releaseResponse.countDown(); }
        }
    }

    @Test
    void successfulProbeReturnsResponseWithoutInventingFailure() throws Exception {
        URI uri = server(false);
        var report = new LinkedHashMap<String, Object>();
        try (var client = HttpClient.newHttpClient()) {
            var response = CampaignDeliveryDiagnostics.sendProbe(client,
                    HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).GET().build(), report, "BASELINE", 1, 0);
            assertThat(response.statusCode()).isEqualTo(202);
            assertThat(response.body()).isEqualTo("synthetic acceptance");
            assertThat(report).isEmpty();
        }
    }

    @Test
    void timeoutRemainsUnknownAndRecordsOnlyBoundedNonSensitiveMetadata() throws Exception {
        URI uri = server(true);
        var report = new LinkedHashMap<String, Object>();
        try (var client = HttpClient.newHttpClient()) {
            try {
                var request = HttpRequest.newBuilder(uri).timeout(Duration.ofMillis(150))
                        .POST(HttpRequest.BodyPublishers.ofString("synthetic-private-body")).build();
                assertThatThrownBy(() -> CampaignDeliveryDiagnostics.sendProbe(client, request, report, "MIXED", 7, 6))
                        .isInstanceOf(HttpTimeoutException.class);
                assertThat(report.get("probeFailure")).isEqualTo(Map.of("phase", "MIXED", "ordinal", 7,
                        "acceptedResponses", 6, "timeoutMillis", 150L, "responseObserved", false, "serverOutcome", "UNKNOWN"));
                assertThat(report.toString()).doesNotContain(uri.toString(), "synthetic-private-body");
            } finally { releaseResponse.countDown(); }
        }
    }

    @Test
    void unavailablePoolAndMetersAreExplicitWithoutExposingConfiguration() {
        var registry = new SimpleMeterRegistry();
        try (var source = new HikariDataSource();
             var app = new GenericApplicationContext()) {
            source.setUsername("synthetic-private-user");
            source.setPassword("synthetic-private-password");
            app.getBeanFactory().registerSingleton("registry", registry);
            app.getBeanFactory().registerSingleton("dataSource", source);
            app.refresh();
            Map<String, Object> state = CampaignDeliveryDiagnostics.application(app);
            assertThat(state.get("hikari")).isEqualTo(Map.of("available", false));
            assertThat(state).containsEntry("notification.consumer.lag", null).containsEntry("listenerRunning", null);
            assertThat(state.toString()).doesNotContain("synthetic-private-user", "synthetic-private-password");
            assertThat(app.getBean(DataSource.class)).isSameAs(source);
        } finally { registry.close(); }
    }

    @Test
    void cgroupEvidencePreservesOomAndThrottlingCountersAndRejectsMissingValues() {
        var counters = CampaignDeliveryDiagnostics.cgroupCounters("max 42\noom 1\noom_kill 0\nusage_usec 900\nnr_throttled 3\n");
        assertThat(counters).containsEntry("oom", 1L).containsEntry("oom_kill", 0L).containsEntry("nr_throttled", 3L);
        assertThatThrownBy(() -> CampaignDeliveryDiagnostics.cgroupCounters("max 42\n"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CampaignDeliveryDiagnostics.cgroupCounters("oom_kill 0\nusage_usec NaN\n"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private URI server(boolean hold) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 1);
        server.setExecutor(executor);
        server.createContext("/notifications", exchange -> {
            try {
                exchange.getRequestBody().readAllBytes();
                if (hold) releaseResponse.await(5, TimeUnit.SECONDS);
                byte[] body = "synthetic acceptance".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(202, body.length);
                exchange.getResponseBody().write(body);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally { exchange.close(); }
        });
        server.start();
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/notifications");
    }

    @AfterEach
    void close() throws Exception {
        releaseResponse.countDown();
        if (server != null) server.stop(0);
        executor.shutdownNow();
        assertThat(executor.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
    }
}
