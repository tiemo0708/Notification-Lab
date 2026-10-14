package dev.jinhyeon.notificationlab.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import dev.jinhyeon.notificationlab.config.ProviderProperties;
import dev.jinhyeon.notificationmock.MockPushEndpoint;
import dev.jinhyeon.notificationmock.MockServerApplication;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class HttpPushProviderTest {
    private ServletWebServerApplicationContext mock;
    private HttpPushProvider provider;
    private HttpServer raw;
    private final PushRequest request = new PushRequest(UUID.randomUUID(), "synthetic-user", "가상 제목", "가상 본문");

    @ParameterizedTest
    @CsvSource({"SUCCESS,ACCEPTED,", "TOO_MANY_REQUESTS,RETRYABLE_FAILURE,HTTP_STATUS_429",
            "SERVER_ERROR,RETRYABLE_FAILURE,HTTP_STATUS_500", "INVALID_RECIPIENT,PERMANENT_FAILURE,HTTP_STATUS_400"})
    void mapsHttpResultsWithoutHiddenRetries(String mode, PushResult.Outcome outcome, String code) {
        int port = startMock(mode, 0);
        provider = new HttpPushProvider(settings(port, 1, 1, 200, 1000, 2000), new ObjectMapper());
        var result = provider.send(request);
        assertThat(result.outcome()).isEqualTo(outcome);
        assertThat(result.errorCode()).isEqualTo(code);
        assertThat(stats().requests()).isEqualTo(1);
        if (outcome == PushResult.Outcome.ACCEPTED) assertThat(result.providerMessageId()).startsWith("http-mock-");
        assertThat(provider.poolStats().getLeased()).isZero();
        assertThat(provider.pendingDeadlines()).isZero();
    }

    @Test
    void responseTimeoutKeepsAcceptanceUncertainAndReleasesPool() {
        int port = startMock("SUCCESS", 600);
        provider = new HttpPushProvider(settings(port, 1, 0, 100, 150, 1000), new ObjectMapper());
        long began = System.nanoTime();
        assertThat(provider.send(request).errorCode()).isEqualTo("HTTP_UNKNOWN_TIMEOUT");
        assertThat(Duration.ofNanos(System.nanoTime() - began)).isLessThan(Duration.ofSeconds(2));
        await().atMost(Duration.ofSeconds(3)).until(() -> stats().active() == 0);
        assertThat(stats().accepted()).isEqualTo(1);
        assertThat(provider.poolStats().getLeased()).isZero();
        // A second explicit call can be accepted again; the client does not promise idempotency.
        assertThat(provider.send(request).errorCode()).isEqualTo("HTTP_UNKNOWN_TIMEOUT");
        assertThat(stats().accepted()).isEqualTo(2);
    }

    @Test
    void poolWaitAndAdmissionAreBoundedAndConnectionIsReusable() throws Exception {
        int port = startMock("SUCCESS", 700);
        provider = new HttpPushProvider(settings(port, 1, 1, 200, 2000, 3000), new ObjectMapper());
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> provider.send(request));
            await().atMost(Duration.ofSeconds(2)).until(() -> stats().active() == 1);
            var waiting = executor.submit(() -> provider.send(request));
            await().atMost(Duration.ofSeconds(2)).until(() -> provider.poolStats().getPending() == 1);
            assertThat(provider.send(request).errorCode()).isEqualTo("HTTP_CLIENT_BUSY");
            assertThat(waiting.get(2, TimeUnit.SECONDS).errorCode()).isEqualTo("HTTP_POOL_TIMEOUT");
            assertThat(first.get(3, TimeUnit.SECONDS).outcome()).isEqualTo(PushResult.Outcome.ACCEPTED);
            assertThat(stats().requests()).isEqualTo(1);
            assertThat(provider.poolStats().getAvailable()).isEqualTo(1);
            assertThat(provider.poolStats().getPending()).isZero();
            assertThat(provider.pendingDeadlines()).isZero();
            assertThat(provider.send(request).outcome()).isEqualTo(PushResult.Outcome.ACCEPTED);
            assertThat(stats().maxActive()).isEqualTo(1);
        }
    }

    @Test
    void overallDeadlineCancelsSlowDrippingBodyEvenWhileReadsMakeProgress() throws Exception {
        raw = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        raw.createContext("/pushes", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(202, 0);
            try (var output = exchange.getResponseBody()) {
                for (int i = 0; i < 30; i++) {
                    output.write(' '); output.flush();
                    try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                }
            } catch (java.io.IOException ignored) { }
        });
        raw.start();
        provider = new HttpPushProvider(settings(raw.getAddress().getPort(), 1, 0, 100, 500, 600), new ObjectMapper());
        long began = System.nanoTime();
        assertThat(provider.send(request).errorCode()).isEqualTo("HTTP_UNKNOWN_DEADLINE");
        assertThat(Duration.ofNanos(System.nanoTime() - began)).isLessThan(Duration.ofSeconds(2));
        assertThat(provider.poolStats().getLeased()).isZero();
        assertThat(provider.pendingDeadlines()).isZero();
    }

    @ParameterizedTest
    @CsvSource({"302,redirect", "202,invalid", "202,oversized", "202,sensitive", "408,timeout"})
    void refusesRedirectsAndUntrustedOrUnboundedResponses(int status, String mode) throws Exception {
        var calls = new AtomicInteger();
        raw = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        raw.createContext("/pushes", exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Location", "/pushes");
            String body = switch (mode) {
                case "oversized" -> "x".repeat(16384);
                case "sensitive" -> "{\"providerMessageId\":\"sensitive-response\"}";
                default -> "untrusted-response";
            };
            exchange.sendResponseHeaders(status, body.length());
            try (var output = exchange.getResponseBody()) { output.write(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
            catch (java.io.IOException ignored) { }
        });
        raw.start();
        provider = new HttpPushProvider(settings(raw.getAddress().getPort(), 1, 0, 100, 500, 1000), new ObjectMapper());
        var result = provider.send(request);
        assertThat(result.outcome()).isEqualTo(PushResult.Outcome.RETRYABLE_FAILURE);
        assertThat(result.providerMessageId()).isNull();
        assertThat(result.errorCode()).startsWith("HTTP_UNKNOWN_").doesNotContain("sensitive", "untrusted");
        assertThat(calls.get()).isEqualTo(1);
        assertThat(provider.poolStats().getLeased()).isZero();
    }

    @Test
    void connectionFailureAndShutdownReleaseResourcesWithoutSendingAgain() throws Exception {
        int port;
        try (var unused = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) { port = unused.getLocalPort(); }
        provider = new HttpPushProvider(settings(port, 1, 0, 100, 500, 1000), new ObjectMapper());
        assertThat(provider.send(request).errorCode()).isEqualTo("HTTP_UNKNOWN_IO");
        assertThat(provider.poolStats().getLeased()).isZero();
        provider.close();
        assertThat(provider.send(request).errorCode()).isEqualTo("HTTP_CLIENT_CLOSED");
        assertThat(provider.pendingDeadlines()).isZero();
    }

    @Test
    void closingClientCancelsAnInflightRequestAndClearsDeadlineTasks() throws Exception {
        int port = startMock("SUCCESS", 2000);
        provider = new HttpPushProvider(settings(port, 1, 0, 100, 3000, 4000), new ObjectMapper());
        try (var executor = Executors.newSingleThreadExecutor()) {
            var pending = executor.submit(() -> provider.send(request));
            await().atMost(Duration.ofSeconds(2)).until(() -> stats().active() == 1);
            provider.close();
            assertThat(pending.get(2, TimeUnit.SECONDS).outcome()).isEqualTo(PushResult.Outcome.RETRYABLE_FAILURE);
            assertThat(provider.pendingDeadlines()).isZero();
            assertThat(stats().accepted()).isEqualTo(1);
        }
    }

    @Test
    void interruptBeforeCallDoesNotSendOrClearInterrupt() {
        int port = startMock("SUCCESS", 0);
        provider = new HttpPushProvider(settings(port, 1, 0, 100, 500, 1000), new ObjectMapper());
        Thread.currentThread().interrupt();
        try {
            assertThat(provider.send(request).errorCode()).isEqualTo("HTTP_INTERRUPTED");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(stats().requests()).isZero();
        } finally { Thread.interrupted(); }
    }

    @Test
    void rejectsExternalOriginsAndUnboundedSettings() {
        for (String url : new String[]{"https://127.0.0.1:18081", "http://example.com:80", "http://127.0.0.1:18081/path",
                "http://user:pass@127.0.0.1:18081", "http://127.0.0.1", "http://127.0.0.1:18081?key=x"}) {
            assertThatThrownBy(() -> new ProviderProperties.Http(URI.create(url), 1, 1, 100, 100, 500, 1000))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> settings(18081, 9, 0, 100, 500, 1000)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings(18081, 1, 17, 100, 500, 1000)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings(18081, 1, 0, 0, 500, 1000)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings(18081, 1, 0, 100, 500, 10001)).isInstanceOf(IllegalArgumentException.class);
    }

    private int startMock(String mode, int delay) {
        mock = MockServerApplication.start("--server.port=0", "--mock.http.mode=" + mode,
                "--mock.http.response-delay-ms=" + delay, "--logging.level.root=WARN", "--spring.main.banner-mode=off");
        return mock.getWebServer().getPort();
    }

    private MockPushEndpoint.Stats stats() { return mock.getBean(MockPushEndpoint.class).stats(); }

    private ProviderProperties.Http settings(int port, int connections, int pending, int acquire, int response, int deadline) {
        return new ProviderProperties.Http(URI.create("http://127.0.0.1:" + port), connections, pending, acquire, 100, response, deadline);
    }

    @AfterEach
    void close() {
        if (provider != null) provider.close();
        if (mock != null) mock.close();
        if (raw != null) raw.stop(0);
    }
}
