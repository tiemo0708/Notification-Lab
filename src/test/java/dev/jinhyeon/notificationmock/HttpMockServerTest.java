package dev.jinhyeon.notificationmock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class HttpMockServerTest {
    private ServletWebServerApplicationContext app;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String payload = """
            {"notificationId":"%s","recipientId":"synthetic-user","title":"가상 제목","body":"가상 본문"}
            """.formatted(UUID.randomUUID());

    @Test
    void runsWithoutDatabaseOrKafkaAndAcceptsEachCallIndependently() throws Exception {
        start();
        assertThat(get("/actuator/health").path("status").asText()).isEqualTo("UP");
        assertThat(app.containsBean("dataSource")).isFalse();
        assertThat(app.containsBean("kafkaTemplate")).isFalse();
        var first = send(payload, Duration.ofSeconds(3));
        var second = send(payload, Duration.ofSeconds(3));
        assertThat(first.statusCode()).isEqualTo(202);
        assertThat(second.statusCode()).isEqualTo(202);
        assertThat(mapper.readTree(first.body()).path("providerMessageId").asText()).startsWith("http-mock-")
                .isNotEqualTo(mapper.readTree(second.body()).path("providerMessageId").asText());
        assertThat(first.body()).doesNotContain("synthetic-user", "가상 본문");
        assertThat(get("/mock/stats").path("accepted").asInt()).isEqualTo(2);
    }

    @ParameterizedTest
    @CsvSource({"TOO_MANY_REQUESTS,429,MOCK_RATE_LIMITED,rateLimited",
            "SERVER_ERROR,500,MOCK_UNAVAILABLE,serverErrors",
            "INVALID_RECIPIENT,400,MOCK_INVALID_RECIPIENT,permanentFailures"})
    void exposesConfiguredFailure(String mode, int status, String code, String counter) throws Exception {
        start("--mock.http.mode=" + mode);
        var response = send(payload, Duration.ofSeconds(3));
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(mapper.readTree(response.body()).path("errorCode").asText()).isEqualTo(code);
        if (status == 429) assertThat(response.headers().firstValue("Retry-After")).contains("1");
        assertThat(get("/mock/stats").path(counter).asInt()).isEqualTo(1);
        assertThat(get("/mock/stats").path("accepted").asInt()).isZero();
    }

    @Test
    void clientTimeoutDoesNotUndoAlreadyRecordedAcceptance() throws Exception {
        start("--mock.http.response-delay-ms=800");
        get("/actuator/health");
        assertThatThrownBy(() -> send(payload, Duration.ofMillis(200))).isInstanceOf(HttpTimeoutException.class);
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            var stats = get("/mock/stats");
            assertThat(stats.path("accepted").asInt()).isEqualTo(1);
            assertThat(stats.path("active").asInt()).isZero();
        });
        assertThat(get("/mock/stats").path("requests").asInt()).isEqualTo(1);
    }

    @Test
    void concurrentRequestsShareOneFiniteQuota() throws Exception {
        start("--mock.http.requests-per-window=3", "--mock.http.window-ms=60000");
        var responses = burst(20);
        assertThat(responses.stream().filter(r -> r.statusCode() == 202).count()).isEqualTo(3);
        assertThat(responses.stream().filter(r -> r.statusCode() == 429).count()).isEqualTo(17);
        assertThat(responses.stream().filter(r -> r.statusCode() == 429))
                .allSatisfy(r -> assertThat(Integer.parseInt(r.headers().firstValue("Retry-After").orElseThrow())).isBetween(1, 60));
        var stats = get("/mock/stats");
        assertThat(stats.path("requests").asInt()).isEqualTo(20);
        assertThat(stats.path("accepted").asInt()).isEqualTo(3);
        assertThat(stats.path("rateLimited").asInt()).isEqualTo(17);
    }

    @Test
    void delayedRequestsHaveBoundedConcurrencyAndRejectExcess() throws Exception {
        start("--mock.http.max-in-flight=2", "--mock.http.response-delay-ms=500");
        var responses = burst(20);
        assertThat(responses).allSatisfy(r -> assertThat(r.statusCode()).isIn(202, 503));
        long accepted = responses.stream().filter(r -> r.statusCode() == 202).count();
        long rejected = responses.stream().filter(r -> r.statusCode() == 503).count();
        assertThat(accepted).isGreaterThanOrEqualTo(2);
        assertThat(rejected).isPositive();
        var stats = get("/mock/stats");
        assertThat(stats.path("maxActive").asInt()).isEqualTo(2);
        assertThat(stats.path("active").asInt()).isZero();
        assertThat(stats.path("accepted").asLong()).isEqualTo(accepted);
        assertThat(stats.path("busy").asLong()).isEqualTo(rejected);
    }

    @Test
    void rejectsInvalidAndOversizedBodiesWithoutConsumingQuota() throws Exception {
        start("--mock.http.requests-per-window=1", "--mock.http.window-ms=60000");
        for (String invalid : List.of("null", "{}", "{", payload + " true", payload.replace("synthetic-user", ""))) {
            var response = send(invalid, Duration.ofSeconds(3));
            assertThat(response.statusCode()).isEqualTo(400);
            assertThat(response.body()).doesNotContain("synthetic-user", "가상 본문");
        }
        assertThat(send("x".repeat(8193), Duration.ofSeconds(3)).statusCode()).isEqualTo(413);
        var chunked = HttpRequest.newBuilder(uri("/pushes")).timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new java.io.ByteArrayInputStream(new byte[8193]))).build();
        assertThat(http.send(chunked, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(413);
        assertThat(get("/mock/stats").path("requests").asInt()).isZero();
        assertThat(send(payload, Duration.ofSeconds(3)).statusCode()).isEqualTo(202);
    }

    @Test
    void rejectsUnboundedOrInvalidConfiguration() {
        for (String setting : List.of("--mock.http.response-delay-ms=10001", "--mock.http.requests-per-window=0",
                "--mock.http.window-ms=0", "--mock.http.max-in-flight=9", "--mock.http.mode=UNKNOWN")) {
            assertThatThrownBy(() -> start(setting)).isInstanceOf(Exception.class);
        }
    }

    private void start(String... overrides) {
        var args = new ArrayList<>(List.of("--server.port=0", "--mock.http.response-delay-ms=0",
                "--logging.level.root=WARN", "--spring.main.banner-mode=off"));
        // Use distinct keys for override precedence instead of duplicate command-line properties.
        for (String override : overrides) {
            String prefix = override.substring(0, override.indexOf('=') + 1);
            args.removeIf(arg -> arg.startsWith(prefix));
            args.add(override);
        }
        app = MockServerApplication.start(args.toArray(String[]::new));
    }

    private List<HttpResponse<String>> burst(int count) throws Exception {
        var futures = new ArrayList<CompletableFuture<HttpResponse<String>>>();
        for (int i = 0; i < count; i++) futures.add(http.sendAsync(request(payload, Duration.ofSeconds(5)), HttpResponse.BodyHandlers.ofString()));
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private JsonNode get(String path) throws Exception {
        var response = http.send(HttpRequest.newBuilder(uri(path)).timeout(Duration.ofSeconds(3)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return mapper.readTree(response.body());
    }

    private HttpResponse<String> send(String body, Duration timeout) throws Exception {
        return http.send(request(body, timeout), HttpResponse.BodyHandlers.ofString());
    }

    private HttpRequest request(String body, Duration timeout) {
        return HttpRequest.newBuilder(uri("/pushes")).timeout(timeout).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }

    private URI uri(String path) { return URI.create("http://127.0.0.1:" + app.getWebServer().getPort() + path); }

    @AfterEach
    void close() {
        if (app != null) app.close();
        http.close();
    }
}
