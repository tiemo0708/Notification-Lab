package dev.jinhyeon.notificationlab.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.jinhyeon.notificationlab.config.FcmLabProperties;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static dev.jinhyeon.notificationlab.provider.PushResult.Outcome.ACCEPTED;
import static dev.jinhyeon.notificationlab.provider.PushResult.Outcome.PERMANENT_FAILURE;
import static dev.jinhyeon.notificationlab.provider.PushResult.Outcome.RETRYABLE_FAILURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FcmPushProviderTest {
    private static final String PROJECT = "synthetic-fcm-test";
    private static final String DEVICE = "synthetic_emulator_registration_token";
    private static final String ACCESS = "synthetic.oauth.access.token";
    private static final String FCM_ID = "projects/" + PROJECT + "/messages/0:1234%5678";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir Path directory;

    @Test
    void postsOnlyTheNotificationIdToTheSingleDevice() throws Exception {
        var request = new AtomicReference<String>();
        var authorization = new AtomicReference<String>();
        var calls = new AtomicInteger();
        try (var server = server(exchange -> {
            calls.incrementAndGet();
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            request.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 200, "{\"name\":\"" + FCM_ID + "\"}");
        }); var provider = provider(settings(), server)) {
            assertThat(provider.send(new PushRequest(UUID.randomUUID(), "other", "ignore", "ignore")))
                    .isEqualTo(new PushResult(PERMANENT_FAILURE, null, "FCM_LAB_TARGET_NOT_ALLOWED"));
            assertThat(calls.get()).isZero();
            assertThat(directory.resolve("used")).doesNotExist();

            var notification = notification();
            assertThat(provider.send(notification)).isEqualTo(new PushResult(ACCEPTED, FCM_ID, null));
            assertThat(authorization.get()).isEqualTo("Bearer " + ACCESS);
            var payload = MAPPER.readTree(request.get());
            assertThat(payload.size()).isEqualTo(1);
            assertThat(payload.path("message").size()).isEqualTo(2);
            assertThat(payload.at("/message/token").asText()).isEqualTo(DEVICE);
            assertThat(payload.at("/message/data/smokeId").asText()).isEqualTo(notification.notificationId().toString());
            assertThat(directory.resolve("used")).hasContent(notification.notificationId().toString());
            assertThat(provider.send(notification())).isEqualTo(new PushResult(PERMANENT_FAILURE, null, "FCM_LAB_SEND_LIMIT_REACHED"));
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void competingProviderInstancesUsingTheSameMarkerCanOnlyPostOnce() throws Exception {
        var calls = new AtomicInteger();
        var settings = settings();
        try (var server = server(exchange -> {
            calls.incrementAndGet();
            respond(exchange, 200, "{\"name\":\"" + FCM_ID + "\"}");
        }); var first = provider(settings, server); var second = provider(settings, server);
             var executor = Executors.newFixedThreadPool(2)) {
            var left = executor.submit(() -> first.send(notification()));
            var right = executor.submit(() -> second.send(notification()));
            assertThat(new PushResult[]{left.get(), right.get()}).containsExactlyInAnyOrder(
                    new PushResult(ACCEPTED, FCM_ID, null),
                    new PushResult(PERMANENT_FAILURE, null, "FCM_LAB_SEND_LIMIT_REACHED"));
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void lostResponseCannotSendAgainAfterRestart() throws Exception {
        var calls = new AtomicInteger();
        var settings = settings();
        try (var server = server(exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.close();
        })) {
            try (var provider = provider(settings, server)) {
                assertThat(provider.send(notification())).isEqualTo(new PushResult(RETRYABLE_FAILURE, null, "FCM_OUTCOME_UNKNOWN"));
            }
            try (var restarted = provider(settings, server)) {
                assertThat(restarted.send(notification())).isEqualTo(new PushResult(PERMANENT_FAILURE, null, "FCM_LAB_SEND_LIMIT_REACHED"));
            }
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void ignoresRedirectsAndUntrustedReceiptIds() throws Exception {
        var settings = settings();
        var redirected = new AtomicInteger();
        try (var server = server(exchange -> {
            if (exchange.getRequestURI().getPath().equals("/redirected")) {
                redirected.incrementAndGet();
                respond(exchange, 200, "{}");
            } else {
                exchange.getResponseHeaders().set("Location", "/redirected");
                respond(exchange, 307, "");
            }
        }); var provider = provider(settings, server)) {
            assertThat(provider.send(notification())).isEqualTo(new PushResult(RETRYABLE_FAILURE, null, "FCM_OUTCOME_UNKNOWN"));
            assertThat(redirected.get()).isZero();
        }
        var newMarker = new FcmLabProperties(PROJECT, settings.deviceTokenFile(), settings.accessTokenFile(), directory.resolve("second"));
        try (var server = server(exchange -> respond(exchange, 200, "{\"name\":\"projects/another-project/messages/value\"}"));
             var provider = provider(newMarker, server)) {
            assertThat(provider.send(notification())).isEqualTo(new PushResult(RETRYABLE_FAILURE, null, "FCM_OUTCOME_UNKNOWN"));
        }
    }

    @Test
    void rejectsCredentialsWithoutEchoingFileContents() throws Exception {
        var settings = settings();
        Files.writeString(settings.accessTokenFile(), ACCESS + "\r\nnot-a-token");
        assertThatThrownBy(() -> new FcmPushProvider(settings, MAPPER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FCM lab token file is unreadable or invalid");
    }

    private FcmLabProperties settings() throws IOException {
        return new FcmLabProperties(PROJECT, Files.writeString(directory.resolve("device"), DEVICE),
                Files.writeString(directory.resolve("access"), ACCESS), directory.resolve("used"));
    }

    private static PushRequest notification() {
        return new PushRequest(UUID.randomUUID(), FcmPushProvider.LAB_RECIPIENT, "local title", "local body");
    }

    private static FcmPushProvider provider(FcmLabProperties settings, TestServer server) {
        return new FcmPushProvider(settings, MAPPER, URI.create("http://127.0.0.1:" + server.http().getAddress().getPort() + "/v1/send"));
    }

    private static TestServer server(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", handler);
        server.start();
        return new TestServer(server);
    }

    private record TestServer(HttpServer http) implements AutoCloseable {
        @Override public void close() { http.stop(0); }
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        try (exchange) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(code, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }
}
