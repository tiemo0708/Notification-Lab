package dev.jinhyeon.notificationlab.provider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.config.FcmLabProperties;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;

/** FCM HTTP v1 for one synthetic send in an explicitly armed local lab. */
public final class FcmPushProvider implements PushProvider, AutoCloseable {
    static final String LAB_RECIPIENT = "fcm-smoke-emulator";
    private static final int MAX_RESPONSE = 8192;
    private static final int MAX_RECEIPT_ID = 255;
    private final ObjectMapper mapper;
    private final URI endpoint;
    private final String deviceToken;
    private final String accessToken;
    private final Path sendMarker;
    private final Pattern receiptId;
    private final CloseableHttpClient client;
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(1, task -> {
        var thread = new Thread(task, "fcm-lab-deadline");
        thread.setDaemon(true);
        return thread;
    });

    public FcmPushProvider(FcmLabProperties settings, ObjectMapper mapper) {
        this(settings, mapper, URI.create("https://fcm.googleapis.com/v1/projects/" + settings.projectId() + "/messages:send"));
    }

    FcmPushProvider(FcmLabProperties settings, ObjectMapper mapper, URI endpoint) {
        this.mapper = mapper;
        this.endpoint = endpoint;
        this.deviceToken = readToken(settings.deviceTokenFile(), Pattern.compile("[A-Za-z0-9:_-]{32,4096}"), 4096);
        this.accessToken = readToken(settings.accessTokenFile(), Pattern.compile("[A-Za-z0-9._~+/=-]{20,8192}"), 8192);
        this.sendMarker = settings.sendMarker();
        this.receiptId = Pattern.compile("projects/(?:" + Pattern.quote(settings.projectId())
                + "|[0-9]{1,20})/messages/[A-Za-z0-9:%._~-]+");
        var connections = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(1).setMaxConnPerRoute(1)
                .setDefaultConnectionConfig(ConnectionConfig.custom().setConnectTimeout(Timeout.ofSeconds(3))
                        .setSocketTimeout(Timeout.ofSeconds(5)).build()).build();
        this.client = HttpClients.custom().setConnectionManager(connections)
                .setDefaultRequestConfig(RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofSeconds(1))
                        .setResponseTimeout(Timeout.ofSeconds(5)).build())
                .disableAutomaticRetries().disableRedirectHandling().disableCookieManagement()
                .disableContentCompression().build();
        deadlines.setRemoveOnCancelPolicy(true);
    }

    @Override
    public PushResult send(PushRequest request) {
        if (!LAB_RECIPIENT.equals(request.recipientId())) return permanent("FCM_LAB_TARGET_NOT_ALLOWED");
        if (Thread.currentThread().isInterrupted()) return transientFailure("FCM_INTERRUPTED");
        var post = new HttpPost(endpoint);
        try {
            // Only the synthetic correlation ID crosses the lab boundary.
            post.setEntity(new ByteArrayEntity(mapper.writeValueAsBytes(Map.of("message", Map.of(
                    "token", deviceToken, "data", Map.of("smokeId", request.notificationId().toString())))),
                    ContentType.APPLICATION_JSON));
            post.setHeader("Authorization", "Bearer " + accessToken);
            // Retain the marker even if the response is lost or MySQL fails after FCM accepted.
            Files.writeString(sendMarker, request.notificationId().toString(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (FileAlreadyExistsException used) {
            return permanent("FCM_LAB_SEND_LIMIT_REACHED");
        } catch (IOException unavailable) {
            return permanent("FCM_LAB_PREPARATION_FAILED");
        }
        return executeOnce(post);
    }

    private PushResult executeOnce(HttpPost post) {
        var deadline = deadlines.schedule(post::cancel, 10, TimeUnit.SECONDS);
        try (var response = client.executeOpen(null, post, null)) {
            int status = response.getCode();
            if (status != 200) {
                post.cancel();
                if (status == 408 || status == 429 || status >= 500) return transientFailure("FCM_HTTP_" + status);
                if (status >= 400) return permanent("FCM_HTTP_" + status);
                return transientFailure("FCM_OUTCOME_UNKNOWN");
            }
            if (response.getEntity() != null) {
                byte[] body = response.getEntity().getContent().readNBytes(MAX_RESPONSE + 1);
                if (body.length <= MAX_RESPONSE) {
                    var json = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(body);
                    var name = json == null ? null : json.get("name");
                    if (name != null && name.isTextual() && name.asText().length() <= MAX_RECEIPT_ID
                            && receiptId.matcher(name.asText()).matches()) {
                        return new PushResult(PushResult.Outcome.ACCEPTED, name.asText(), null);
                    }
                }
            }
            post.cancel();
            return transientFailure("FCM_OUTCOME_UNKNOWN");
        } catch (IOException failure) {
            return transientFailure("FCM_OUTCOME_UNKNOWN");
        } finally {
            deadline.cancel(false);
        }
    }

    private static String readToken(Path path, Pattern format, int limit) {
        try (var source = Files.newInputStream(path)) {
            byte[] bytes = source.readNBytes(limit + 3);
            String token = new String(bytes, StandardCharsets.US_ASCII).strip();
            if (bytes.length <= limit + 2 && format.matcher(token).matches()) return token;
        } catch (IOException unavailable) {
            // Configuration errors never contain credentials or file contents.
        }
        throw new IllegalArgumentException("FCM lab token file is unreadable or invalid");
    }

    private static PushResult permanent(String code) {
        return new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, code);
    }

    private static PushResult transientFailure(String code) {
        return new PushResult(PushResult.Outcome.RETRYABLE_FAILURE, null, code);
    }

    @Override
    public void close() {
        client.close(CloseMode.IMMEDIATE);
        deadlines.shutdownNow();
    }
}
