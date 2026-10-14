package dev.jinhyeon.notificationlab.provider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.config.ProviderProperties;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ConnectionRequestTimeoutException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolStats;
import org.apache.hc.core5.util.Timeout;

/** Local simulator adapter. One invocation makes at most one HTTP attempt. */
public final class HttpPushProvider implements PushProvider, AutoCloseable {
    private static final int MAX_RESPONSE_BYTES = 8192;
    private final ProviderProperties.Http settings;
    private final ObjectMapper mapper;
    private final PoolingHttpClientConnectionManager pool;
    private final CloseableHttpClient client;
    private final Semaphore admission;
    private final ScheduledThreadPoolExecutor deadlines;
    private final Set<HttpPost> active = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    public HttpPushProvider(ProviderProperties.Http settings, ObjectMapper mapper) {
        this.settings = settings;
        this.mapper = mapper;
        this.admission = new Semaphore(settings.maxConnections() + settings.maxPending());
        this.pool = PoolingHttpClientConnectionManagerBuilder.create()
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
                .setMaxConnTotal(settings.maxConnections()).setMaxConnPerRoute(settings.maxConnections())
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(settings.connectTimeoutMs()))
                        .setSocketTimeout(Timeout.ofMilliseconds(settings.responseTimeoutMs())).build()).build();
        this.client = HttpClients.custom().setConnectionManager(pool)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofMilliseconds(settings.acquireTimeoutMs()))
                        .setResponseTimeout(Timeout.ofMilliseconds(settings.responseTimeoutMs())).build())
                .disableAutomaticRetries().disableRedirectHandling().disableCookieManagement()
                .disableContentCompression().build();
        this.deadlines = new ScheduledThreadPoolExecutor(1, runnable -> {
            var thread = new Thread(runnable, "http-mock-deadline");
            thread.setDaemon(true);
            return thread;
        });
        deadlines.setRemoveOnCancelPolicy(true);
    }

    @Override
    public PushResult send(PushRequest request) {
        if (Thread.currentThread().isInterrupted()) return retry("HTTP_INTERRUPTED");
        if (closed.get()) return retry("HTTP_CLIENT_CLOSED");
        if (!admission.tryAcquire()) return retry("HTTP_CLIENT_BUSY");
        var post = new HttpPost(settings.baseUrl().resolve("/pushes"));
        var expired = new AtomicBoolean();
        java.util.concurrent.ScheduledFuture<?> deadline = null;
        ClassicHttpResponse response = null;
        active.add(post);
        try {
            if (closed.get()) return retry("HTTP_CLIENT_CLOSED");
            post.setEntity(new ByteArrayEntity(mapper.writeValueAsBytes(request), ContentType.APPLICATION_JSON));
            // At most maxConnections + maxPending scheduled tasks; cancellation removes completed tasks.
            deadline = deadlines.schedule(() -> {
                expired.set(true);
                post.cancel();
            }, settings.deadlineMs(), TimeUnit.MILLISECONDS);
            response = client.executeOpen(null, post, null);
            int status = response.getCode();
            if (status != 202) {
                post.cancel(); // Never drain an arbitrary error/redirect body or follow a Location.
                if (status == 429 || status >= 500 && status <= 599) return retry("HTTP_STATUS_" + status);
                if (status >= 400 && status <= 499 && status != 408) {
                    return new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "HTTP_STATUS_" + status);
                }
                return retry("HTTP_UNKNOWN_STATUS");
            }
            if (response.getEntity() == null) return retry("HTTP_UNKNOWN_RESPONSE");
            byte[] body = response.getEntity().getContent().readNBytes(MAX_RESPONSE_BYTES + 1);
            if (body.length > MAX_RESPONSE_BYTES) {
                post.cancel();
                return retry("HTTP_UNKNOWN_RESPONSE");
            }
            var json = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(body);
            var id = json == null ? null : json.get("providerMessageId");
            // Accept only the local mock's generated identifier, never arbitrary response contents.
            if (id == null || !id.isTextual() || !id.asText().matches("http-mock-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}")) {
                return retry("HTTP_UNKNOWN_RESPONSE");
            }
            if (expired.get()) return retry("HTTP_UNKNOWN_DEADLINE");
            return new PushResult(PushResult.Outcome.ACCEPTED, id.asText(), null);
        } catch (ConnectionRequestTimeoutException failure) {
            return retry("HTTP_POOL_TIMEOUT");
        } catch (SocketTimeoutException failure) {
            return retry("HTTP_UNKNOWN_TIMEOUT");
        } catch (IOException failure) {
            return retry(expired.get() ? "HTTP_UNKNOWN_DEADLINE" : "HTTP_UNKNOWN_IO");
        } catch (java.util.concurrent.RejectedExecutionException failure) {
            return retry("HTTP_CLIENT_CLOSED");
        } finally {
            if (response != null) {
                try { response.close(); } catch (IOException ignored) { /* No exception payload logging. */ }
            }
            if (deadline != null) deadline.cancel(false);
            active.remove(post);
            admission.release();
        }
    }

    private static PushResult retry(String code) {
        return new PushResult(PushResult.Outcome.RETRYABLE_FAILURE, null, code);
    }

    PoolStats poolStats() { return pool.getTotalStats(); }
    int pendingDeadlines() { return deadlines.getQueue().size(); }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        active.forEach(HttpPost::cancel);
        client.close(CloseMode.IMMEDIATE);
        deadlines.shutdownNow();
    }
}
