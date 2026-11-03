package dev.jinhyeon.notificationlab.campaign.batch;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/** Test-only observations. No diagnostic DB queries, credentials, payloads or hidden retries. */
final class CampaignDeliveryDiagnostics {
    private CampaignDeliveryDiagnostics() { }

    static HttpResponse<String> sendProbe(HttpClient client, HttpRequest request, Map<String, Object> report,
                                           String phase, int ordinal, int acceptedResponses)
            throws IOException, InterruptedException {
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (HttpTimeoutException timeout) {
            // The server may already have committed. Never infer rejection or retry automatically.
            report.put("probeFailure", Map.of("phase", phase, "ordinal", ordinal,
                    "acceptedResponses", acceptedResponses, "timeoutMillis", request.timeout().orElseThrow().toMillis(),
                    "responseObserved", false, "serverOutcome", "UNKNOWN"));
            throw timeout;
        }
    }

    static Map<String, Object> application(ConfigurableApplicationContext app) {
        var result = new LinkedHashMap<String, Object>();
        var registry = app.getBean(MeterRegistry.class);
        var limiter = app.getBeanProvider(dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter.class).getIfAvailable();
        if (limiter != null) result.put("redisQuota", limiter.diagnostics());
        result.put("transientQuotaRecoveryEnabled", app.getEnvironment().getProperty("app.rate-limit.transient-recovery-enabled", Boolean.class, false));
        var exhausted = registry.find("notification.worker.quota.transient.exhausted").counter();
        result.put("transientQuotaExhausted", exhausted == null ? null : exhausted.count());
        for (String name : List.of("notification.consumer.lag", "outbox.unpublished", "outbox.oldest.age.seconds",
                "outbox.sample.age.seconds", "notification.consumer.sample.age.seconds",
                "notification.worker.active", "notification.worker.waiting")) {
            var gauge = registry.find(name).gauge();
            result.put(name, gauge == null ? null : gauge.value());
        }
        var completed = registry.find("notification.completed").tag("status", "PROVIDER_ACCEPTED").counter();
        result.put("providerAcceptedCounter", completed == null ? null : completed.count());
        var listeners = app.getBeanProvider(KafkaListenerEndpointRegistry.class).getIfAvailable();
        var listener = listeners == null ? null : listeners.getListenerContainer("notification-worker");
        result.put("listenerRunning", listener == null ? null : listener.isRunning());
        var dataSource = app.getBean(DataSource.class);
        HikariDataSource hikari = null;
        try {
            hikari = dataSource instanceof HikariDataSource direct ? direct :
                    dataSource.isWrapperFor(HikariDataSource.class) ? dataSource.unwrap(HikariDataSource.class) : null;
        } catch (java.sql.SQLException unavailable) {
            // An unavailable wrapper remains missing evidence; never expose its raw exception.
        }
        if (hikari != null && hikari.getHikariPoolMXBean() != null) {
            var pool = hikari.getHikariPoolMXBean();
            result.put("hikari", Map.of("available", true, "active", pool.getActiveConnections(),
                    "idle", pool.getIdleConnections(), "total", pool.getTotalConnections(),
                    "waiting", pool.getThreadsAwaitingConnection(), "maximum", hikari.getMaximumPoolSize()));
        } else {
            result.put("hikari", Map.of("available", false));
        }
        return result;
    }

    static Map<String, Long> cgroupCounters(String text) {
        var values = new LinkedHashMap<String, Long>();
        for (String line : text.strip().split("\\R")) {
            String[] fields = line.strip().split("\\s+");
            if (fields.length != 2 || values.put(fields[0], Long.parseLong(fields[1])) != null) {
                throw new IllegalArgumentException("Invalid cgroup counters");
            }
        }
        if (!values.containsKey("oom_kill") || !values.containsKey("usage_usec")) {
            throw new IllegalArgumentException("Missing cgroup counters");
        }
        return values;
    }
}
