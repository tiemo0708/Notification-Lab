package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.config.WorkerProperties;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.worker.CampaignCompletionBatches;
import dev.jinhyeon.notificationlab.worker.WorkerStore;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Reads the normal worker's meters after reconciliation. Crash injection exists only in the first child JVM. */
public final class CampaignCompletionExperiment {
    static Map<String, Object> snapshot(MeterRegistry registry, int limit) {
        if (limit == 1) {
            // The isolated campaign worker has one durable store call per accepted result in the single-success run.
            long singles = registry.get("notification.pipeline.stage").tag("stage", "worker.complete").timer().count();
            return Map.of("limit", 1, "waitMs", CampaignCompletionBatches.MAX_WAIT_MS, "rows", singles,
                    "commitsBySize", List.of(singles), "commits", singles, "deadlineTriggers", 0L);
        }
        var counts = new ArrayList<Long>();
        long rows = 0;
        long commits = 0;
        for (int size = 1; size <= CampaignCompletionBatches.MAX_SIZE; size++) {
            long count = (long) registry.get("notification.worker.campaign.completion.commits")
                    .tag("size", String.valueOf(size)).counter().count();
            counts.add(count);
            rows += size * count;
            commits += count;
        }
        long deadlines = (long) registry.get("notification.worker.campaign.completion.deadlines").counter().count();
        return Map.of("limit", limit, "waitMs", CampaignCompletionBatches.MAX_WAIT_MS, "rows", rows,
                "commitsBySize", counts, "commits", commits, "deadlineTriggers", deadlines);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class CrashWiring {
        @Bean
        @Primary
        CampaignCompletionBatches crashBatches(WorkerProperties workers, WorkerStore store,
                NotificationService service, PlatformTransactionManager manager, MeterRegistry registry) {
            // Give the cold child time to collect exactly five successes before the forced kill.
            return new CampaignCompletionBatches(workers.maxActive(), 2000, store, service, manager, registry);
        }

        @Bean
        SmartInitializingSingleton holdFifthWrite(WorkerStore store,
                @Value("${experiment.hold-five-before-commit}") Path marker) {
            return () -> {
                var writes = new AtomicInteger();
                ((Advised) store).addAdvice((MethodInterceptor) call -> {
                    Object result = call.proceed();
                    if (call.getMethod().getName().equals("complete") && writes.incrementAndGet() == 5) {
                        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
                            throw new IllegalStateException("Expected an uncommitted result transaction");
                        }
                        Files.writeString(marker, "5", StandardOpenOption.CREATE_NEW);
                        new CountDownLatch(1).await(45, TimeUnit.SECONDS);
                        throw new IllegalStateException("Test worker was not killed before commit");
                    }
                    return result;
                });
            };
        }
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(NotificationLabApplication.class, CrashWiring.class)
                .web(WebApplicationType.NONE).run(args);
    }
}
