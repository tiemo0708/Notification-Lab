package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.config.AsyncProperties;
import dev.jinhyeon.notificationlab.config.RelayProperties;
import dev.jinhyeon.notificationlab.config.TrafficRouting;
import dev.jinhyeon.notificationlab.outbox.OutboxRelay;
import dev.jinhyeon.notificationlab.outbox.OutboxStore;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

/** Test-only Relay concurrency, sharing one campaign app's existing DB pool and producer. */
final class CampaignRelayGroup implements AutoCloseable {
    interface Loop {
        void run();
        void stop();
    }

    private final ScheduledThreadPoolExecutor executor;
    private final List<Loop> loops = new ArrayList<>();
    private final List<ScheduledFuture<?>> futures = new ArrayList<>();
    private final List<AtomicLong> invocations = new ArrayList<>();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final AtomicBoolean stopping = new AtomicBoolean();

    static int validate(int instances) {
        if (instances < 1 || instances > 3) throw new IllegalArgumentException("Relay instances must be 1 to 3");
        return instances;
    }

    @SuppressWarnings("unchecked")
    static CampaignRelayGroup start(ConfigurableApplicationContext app, int instances) {
        if (!app.getBeansOfType(OutboxRelay.class).isEmpty()) {
            throw new IllegalStateException("Automatic campaign Relay must be disabled for this experiment");
        }
        // Boot exposes KafkaTemplate<Object,Object>; this app configures String serializers.
        KafkaTemplate<String, String> kafka = app.getBean(KafkaTemplate.class);
        return new CampaignRelayGroup(instances, index -> {
            var relay = new OutboxRelay(app.getBean(OutboxStore.class), kafka,
                    app.getBean(AsyncProperties.class), app.getBean(MeterRegistry.class),
                    app.getBean(TrafficRouting.class), app.getBean(RelayProperties.class));
            return new Loop() {
                public void run() { relay.relay(); }
                public void stop() { relay.stopClaiming(); }
            };
        });
    }

    CampaignRelayGroup(int instances, IntFunction<Loop> factory) {
        validate(instances);
        // Only these fixed periodic tasks are submitted; no per-record futures or submission queue.
        executor = new ScheduledThreadPoolExecutor(instances, Thread.ofPlatform().name("campaign-relay-probe-", 0).factory());
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        try {
            for (int i = 0; i < instances; i++) {
                loops.add(factory.apply(i));
                invocations.add(new AtomicLong());
            }
            for (int i = 0; i < instances; i++) {
                var loop = loops.get(i);
                var count = invocations.get(i);
                futures.add(executor.scheduleWithFixedDelay(() -> {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    count.incrementAndGet();
                    try { loop.run(); }
                    finally { active.decrementAndGet(); }
                }, 0, 100, TimeUnit.MILLISECONDS));
            }
        } catch (RuntimeException | Error failure) {
            loops.forEach(Loop::stop);
            executor.shutdownNow();
            throw failure;
        }
    }

    void checkHealthy() {
        // A periodic task's uncaught failure otherwise silently suppresses its next invocation.
        for (var future : futures) {
            if (future.isDone()) throw new IllegalStateException("Campaign Relay periodic task stopped unexpectedly");
        }
    }

    Map<String, Object> snapshot() {
        return Map.of("instances", loops.size(), "activeRuns", active.get(), "maxConcurrentRuns", peak.get(),
                "invocationsPerInstance", invocations.stream().map(AtomicLong::get).toList(),
                "queuedPeriodicTasks", executor.getQueue().size(), "terminated", executor.isTerminated());
    }

    @Override
    public void close() {
        if (stopping.compareAndSet(false, true)) {
            loops.forEach(Loop::stop);
            futures.forEach(future -> future.cancel(false));
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Campaign Relay shutdown deadline exceeded");
                }
            }
        } catch (InterruptedException failure) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Campaign Relay shutdown interrupted");
        }
    }
}
