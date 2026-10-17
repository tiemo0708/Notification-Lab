package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter.Decision;
import dev.jinhyeon.notificationlab.ratelimit.RedisRateLimiter.Outcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class WorkerQuotaTest {
    @Test
    void pauseModeChecksOnceAndReturnsOnlyABoundedSafeDelay() {
        var calls = new AtomicInteger();
        var registry = new SimpleMeterRegistry();
        var quota = new WorkerQuota(() -> { calls.incrementAndGet(); return new Decision(Outcome.LIMITED, Long.MAX_VALUE); },
                1, true, registry);
        assertThatThrownBy(quota::awaitPermit).isInstanceOfSatisfying(WorkerQuota.QuotaDeferred.class,
                deferred -> assertThat(deferred.delayMs()).isEqualTo(60000));
        assertThat(calls.get()).isEqualTo(1);
        assertThat(registry.get("notification.worker.quota.waiting").gauge().value()).isZero();
        assertThat(new WorkerQuota.QuotaDeferred(0).delayMs()).isEqualTo(100);
    }

    @Test
    void pauseModeNeverDefersRedisErrorsOrShutdown() {
        var quota = new WorkerQuota(() -> new Decision(Outcome.UNAVAILABLE, 0), 100, true, new SimpleMeterRegistry());
        assertThatThrownBy(quota::awaitPermit).isNotInstanceOf(WorkerQuota.QuotaDeferred.class).hasMessageContaining("unavailable");
        quota.stopAdmission();
        assertThatThrownBy(quota::awaitPermit).hasMessageContaining("stopping");
    }

    @Test
    void retriesLimitedDecisionAndReleasesWaitingCountAfterPermit() {
        var calls = new AtomicInteger();
        var registry = new SimpleMeterRegistry();
        var quota = new WorkerQuota(() -> new Decision(calls.incrementAndGet() == 3 ? Outcome.ALLOWED : Outcome.LIMITED, 1), 1000, registry);
        quota.awaitPermit();
        assertThat(calls.get()).isEqualTo(3);
        assertThat(registry.get("notification.worker.quota.waiting").gauge().value()).isZero();
    }

    @Test
    void timeoutDoesNotTreatAdvisoryRetryDelayAsPermitOrPollContinuously() {
        var calls = new AtomicInteger();
        var quota = new WorkerQuota(() -> { calls.incrementAndGet(); return new Decision(Outcome.LIMITED, 10000); },
                100, new SimpleMeterRegistry());
        assertThatThrownBy(quota::awaitPermit).hasMessageContaining("timeout");
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void unavailableStopsWithoutAutomaticRetry() {
        var calls = new AtomicInteger();
        var quota = new WorkerQuota(() -> { calls.incrementAndGet(); return new Decision(Outcome.UNAVAILABLE, 0); },
                1000, new SimpleMeterRegistry());
        assertThatThrownBy(quota::awaitPermit).hasMessageContaining("unavailable");
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void permitArrivingAfterWaitBudgetIsDiscarded() {
        var registry = new SimpleMeterRegistry();
        var quota = new WorkerQuota(() -> {
            try { TimeUnit.MILLISECONDS.sleep(75); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return new Decision(Outcome.ALLOWED, 0);
        }, 25, registry);
        assertThatThrownBy(quota::awaitPermit).hasMessageContaining("timeout");
        assertThat(registry.get("notification.worker.quota.waiting").gauge().value()).isZero();
        assertThat(registry.get("notification.worker.quota.rejected").tag("reason", "timeout").counter().count()).isEqualTo(1);
    }

    @Test
    void shutdownWakesWaitingCallerWithoutAnotherAcquisition() throws Exception {
        var calls = new AtomicInteger();
        var quota = new WorkerQuota(() -> { calls.incrementAndGet(); return new Decision(Outcome.LIMITED, 5000); },
                5000, new SimpleMeterRegistry());
        try (var executor = Executors.newSingleThreadExecutor()) {
            var work = executor.submit(() -> assertThatThrownBy(quota::awaitPermit).hasMessageContaining("stopping"));
            await().atMost(Duration.ofSeconds(1)).until(() -> calls.get() == 1);
            quota.stopAdmission();
            work.get(1, TimeUnit.SECONDS);
            assertThat(calls.get()).isEqualTo(1);
            assertThatThrownBy(quota::awaitPermit).hasMessageContaining("stopping");
        }
    }

    @Test
    void interruptionPreservesFlagAndStopsFurtherAcquisitions() throws Exception {
        var calls = new AtomicInteger();
        var preserved = new AtomicBoolean();
        var quota = new WorkerQuota(() -> { calls.incrementAndGet(); return new Decision(Outcome.LIMITED, 5000); },
                5000, new SimpleMeterRegistry());
        var thread = new Thread(() -> {
            assertThatThrownBy(quota::awaitPermit).hasMessageContaining("interrupted");
            preserved.set(Thread.currentThread().isInterrupted());
        });
        thread.start();
        try {
            await().atMost(Duration.ofSeconds(1)).until(() -> calls.get() == 1);
            thread.interrupt(); thread.join(1000);
            assertThat(thread.isAlive()).isFalse();
            assertThat(preserved).isTrue();
            assertThat(calls.get()).isEqualTo(1);
        } finally { thread.interrupt(); }
    }

    @Test
    void absentClientBypassesQuotaAndInvalidWaitBoundsAreRejected() {
        var quota = new WorkerQuota((Supplier<Decision>) null, 2000, new SimpleMeterRegistry());
        assertThat(quota.enabled()).isFalse();
        quota.awaitPermit();
        for (int timeout : new int[]{0, 5001}) assertThatThrownBy(() -> new WorkerQuota((Supplier<Decision>) null,
                timeout, new SimpleMeterRegistry())).isInstanceOf(IllegalArgumentException.class);
    }
}
