package dev.jinhyeon.notificationlab.campaign.batch;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CampaignRelayGroupTest {
    @Test
    void rejectsUnboundedCountsBeforeCreatingAnyLoop() {
        var created = new AtomicInteger();
        for (int count : new int[] {0, -1, 4, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> new CampaignRelayGroup(count, index -> {
                created.incrementAndGet();
                throw new AssertionError("Factory must not run");
            })).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(created).hasValue(0);
    }

    @Test
    void runsBothLoopsConcurrentlyAndStopsInFlightWorkBeforeReturning() throws Exception {
        var entered = new CountDownLatch(2);
        var released = new CountDownLatch(1);
        var stopped = new AtomicInteger();
        var completed = new AtomicInteger();
        var group = new CampaignRelayGroup(2, index -> new CampaignRelayGroup.Loop() {
            public void run() {
                entered.countDown();
                try {
                    if (!released.await(5, TimeUnit.SECONDS)) throw new AssertionError("Stop was not delivered");
                    completed.incrementAndGet();
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Normal shutdown must not interrupt the ACK flush");
                }
            }
            public void stop() { stopped.incrementAndGet(); released.countDown(); }
        });
        try (group) {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(group.snapshot()).containsEntry("activeRuns", 2).containsEntry("maxConcurrentRuns", 2);
            group.checkHealthy();
        }
        assertThat(stopped).hasValue(2);
        group.close();
        assertThat(stopped).as("Closing twice must not stop loops twice").hasValue(2);
        assertThat(completed.get()).isGreaterThanOrEqualTo(2);
        assertThat(group.snapshot()).containsEntry("activeRuns", 0).containsEntry("queuedPeriodicTasks", 0).containsEntry("terminated", true);
    }

    @Test
    void exposesSuppressedPeriodicFailuresInsteadOfSilentlyLosingAWorker() {
        try (var group = new CampaignRelayGroup(1, index -> new CampaignRelayGroup.Loop() {
            public void run() { throw new IllegalStateException("synthetic failure"); }
            public void stop() { }
        })) {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThatThrownBy(group::checkHealthy).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Campaign Relay periodic task stopped unexpectedly"));
        }
    }
}
