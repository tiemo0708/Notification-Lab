package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.config.WorkerProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class WorkerAdmissionTest {
    @Test
    void boundsActiveAndWaitingWorkAndPreservesFifoOnCallerThreads() throws Exception {
        var registry = new SimpleMeterRegistry();
        var gate = new WorkerAdmission(new WorkerProperties(3, 1, 2, 5000), registry);
        var release = new CountDownLatch(1);
        var order = new CopyOnWriteArrayList<Integer>();
        try (var executor = Executors.newFixedThreadPool(3)) {
            var active = executor.submit(() -> gate.execute(() -> waitFor(release)));
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.active() == 1);
            var first = executor.submit(() -> {
                Thread caller = Thread.currentThread();
                gate.execute(() -> { assertThat(Thread.currentThread()).isSameAs(caller); order.add(1); });
            });
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.waiting() == 1);
            var second = executor.submit(() -> gate.execute(() -> order.add(2)));
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.waiting() == 2);
            assertThatThrownBy(() -> gate.execute(() -> order.add(99))).hasMessageContaining("full");
            assertThat(registry.get("notification.worker.active").gauge().value()).isEqualTo(1);
            assertThat(registry.get("notification.worker.waiting").gauge().value()).isEqualTo(2);
            release.countDown();
            active.get(2, TimeUnit.SECONDS); first.get(2, TimeUnit.SECONDS); second.get(2, TimeUnit.SECONDS);
            assertThat(order).containsExactly(1, 2);
            assertThat(gate.active()).isZero();
            assertThat(gate.waiting()).isZero();
            assertThat(registry.get("notification.worker.admission.rejected").tag("reason", "full").counter().count()).isEqualTo(1);
        } finally { release.countDown(); }
    }

    @Test
    void timeoutRemovesWaitingWorkWithoutExecutingIt() throws Exception {
        var gate = new WorkerAdmission(new WorkerProperties(2, 1, 1, 100), new SimpleMeterRegistry());
        var release = new CountDownLatch(1);
        var called = new AtomicBoolean();
        try (var executor = Executors.newSingleThreadExecutor()) {
            var active = executor.submit(() -> gate.execute(() -> waitFor(release)));
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.active() == 1);
            assertThatThrownBy(() -> gate.execute(() -> called.set(true))).hasMessageContaining("timeout");
            assertThat(called).isFalse();
            assertThat(gate.waiting()).isZero();
            release.countDown(); active.get(2, TimeUnit.SECONDS);
            gate.execute(() -> called.set(true));
            assertThat(called).isTrue();
        } finally { release.countDown(); }
    }

    @Test
    void shutdownWakesWaitersAndLetsAlreadyAdmittedWorkFinish() throws Exception {
        var gate = new WorkerAdmission(new WorkerProperties(2, 1, 1, 5000), new SimpleMeterRegistry());
        var release = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var active = executor.submit(() -> gate.execute(() -> waitFor(release)));
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.active() == 1);
            var waiting = executor.submit(() -> assertThatThrownBy(() -> gate.execute(() -> { throw new AssertionError(); }))
                    .hasMessageContaining("stopping"));
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.waiting() == 1);
            gate.stopAdmission();
            waiting.get(1, TimeUnit.SECONDS);
            assertThat(gate.active()).isEqualTo(1);
            assertThat(gate.waiting()).isZero();
            assertThatThrownBy(() -> gate.execute(() -> { })).hasMessageContaining("stopping");
            release.countDown(); active.get(2, TimeUnit.SECONDS);
            assertThat(gate.active()).isZero();
        } finally { release.countDown(); }
    }

    @Test
    void interruptRemovesWaiterAndPreservesInterruptFlag() throws Exception {
        var gate = new WorkerAdmission(new WorkerProperties(2, 1, 1, 5000), new SimpleMeterRegistry());
        var release = new CountDownLatch(1);
        var preserved = new AtomicBoolean();
        try (var executor = Executors.newSingleThreadExecutor()) {
            var active = executor.submit(() -> gate.execute(() -> waitFor(release)));
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.active() == 1);
            var waiter = new Thread(() -> {
                try { gate.execute(() -> { throw new AssertionError(); }); }
                catch (IllegalStateException failure) { preserved.set(Thread.currentThread().isInterrupted()); }
            });
            waiter.start();
            await().atMost(Duration.ofSeconds(2)).until(() -> gate.waiting() == 1);
            waiter.interrupt(); waiter.join(1000);
            assertThat(waiter.isAlive()).isFalse();
            assertThat(preserved).isTrue();
            assertThat(gate.waiting()).isZero();
            release.countDown(); active.get(2, TimeUnit.SECONDS);
        } finally { release.countDown(); }
    }

    @Test
    void processingFailureReleasesCapacityAndZeroCapacityRejectsImmediately() throws Exception {
        var gate = new WorkerAdmission(new WorkerProperties(1, 1, 0, 100), new SimpleMeterRegistry());
        assertThatThrownBy(() -> gate.execute(() -> {
            assertThatThrownBy(() -> gate.execute(() -> { })).hasMessageContaining("full");
            throw new IllegalStateException("synthetic failure");
        })).hasMessage("synthetic failure");
        assertThat(gate.active()).isZero();
        gate.execute(() -> assertThat(gate.active()).isEqualTo(1));
    }

    @Test
    void rejectsInvalidBounds() {
        for (int[] v : List.of(new int[]{0,1,1,100}, new int[]{4,1,1,100}, new int[]{2,3,1,100},
                new int[]{1,0,1,100}, new int[]{1,1,-1,100}, new int[]{1,1,4,100},
                new int[]{1,1,1,0}, new int[]{1,1,1,5001})) {
            assertThatThrownBy(() -> new WorkerProperties(v[0],v[1],v[2],v[3])).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static void waitFor(CountDownLatch latch) {
        try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("Test release deadline"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
}
