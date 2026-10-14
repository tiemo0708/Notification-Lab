package dev.jinhyeon.notificationmock;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FixedWindowQuotaTest {
    @Test
    void refillsAtTheMonotonicWindowBoundaryWithoutAccumulatingCredit() {
        var now = new AtomicLong(100);
        var quota = new FixedWindowQuota(2, 1000, now::get);
        assertThat(quota.acquire()).isZero();
        assertThat(quota.acquire()).isZero();
        assertThat(quota.acquire()).isEqualTo(1);
        now.addAndGet(999_999_999);
        assertThat(quota.acquire()).isEqualTo(1);
        now.incrementAndGet();
        assertThat(quota.acquire()).isZero();
        now.addAndGet(10_000_000_000L);
        assertThat(quota.acquire()).isZero();
        assertThat(quota.acquire()).isZero();
        assertThat(quota.acquire()).isEqualTo(1);
    }

    @Test
    void admitsOnlyTheConfiguredCountUnderConcurrentContention() throws Exception {
        var quota = new FixedWindowQuota(3, 1000, () -> 0L);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var tasks = new ArrayList<Callable<Long>>();
            for (int i = 0; i < 40; i++) tasks.add(quota::acquire);
            long admitted = 0;
            for (var result : executor.invokeAll(tasks)) if (result.get() == 0) admitted++;
            assertThat(admitted).isEqualTo(3);
        }
    }
}
