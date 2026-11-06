package dev.jinhyeon.notificationlab.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommitWakeupRetryTest {
    private static final Map<TopicPartition, OffsetAndMetadata> OFFSETS = Map.of(new TopicPartition("requested", 0), new OffsetAndMetadata(7));

    @Test
    void commitsNormallyWithoutAnExtraAttempt() {
        try (var delegate = new CommitConsumer(0)) {
            var retries = new AtomicInteger();
            var consumer = CommitWakeupRetry.wrap(delegate, retries::incrementAndGet);
            consumer.commitSync(OFFSETS, Duration.ofSeconds(1));
            assertThat(delegate.calls).isEqualTo(1);
            assertThat(delegate.committedOffsets).isEqualTo(OFFSETS);
            assertThat(retries).hasValue(0);
        }
    }

    @Test
    void retriesTheSameCompletedOffsetsWithOnlyTheRemainingBudget() {
        try (var delegate = new CommitConsumer(1)) {
            var retries = new AtomicInteger();
            var clock = new AtomicLong();
            var consumer = CommitWakeupRetry.wrap(delegate, retries::incrementAndGet, () -> clock.getAndAdd(100));
            consumer.commitSync(OFFSETS, Duration.ofNanos(1000));
            assertThat(delegate.calls).isEqualTo(2);
            assertThat(delegate.committedOffsets).isEqualTo(OFFSETS);
            assertThat(delegate.timeouts).containsExactly(Duration.ofNanos(1000), Duration.ofNanos(900));
            assertThat(retries).hasValue(1);
        }
    }

    @Test
    void propagatesASecondWakeupWithoutLoopingOrClaimingCommitSuccess() {
        try (var delegate = new CommitConsumer(2)) {
            var retries = new AtomicInteger();
            var consumer = CommitWakeupRetry.wrap(delegate, retries::incrementAndGet);
            assertThatThrownBy(() -> consumer.commitSync(OFFSETS, Duration.ofSeconds(1))).isInstanceOf(WakeupException.class);
            assertThat(delegate.calls).isEqualTo(2);
            assertThat(delegate.committedOffsets).isNull();
            assertThat(retries).hasValue(1);
        }
    }

    @Test
    void doesNotRetryWhenTheOriginalDeadlineHasExpired() {
        try (var delegate = new CommitConsumer(1)) {
            var clock = new AtomicLong();
            var retries = new AtomicInteger();
            var consumer = CommitWakeupRetry.wrap(delegate, retries::incrementAndGet, () -> clock.getAndAdd(2000));
            assertThatThrownBy(() -> consumer.commitSync(OFFSETS, Duration.ofNanos(1000))).isInstanceOf(WakeupException.class);
            assertThat(delegate.calls).isEqualTo(1);
            assertThat(retries).hasValue(0);
        }
    }

    @Test
    void propagatesAnOrdinaryCommitFailureUnchanged() {
        try (var delegate = new CommitConsumer(0)) {
            delegate.failure = new TimeoutException("synthetic commit failure");
            var retries = new AtomicInteger();
            var consumer = CommitWakeupRetry.wrap(delegate, retries::incrementAndGet);
            assertThatThrownBy(() -> consumer.commitSync(OFFSETS, Duration.ofSeconds(1))).isSameAs(delegate.failure);
            assertThat(delegate.calls).isEqualTo(1);
            assertThat(retries).hasValue(0);
        }
    }

    @Test
    void leavesPollWakeupAndLifecycleDelegationUnchanged() {
        try (var delegate = new CommitConsumer(0)) {
            var retries = new AtomicInteger();
            var consumer = CommitWakeupRetry.wrap(delegate, retries::incrementAndGet);
            assertThat(consumer.equals(consumer)).isTrue();
            assertThat(consumer.equals(delegate)).isFalse();
            assertThat(consumer.equals(null)).isFalse();
            int identityHash = consumer.hashCode();
            consumer.assign(OFFSETS.keySet());
            assertThat(consumer.hashCode()).isEqualTo(identityHash);
            assertThat(consumer.assignment()).containsExactlyElementsOf(OFFSETS.keySet());
            consumer.wakeup();
            assertThatThrownBy(() -> consumer.poll(Duration.ZERO)).isInstanceOf(WakeupException.class);
            assertThat(delegate.calls).isZero();
            assertThat(retries).hasValue(0);
        }
    }

    @Test
    void doesNotRetryAnImplicitPositionCommit() {
        try (var delegate = new CommitConsumer(0)) {
            var retries = new AtomicInteger();
            var consumer = CommitWakeupRetry.wrap(delegate, retries::incrementAndGet);
            assertThatThrownBy(() -> consumer.commitSync(Duration.ofSeconds(1))).isInstanceOf(WakeupException.class);
            assertThat(retries).hasValue(0);
        }
    }

    private static class CommitConsumer extends MockConsumer<String, String> {
        private final int wakeups;
        private int calls;
        private RuntimeException failure;
        private Map<TopicPartition, OffsetAndMetadata> committedOffsets;
        private final List<Duration> timeouts = new ArrayList<>();
        CommitConsumer(int wakeups) { super(OffsetResetStrategy.EARLIEST); this.wakeups = wakeups; }
        @Override public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            calls++;
            timeouts.add(timeout);
            if (calls <= wakeups) throw new WakeupException();
            if (failure != null) throw failure;
            committedOffsets = Map.copyOf(offsets);
        }
        @Override public void commitSync(Duration timeout) { throw new WakeupException(); }
    }
}
