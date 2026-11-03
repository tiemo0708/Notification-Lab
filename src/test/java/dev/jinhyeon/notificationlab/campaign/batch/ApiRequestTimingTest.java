package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Part;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiRequestTimingTest {
    @Test
    void rejectsMissingConnectionCallsWrongCompletionAndUnboundedDetails() {
        var calls = new ApiConnectionTiming();
        calls.record(ApiConnectionTiming.Method.GET_AUTO_COMMIT, ApiConnectionTiming.Boundary.BEFORE_FIRST_SQL, 1000, false);
        calls.record(ApiConnectionTiming.Method.SET_AUTO_COMMIT_FALSE, ApiConnectionTiming.Boundary.BEFORE_FIRST_SQL, 1000, false);
        assertThatThrownBy(() -> calls.verifySuccessfulAdmission(2, 2000)).isInstanceOf(IllegalStateException.class);
        calls.record(ApiConnectionTiming.Method.SET_AUTO_COMMIT_TRUE, ApiConnectionTiming.Boundary.AFTER_ROLLBACK, 1000, false);
        assertThatThrownBy(() -> calls.verifySuccessfulAdmission(3, 3000)).isInstanceOf(IllegalStateException.class);
        for (int i = 3; i < 8; i++) calls.record(ApiConnectionTiming.Method.GET_AUTO_COMMIT, ApiConnectionTiming.Boundary.BEFORE_FIRST_SQL, 0, false);
        assertThatThrownBy(() -> calls.record(ApiConnectionTiming.Method.GET_AUTO_COMMIT, ApiConnectionTiming.Boundary.BEFORE_FIRST_SQL, 0, false))
                .isInstanceOf(IllegalStateException.class);
        assertThat(calls.snapshot()).hasSize(8);
    }

    @Test
    void pairsDifferentClientAndServletThreadsAndDoesNotLeakTheServletScope() throws Exception {
        var timing = new ApiRequestTiming(1);
        var capture = timing.open(ApiRequestTiming.Window.MIXED);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var server = executor.submit(() -> {
                timing.serve(() -> {
                    entered.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("Test release timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt(); throw new IOException("Test interrupted");
                    }
                    successfulTransaction(timing);
                });
                assertThatThrownBy(() -> timing.jdbc(Part.EXECUTE, 1, false)).isInstanceOf(IllegalStateException.class);
                return null;
            });
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> timing.jdbc(Part.EXECUTE, 1, false)).isInstanceOf(IllegalStateException.class);
            } finally { release.countDown(); }
            timing.complete(capture, 100, 202);
            server.get(5, TimeUnit.SECONDS);
            assertThat(timing.snapshot()).containsEntry("completed", 1).containsEntry("pending", false);
        }
    }

    @Test
    void pairsClientAndServletJdbcWithoutExportingRequestIdentifiers() throws Exception {
        var timing = new ApiRequestTiming(1);
        var capture = timing.open(ApiRequestTiming.Window.BASELINE);
        timing.serve(() -> successfulTransaction(timing));
        timing.complete(capture, 25, 202);
        assertThat(timing.snapshot()).containsEntry("completed", 1).containsEntry("pending", false);
        String json = new ObjectMapper().writeValueAsString(timing.snapshot());
        assertThat(json).contains("BASELINE", "clientMillis", "serverMillis", "ACQUIRE", "EXECUTE", "COMMIT", "sqlExecutionMillis");
        assertThat(json).doesNotContain("notificationId", "recipientId", "payload", "\"sql\"", "password");
        assertThatThrownBy(() -> timing.open(ApiRequestTiming.Window.MIXED)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsOverlappingClientsAndUnscopedOrDuplicateServerWork() throws Exception {
        var timing = new ApiRequestTiming(2);
        assertThatThrownBy(() -> timing.jdbc(Part.EXECUTE, 1, false)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> timing.serve(() -> { })).isInstanceOf(IllegalStateException.class);
        var first = timing.open(ApiRequestTiming.Window.BASELINE);
        assertThatThrownBy(() -> timing.open(ApiRequestTiming.Window.MIXED)).isInstanceOf(IllegalStateException.class);
        timing.serve(() -> successfulTransaction(timing));
        assertThatThrownBy(() -> timing.serve(() -> { })).isInstanceOf(IllegalStateException.class);
        timing.complete(first, 1, 202);
        assertThatThrownBy(() -> timing.jdbc(Part.COMMIT, 1, false)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void preservesServletFailureAndAllowsFollowingProbeWithoutLeakingItsJdbcScope() throws Exception {
        var timing = new ApiRequestTiming(2);
        var first = timing.open(ApiRequestTiming.Window.MIXED);
        var original = new IOException("PRIVATE_FAILURE");
        assertThatThrownBy(() -> timing.serve(() -> { throw original; })).isSameAs(original);
        assertThatThrownBy(() -> timing.complete(first, 1, 500)).isInstanceOf(IllegalStateException.class);
        var next = timing.open(ApiRequestTiming.Window.MIXED);
        timing.serve(() -> successfulTransaction(timing));
        timing.complete(next, 1, 202);
        assertThat(timing.snapshot()).containsEntry("completed", 1).containsEntry("pending", false);
        assertThat(new ObjectMapper().writeValueAsString(timing.snapshot())).doesNotContain("PRIVATE_FAILURE");
    }

    @Test
    void rejectsUncommittedOrUnexpectedSqlAndBoundsStorageBeforeSendingAnotherRequest() throws Exception {
        assertThatThrownBy(() -> new ApiRequestTiming(5001)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ApiRequestTiming(0)).isInstanceOf(IllegalArgumentException.class);
        var timing = new ApiRequestTiming(1);
        var capture = timing.open(ApiRequestTiming.Window.MIXED);
        timing.serve(() -> timing.jdbc(Part.EXECUTE, 1, false));
        assertThatThrownBy(() -> timing.complete(capture, 1, 202)).isInstanceOf(IllegalStateException.class);
        assertThat(timing.snapshot()).containsEntry("completed", 0).containsEntry("pending", false);
    }

    private static void successfulTransaction(ApiRequestTiming timing) {
        for (var part : new Part[] {Part.ACQUIRE, Part.EXECUTE, Part.EXECUTE, Part.EXECUTE, Part.EXECUTE,
                Part.COMMIT, Part.RELEASE, Part.STORE_CALL}) timing.jdbc(part, 1000, false);
    }
}
