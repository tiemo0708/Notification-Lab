package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Part;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiFailureWindowTest {
    @Test
    void retainsOnlyTheLast32CompletedRequestsAfter5000Admissions() {
        var window = new ApiFailureWindow(6000);
        for (int i = 0; i < 5000; i++) {
            window.open(ApiFailureWindow.Window.MIXED); prefix(window); finish(window); window.complete(202, 1);
        }
        var state = window.snapshot();
        assertThat(state).containsEntry("opened", 5000).containsEntry("completed", 5000).containsEntry("evicted", 4968)
                .containsEntry("activePresent", false).containsEntry("stopped", false);
        var recent = recent(state);
        assertThat(recent).hasSize(32);
        assertThat(recent.getFirst()).containsEntry("ordinal", 4969).containsEntry("responseObserved", true);
        assertThat(recent.getLast()).containsEntry("ordinal", 5000);
        assertThatThrownBy(() -> recent.getFirst().put("ordinal", 0)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void freezesAnUnfinishedCommitWithoutInventingItsDurationOrOverwritingItWithLateCompletion() throws Exception {
        var clock = new AtomicLong();
        var window = new ApiFailureWindow(10, clock::get);
        window.open(ApiFailureWindow.Window.BASELINE); prefix(window); finish(window); window.complete(202, 1);
        window.open(ApiFailureWindow.Window.MIXED); prefix(window); window.starting(Part.COMMIT);
        clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
        var frozen = window.freeze(ApiFailureWindow.Stop.HTTP_TIMEOUT);
        assertThat(frozen).containsEntry("activeServerOutcome", "UNKNOWN").containsEntry("completed", 1);
        assertThat(active(frozen)).containsEntry("activePart", "COMMIT").containsEntry("activePartAgeMillis", 5000.0)
                .containsEntry("commitReturned", false).containsEntry("responseObserved", false);
        @SuppressWarnings("unchecked") var parts = (Map<String, Map<String, Object>>) active(frozen).get("parts");
        assertThat(parts.get("COMMIT")).containsEntry("count", 0L); // Still running; 0 completed calls is not a 0ms wait.
        window.record(Part.COMMIT, 9, false); finish(window); window.complete(202, 6000);
        clock.addAndGet(TimeUnit.SECONDS.toNanos(20));
        assertThat(window.freeze(ApiFailureWindow.Stop.INFRA_FAILURE)).isSameAs(frozen);
        assertThat(window.snapshot()).isSameAs(frozen);
        assertThatThrownBy(() -> window.open(ApiFailureWindow.Window.MIXED)).isInstanceOf(IllegalStateException.class);
        assertThat(new ObjectMapper().writeValueAsString(frozen)).doesNotContain("threadId", "sessionId", "SQL_TEXT", "password", "recipient", "payload");
    }

    @Test
    void commitReturnRemainsEvidenceRatherThanAnObservedHttpOutcome() {
        var window = new ApiFailureWindow(2);
        window.open(ApiFailureWindow.Window.CONTROL); prefix(window); call(window, Part.COMMIT);
        window.starting(Part.AUTOCOMMIT_STATE);
        var frozen = window.freeze(ApiFailureWindow.Stop.HTTP_TIMEOUT);
        assertThat(frozen).containsEntry("activeServerOutcome", "UNKNOWN");
        assertThat(active(frozen)).containsEntry("commitReturned", true).containsEntry("storeReturned", false)
                .containsEntry("activePart", "AUTOCOMMIT_STATE").containsEntry("responseObserved", false);
    }

    @Test
    void stopsBeforeOpeningAnExtraProbeAndRejectsAnIncomplete202() {
        assertThatThrownBy(() -> new ApiFailureWindow(150001)).isInstanceOf(IllegalArgumentException.class);
        var bounded = new ApiFailureWindow(1);
        bounded.open(ApiFailureWindow.Window.CONTROL); prefix(bounded); finish(bounded); bounded.complete(202, 1);
        assertThatThrownBy(() -> bounded.open(ApiFailureWindow.Window.MIXED)).isInstanceOf(IllegalStateException.class);
        assertThat(bounded.snapshot()).containsEntry("stopReason", "PROBE_LIMIT").containsEntry("opened", 1).containsEntry("activePresent", false);
        var incomplete = new ApiFailureWindow(2);
        incomplete.open(ApiFailureWindow.Window.CONTROL);
        assertThatThrownBy(() -> incomplete.complete(202, 1)).isInstanceOf(IllegalStateException.class);
        assertThat(incomplete.snapshot()).containsEntry("stopReason", "PROTOCOL_FAILURE").containsEntry("completed", 0);
        assertThat(active(incomplete.snapshot())).containsEntry("responseObserved", true).containsEntry("clientStatus", 202);
        var rejected = new ApiFailureWindow(2); rejected.open(ApiFailureWindow.Window.CONTROL);
        assertThatThrownBy(() -> rejected.complete(503, 1)).isInstanceOf(IllegalStateException.class);
        assertThat(rejected.snapshot()).containsEntry("stopReason", "HTTP_STATUS").containsEntry("completed", 0);
        assertThat(active(rejected.snapshot())).containsEntry("responseObserved", true).containsEntry("clientStatus", 503);
        assertThat(rejected.snapshot()).containsEntry("activeServerOutcome", "UNKNOWN");
    }

    @Test
    void rejectsOverlapCrossThreadCallbacksAndUnboundedCallNesting() throws Exception {
        var window = new ApiFailureWindow(2); window.open(ApiFailureWindow.Window.CONTROL);
        assertThatThrownBy(() -> window.open(ApiFailureWindow.Window.MIXED)).isInstanceOf(IllegalStateException.class);
        window.starting(Part.STORE_CALL);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var future = executor.submit(() -> window.starting(Part.ACQUIRE));
            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
        }
        window.starting(Part.EXECUTE);
        assertThatThrownBy(() -> window.starting(Part.EXECUTE)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> window.record(Part.COMMIT, 1, false)).isInstanceOf(IllegalStateException.class);
        assertThat(active(window.freeze(ApiFailureWindow.Stop.PROTOCOL_FAILURE))).containsEntry("activePart", "EXECUTE");
    }

    private static void prefix(ApiFailureWindow window) {
        window.starting(Part.STORE_CALL); call(window, Part.ACQUIRE);
        state(window, ApiConnectionTiming.Method.GET_AUTO_COMMIT, ApiConnectionTiming.Boundary.BEFORE_FIRST_SQL);
        state(window, ApiConnectionTiming.Method.SET_AUTO_COMMIT_FALSE, ApiConnectionTiming.Boundary.BEFORE_FIRST_SQL);
        for (int i = 0; i < 4; i++) call(window, Part.EXECUTE);
    }
    private static void finish(ApiFailureWindow window) {
        call(window, Part.COMMIT);
        state(window, ApiConnectionTiming.Method.SET_AUTO_COMMIT_TRUE, ApiConnectionTiming.Boundary.AFTER_COMMIT);
        call(window, Part.RELEASE); window.record(Part.STORE_CALL, 100, false);
    }
    private static void state(ApiFailureWindow window, ApiConnectionTiming.Method method, ApiConnectionTiming.Boundary boundary) {
        window.starting(Part.AUTOCOMMIT_STATE); window.connectionCall(method, boundary, 1, false); window.record(Part.AUTOCOMMIT_STATE, 2, false);
    }
    private static void call(ApiFailureWindow window, Part part) { window.starting(part); window.record(part, 1, false); }
    @SuppressWarnings("unchecked") private static Map<String, Object> active(Map<String, Object> state) { return (Map<String, Object>) state.get("active"); }
    @SuppressWarnings("unchecked") private static List<Map<String, Object>> recent(Map<String, Object> state) { return (List<Map<String, Object>>) state.get("recent"); }
}
