package dev.jinhyeon.notificationlab.campaign.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Bounded method labels and wall times only; no connection IDs, SQL, arguments or failures retained. */
final class ApiConnectionTiming {
    enum Method { GET_AUTO_COMMIT, SET_AUTO_COMMIT_FALSE, SET_AUTO_COMMIT_TRUE }
    enum Boundary { BEFORE_FIRST_SQL, BEFORE_COMPLETION, AFTER_COMMIT, AFTER_ROLLBACK }
    private record Call(Method method, Boundary boundary, long nanos, boolean failed) { }
    private final List<Call> calls = new ArrayList<>();

    void record(Method method, Boundary boundary, long nanos, boolean failed) {
        if (calls.size() >= 8 || nanos < 0) throw new IllegalStateException("Connection call timing bound exceeded");
        calls.add(new Call(method, boundary, nanos, failed));
    }

    void verifySuccessfulAdmission(long aggregateCount, long aggregateNanos) {
        if (aggregateCount != calls.size() || calls.stream().mapToLong(Call::nanos).sum() > aggregateNanos
                || calls.stream().anyMatch(Call::failed)
                || !calls.stream().map(Call::method).toList().equals(List.of(Method.GET_AUTO_COMMIT,
                        Method.SET_AUTO_COMMIT_FALSE, Method.SET_AUTO_COMMIT_TRUE))
                || !calls.stream().map(Call::boundary).toList().equals(List.of(Boundary.BEFORE_FIRST_SQL,
                        Boundary.BEFORE_FIRST_SQL, Boundary.AFTER_COMMIT))) {
            throw new IllegalStateException("Unexpected admission connection call boundaries");
        }
    }

    List<Map<String, Object>> snapshot() {
        var rows = new ArrayList<Map<String, Object>>();
        for (var call : calls) rows.add(Map.of("ordinal", rows.size() + 1, "method", call.method().name(),
                "boundary", call.boundary().name(), "millis", call.nanos() / 1_000_000.0, "failed", call.failed()));
        return List.copyOf(rows);
    }
}
