package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.jdbc.JdbcConnection;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static dev.jinhyeon.notificationlab.campaign.batch.DeliveryTiming.Part;
import static org.assertj.core.api.Assertions.assertThat;

class ApiDbObservationTest {
    @Test
    void rejectsLateQueryEvenWhenTheNextRequestReusesTheSamePhysicalSession() throws Exception {
        var queries = new AtomicInteger();
        var reference = new ApiDbObservation[1];
        var observation = new ApiDbObservation(session -> {
            assertThat(session).isEqualTo(314159265);
            if (queries.getAndIncrement() == 0) {
                finish(reference[0]); begin(reference[0]); // Same native session; a different call window.
            }
            return new ApiDbObservation.Observation(true, true, ApiDbObservation.Kind.INSERT, 1);
        }, (p, n, f) -> { }, () -> "CONTROL", true);
        reference[0] = observation;
        try (observation) {
            begin(observation); observation.pollOnce(); observation.pollOnce(); finish(observation);
            assertThat(observation.snapshot()).containsEntry("staleDiscarded", 1).containsEntry("lockSamples", 1);
            var rows = rows(observation);
            assertThat(rows.get(0)).containsEntry("status", "NOT_OBSERVED");
            assertThat(rows.get(1)).containsEntry("status", "OBSERVED");
            assertThat(new ObjectMapper().writeValueAsString(observation.snapshot())).doesNotContain("314159265", "sessionId", "threadId", "eventId", "SQL_TEXT");
        }
    }

    @Test
    void capsQueriesAndPerRequestSamplesWithoutInventingUnobservedIntervals() throws Exception {
        try (var observation = new ApiDbObservation(s -> new ApiDbObservation.Observation(true, false, ApiDbObservation.Kind.NONE, 0),
                (p, n, f) -> { }, () -> "CONTROL", true)) {
            begin(observation);
            for (int i = 0; i < 600; i++) observation.pollOnce();
            finish(observation);
            assertThat(observation.snapshot()).containsEntry("queries", 512).containsEntry("queryBudgetReached", true);
            assertThat(rows(observation).getFirst()).containsEntry("samplesNotRetained", 504);
            assertThat((List<?>) rows(observation).getFirst().get("samples")).hasSize(8);
        }
    }

    @Test
    void stopsAfterThreeObserverErrorsAndDoesNotExportDriverMessages() throws Exception {
        try (var observation = new ApiDbObservation(s -> { throw new SQLException("PRIVATE_MESSAGE"); },
                (p, n, f) -> { }, () -> "CONTROL", true)) {
            begin(observation);
            for (int i = 0; i < 10; i++) observation.pollOnce();
            finish(observation);
            assertThat(observation.snapshot()).containsEntry("queries", 3).containsEntry("errors", 3);
            assertThat(rows(observation).getFirst()).containsEntry("status", "NOT_OBSERVED");
            assertThat(new ObjectMapper().writeValueAsString(observation.snapshot())).doesNotContain("PRIVATE_MESSAGE");
        }
    }

    @Test
    void disabledModeDoesNotQueryAndStillForwardsExistingTimingCallbacks() throws Exception {
        var forwarded = new AtomicInteger();
        try (var observation = new ApiDbObservation(s -> { throw new AssertionError("Observer must remain off"); },
                (p, n, f) -> forwarded.incrementAndGet(), () -> "CONTROL", false)) {
            begin(observation); observation.pollOnce(); finish(observation);
            assertThat(observation.snapshot()).containsEntry("queries", 0).containsEntry("errors", 0);
            assertThat(rows(observation).getFirst()).containsEntry("status", "DISABLED");
            assertThat(forwarded.get()).isEqualTo(2);
        }
    }

    private static void begin(ApiDbObservation observation) throws SQLException {
        observation.starting(Part.STORE_CALL); observation.connection(connection()); observation.starting(Part.EXECUTE);
    }
    private static void finish(ApiDbObservation observation) {
        observation.record(Part.EXECUTE, 1, false); observation.record(Part.STORE_CALL, 1, false);
    }
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rows(ApiDbObservation observation) { return (List<Map<String, Object>>) observation.snapshot().get("rows"); }
    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(JdbcConnection.class.getClassLoader(), new Class<?>[] {JdbcConnection.class}, (proxy, method, args) -> switch (method.getName()) {
            case "getId" -> 314159265L;
            case "unwrap" -> proxy;
            default -> throw new AssertionError("No application SQL or extra JDBC operations may be introduced");
        });
    }
}
