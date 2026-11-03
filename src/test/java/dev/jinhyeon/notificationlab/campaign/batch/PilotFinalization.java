package dev.jinhyeon.notificationlab.campaign.batch;

import java.io.IOException;
import java.util.Map;

/** Preserve the workload failure if final diagnostics or report persistence also fails. */
final class PilotFinalization {
    @FunctionalInterface interface Action { void run() throws Exception; }

    private PilotFinalization() { }

    static void run(Throwable primary, Map<String, Object> report, Action action) throws Exception {
        try {
            action.run();
        } catch (Exception | AssertionError failure) {
            report.put("finalizationFailure", Map.of("errorType", failure.getClass().getSimpleName(),
                    "originalFailureRetained", primary != null));
            if (primary == null) {
                report.put("result", "FAIL");
                report.put("failureType", failure.getClass().getSimpleName());
                throw failure;
            }
            primary.addSuppressed(new IOException("Pilot finalization failed"));
        }
    }
}
