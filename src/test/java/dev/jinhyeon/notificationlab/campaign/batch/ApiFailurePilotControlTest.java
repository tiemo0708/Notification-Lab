package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpTimeoutException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Assert the existing harness's outcome; an injected timeout must still be stored as FAIL. */
@Tag("api-failure-pilot-controls")
class ApiFailurePilotControlTest {
    @Test
    @Timeout(240)
    void verifiesOptInMixedPilotOutcomeWithoutConvertingFailureToSuccess() throws Exception {
        var mode = ApiFailurePilotSupport.Mode.valueOf(System.getProperty("campaign.delivery.failureMode", "NONE"));
        var pilot = new CampaignDeliveryPilotTest();
        HttpTimeoutException failure = null;
        if (mode == ApiFailurePilotSupport.Mode.NONE) pilot.reconcilesHttpDeliveryAndComparesRealtimeLatencyDuringCampaignDrain();
        else failure = assertThrows(HttpTimeoutException.class, pilot::reconcilesHttpDeliveryAndComparesRealtimeLatencyDuringCampaignDrain);
        var report = pilot.failureReport(); var window = map(report.get("apiFailureWindow"));
        String profile = System.getProperty("campaign.delivery.failureProfile", "PILOT");
        assertThat(report).containsEntry("failureObservationProfile", profile);
        if (profile.equals("REHEARSAL")) {
            assertThat(report).containsEntry("targets", 1000).containsEntry("fineTimingEnabled", false)
                    .containsEntry("offsetComparison", false).containsEntry("campaignOffsetMode", "RECORD")
                    .containsEntry("campaignRelayInstances", 1).containsEntry("publishBatchSize", 1)
                    .containsEntry("relayScheduling", "application-scheduled-bean").doesNotContainKey("fineTimings");
        }
        var control = map(report.get("failureWindowControl")); var file = pilot.failureDirectory().resolve("failure.json");
        assertThat(window.get("recent")).asList().hasSize(32);
        if (mode == ApiFailurePilotSupport.Mode.NONE) {
            assertThat(report).containsEntry("result", "PASS");
            assertThat(window).containsEntry("stopped", false).containsEntry("activePresent", false);
            assertThat((Integer) window.get("completed")).isEqualTo(50 + (Integer) report.get("mixedProbes"));
            assertThat(control).containsEntry("injected", false).containsEntry("captured", false).containsEntry("fileExists", false);
            assertThat(file).doesNotExist();
        } else {
            assertThat(report).containsEntry("result", "FAIL").containsEntry("failureType", "HttpTimeoutException");
            assertThat(map(report.get("probeFailure"))).containsEntry("serverOutcome", "UNKNOWN")
                    .containsEntry("timeoutMillis", 5000L).containsEntry("acceptedResponses", 50);
            assertThat(window).containsEntry("stopReason", "HTTP_TIMEOUT").containsEntry("completed", 50)
                    .containsEntry("opened", 51).containsEntry("evicted", 18).containsEntry("activeServerOutcome", "UNKNOWN");
            assertThat(map(window.get("active"))).containsEntry("activePart", "AUTOCOMMIT_STATE")
                    .containsEntry("commitReturned", true).containsEntry("responseObserved", false);
            assertThat(map(report.get("failureCaptureLive"))).containsEntry("poolAvailable", true).containsEntry("poolClosed", false)
                    .containsEntry("gateReached", true).containsEntry("gateReleased", false).containsEntry("gateAwaitActive", true);
            assertThat(map(report.get("failureCaptureContext"))).containsEntry("available", true).containsEntry("realtimeActive", true);
            assertThat(control).containsEntry("injected", true).containsEntry("captured", true).containsEntry("gateReleased", true);
            var storage = map(report.get("failureSnapshotStorage"));
            if (mode == ApiFailurePilotSupport.Mode.TIMEOUT) {
                assertThat(storage).containsEntry("written", true);
                assertThat(Files.readAllBytes(file)).isEqualTo(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(window));
                assertThat(failure.getSuppressed()).isEmpty();
            } else {
                assertThat(storage).containsEntry("written", false).containsEntry("errorType", "FileAlreadyExistsException");
                assertThat(Files.readString(file)).isEqualTo(ApiFailurePilotSupport.EARLIER_EVIDENCE);
                assertThat(control).containsEntry("earlierEvidenceUnchanged", true);
                assertThat(failure.getSuppressed()).hasSize(1);
            }
        }
        var verified = new LinkedHashMap<String, Object>(); verified.put("mode", mode.name());
        verified.put("profile", profile);
        verified.put("verification", "PASS"); verified.put("harnessResult", report.get("result"));
        verified.put("expectedPrimaryFailureType", failure == null ? "NONE" : failure.getClass().getSimpleName());
        verified.put("primarySuppressedCount", failure == null ? 0 : failure.getSuppressed().length);
        verified.put("failureFileExists", Files.exists(file));
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(pilot.failureDirectory().resolve("verification.json").toFile(), verified);
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> map(Object value) { return (Map<String, Object>) value; }
}
