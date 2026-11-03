package dev.jinhyeon.notificationlab.campaign.batch;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class ApiFailurePilotSupportTest {
    @TempDir Path directory;

    @Test
    void nestedFailureHandlersKeepTheFirstSnapshotAndReason() throws Exception {
        var support = new ApiFailurePilotSupport(2, ApiFailurePilotSupport.Mode.NONE, directory);
        support.open("BASELINE");
        var report = new LinkedHashMap<String, Object>();
        assertThat(support.capture(new IOException("PRIVATE"), ApiFailureWindow.Stop.HTTP_IO_FAILURE, report)).isTrue();
        byte[] bytes = Files.readAllBytes(directory.resolve("failure.json"));
        assertThat(support.capture(new AssertionError("LATER"), ApiFailureWindow.Stop.PROTOCOL_FAILURE, report)).isFalse();
        assertThat(Files.readAllBytes(directory.resolve("failure.json"))).isEqualTo(bytes);
        assertThat(support.window.snapshot()).containsEntry("stopReason", "HTTP_IO_FAILURE");
        assertThat(report.toString()).doesNotContain("PRIVATE", "LATER", directory.toString());
        assertThat(ApiFailurePilotSupport.classify(new HttpTimeoutException("synthetic timeout"))).isEqualTo(ApiFailureWindow.Stop.HTTP_TIMEOUT);
        assertThat(ApiFailurePilotSupport.classify(new AssertionError())).isEqualTo(ApiFailureWindow.Stop.PROTOCOL_FAILURE);
    }

    @Test
    void failedPersistenceIsNotRetriedByOuterFailureHandlers() throws Exception {
        var support = new ApiFailurePilotSupport(2, ApiFailurePilotSupport.Mode.STORAGE_FAILURE, directory);
        support.open("MIXED");
        var original = new HttpTimeoutException("PRIVATE"); var report = new LinkedHashMap<String, Object>();
        assertThat(support.capture(original, ApiFailureWindow.Stop.HTTP_TIMEOUT, report)).isTrue();
        assertThat(support.capture(original, ApiFailureWindow.Stop.INFRA_FAILURE, report)).isFalse();
        assertThat(original.getSuppressed()).hasSize(1);
        assertThat(support.control()).containsEntry("earlierEvidenceUnchanged", true);
        assertThat(report.toString()).contains("written=false", "FileAlreadyExistsException").doesNotContain("PRIVATE");
    }
}
