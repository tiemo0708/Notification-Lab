package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PilotFinalizationTest {
    @TempDir Path directory;

    @Test
    void actualReportWriteFailureCannotReplaceThePendingHttpTimeout() throws Exception {
        var primary = new HttpTimeoutException("PRIVATE_TIMEOUT");
        var target = Files.createDirectory(directory.resolve("PRIVATE_TARGET"));
        var report = new LinkedHashMap<String, Object>(); report.put("result", "FAIL");
        var calls = new AtomicInteger();
        var thrown = assertThrows(HttpTimeoutException.class, () -> {
            try { throw primary; }
            finally {
                PilotFinalization.run(primary, report, () -> {
                    calls.incrementAndGet();
                    new ObjectMapper().writeValue(target.toFile(), report);
                });
            }
        });
        assertThat(thrown).isSameAs(primary);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(thrown.getSuppressed()).hasSize(1);
        assertThat(thrown.getSuppressed()[0]).isInstanceOf(IOException.class).hasMessage("Pilot finalization failed").hasNoCause();
        assertThat(report.toString()).contains("FileNotFoundException", "originalFailureRetained=true").doesNotContain("PRIVATE", directory.toString());
        assertThat(target).isDirectory();
    }

    @Test
    void reportFailureWithoutAPriorFailurePropagatesAndInvalidatesPass() {
        var original = new IOException("PRIVATE_WRITE");
        var report = new LinkedHashMap<String, Object>(); report.put("result", "PASS");
        var thrown = assertThrows(IOException.class, () -> PilotFinalization.run(null, report, () -> { throw original; }));
        assertThat(thrown).isSameAs(original);
        assertThat(report).containsEntry("result", "FAIL").containsEntry("failureType", "IOException");
        assertThat(report.toString()).contains("originalFailureRetained=false").doesNotContain("PRIVATE_WRITE");
    }

    @Test
    void finalAssertionCannotReplaceAnEarlierAssertionOrItsExistingSuppressedFailure() {
        var primary = new AssertionError("PRIVATE_PRIMARY");
        var earlier = new IOException("earlier synthetic storage failure"); primary.addSuppressed(earlier);
        var report = new LinkedHashMap<String, Object>();
        var thrown = assertThrows(AssertionError.class, () -> {
            try { throw primary; }
            finally { PilotFinalization.run(primary, report, () -> { throw new AssertionError("PRIVATE_FINAL"); }); }
        });
        assertThat(thrown).isSameAs(primary);
        assertThat(thrown.getSuppressed()).hasSize(2);
        assertThat(thrown.getSuppressed()[0]).isSameAs(earlier);
        assertThat(thrown.getSuppressed()[1]).hasMessage("Pilot finalization failed").hasNoCause();
        assertThat(report.toString()).contains("AssertionError").doesNotContain("PRIVATE");
    }
}
