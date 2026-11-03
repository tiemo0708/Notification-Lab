package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpTimeoutException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FailureSnapshotStoreTest {
    @TempDir Path directory;

    @Test
    void persistenceFailureAlsoPreservesTheOriginalAssertionError() throws Exception {
        var target = directory.resolve("failure.json"); Files.writeString(target, "earlier evidence");
        var original = new AssertionError("PRIVATE_ASSERTION"); var report = new LinkedHashMap<String, Object>();
        assertThat(new FailureSnapshotStore(window(), target).preserve(original, ApiFailureWindow.Stop.PROTOCOL_FAILURE, report)).isSameAs(original);
        assertThat(original.getSuppressed()).hasSize(1);
        assertThat(report.toString()).doesNotContain("PRIVATE_ASSERTION", target.toString());
    }

    @Test
    void publishesReadableFrozenEvidenceAndRepeatedSaveDoesNotReplaceIt() throws Exception {
        var window = window(); var target = directory.resolve("failure.json");
        var store = new FailureSnapshotStore(window, target);
        var first = store.save(ApiFailureWindow.Stop.HTTP_TIMEOUT);
        byte[] bytes = Files.readAllBytes(target);
        assertThat(first.bytes()).isEqualTo(bytes.length).isLessThanOrEqualTo(FailureSnapshotStore.MAX_BYTES);
        var json = new ObjectMapper().readTree(bytes);
        assertThat(json.path("stopReason").asText()).isEqualTo("HTTP_TIMEOUT");
        assertThat(json.path("activeServerOutcome").asText()).isEqualTo("UNKNOWN");
        assertThat(store.save(ApiFailureWindow.Stop.INFRA_FAILURE)).isSameAs(first);
        assertThat(Files.readAllBytes(target)).isEqualTo(bytes);
        try (var files = Files.list(directory)) { assertThat(files.toList()).containsExactly(target); }
    }

    @Test
    void neverOverwritesEarlierEvidenceAndDoesNotRetryAFailedAttempt() throws Exception {
        var target = directory.resolve("failure.json"); Files.writeString(target, "earlier evidence");
        var store = new FailureSnapshotStore(window(), target);
        assertThatThrownBy(() -> store.save(ApiFailureWindow.Stop.HTTP_TIMEOUT)).isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.readString(target)).isEqualTo("earlier evidence");
        Files.delete(target);
        assertThatThrownBy(() -> store.save(ApiFailureWindow.Stop.HTTP_TIMEOUT)).isInstanceOf(FileAlreadyExistsException.class);
        assertThat(target).doesNotExist();
        try (var files = Files.list(directory)) { assertThat(files.count()).isZero(); }
    }

    @Test
    void storageFailureKeepsTheOriginalTimeoutAndOnlyReportsASanitizedType() throws Exception {
        var target = directory.resolve("failure.json"); Files.writeString(target, "earlier evidence");
        var store = new FailureSnapshotStore(window(), target);
        var original = new HttpTimeoutException("PRIVATE_TIMEOUT");
        var report = new LinkedHashMap<String, Object>();
        assertThat(store.preserve(original, ApiFailureWindow.Stop.HTTP_TIMEOUT, report)).isSameAs(original);
        assertThat(original.getSuppressed()).hasSize(1);
        assertThat(report.toString()).contains("written=false", "FileAlreadyExistsException")
                .doesNotContain("PRIVATE_TIMEOUT", target.toString(), "earlier evidence");
        assertThat(Files.readString(target)).isEqualTo("earlier evidence");
    }

    @Test
    void concurrentPublishersExposeExactlyOneCompleteFileWithoutReplacement() throws Exception {
        var target = directory.resolve("failure.json"); var go = new CountDownLatch(1);
        var first = new FailureSnapshotStore(window(), target); var second = new FailureSnapshotStore(window(), target);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> publish(first, go)); var b = executor.submit(() -> publish(second, go));
            go.countDown();
            assertThat(a.get(3, TimeUnit.SECONDS) + b.get(3, TimeUnit.SECONDS)).isEqualTo(1);
        }
        assertThat(new ObjectMapper().readTree(Files.readAllBytes(target)).path("stopped").asBoolean()).isTrue();
        try (var files = Files.list(directory)) { assertThat(files.toList()).containsExactly(target); }
    }

    private static int publish(FailureSnapshotStore store, CountDownLatch go) throws Exception {
        if (!go.await(2, TimeUnit.SECONDS)) throw new IllegalStateException("Test start timed out");
        try { store.save(ApiFailureWindow.Stop.HTTP_TIMEOUT); return 1; }
        catch (FileAlreadyExistsException expected) { return 0; }
    }
    private static ApiFailureWindow window() {
        var window = new ApiFailureWindow(2); window.open(ApiFailureWindow.Window.CONTROL); return window;
    }
}
