package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/** Publish a complete first-stop snapshot once, without replacing an existing evidence file. */
final class FailureSnapshotStore {
    static final int MAX_BYTES = 256 * 1024;
    record Saved(Path path, int bytes) { }
    private final ApiFailureWindow window;
    private final Path target;
    private Saved saved;
    private IOException failed;

    FailureSnapshotStore(ApiFailureWindow window, Path target) {
        this.window = window; this.target = target.toAbsolutePath().normalize();
    }

    synchronized Saved save(ApiFailureWindow.Stop reason) throws IOException {
        if (saved != null) return saved;
        if (failed != null) throw failed; // No implicit retry after a failed write.
        var snapshot = window.freeze(reason);
        Path temporary = null;
        try {
            byte[] bytes = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(snapshot);
            if (bytes.length > MAX_BYTES) throw new IOException("Failure snapshot size bound exceeded");
            Files.createDirectories(target.getParent());
            temporary = Files.createTempFile(target.getParent(), ".failure-", ".part");
            try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                var buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            // Same-directory hard-link publication cannot replace an existing target. No unsafe move fallback.
            Files.createLink(target, temporary);
            saved = new Saved(target, bytes.length);
            return saved;
        } catch (IOException failure) {
            failed = failure; throw failure;
        } catch (RuntimeException failure) {
            failed = new IOException("Failure snapshot publication unsupported"); throw failed;
        } finally {
            if (temporary != null) {
                try { Files.deleteIfExists(temporary); }
                catch (IOException ignored) { /* A published target stays valid; cleanup must not replace the primary outcome. */ }
            }
        }
    }

    <E extends Throwable> E preserve(E original, ApiFailureWindow.Stop reason, Map<String, Object> report) {
        try {
            var result = save(reason);
            report.put("failureSnapshotStorage", Map.of("written", true, "fileName", result.path().getFileName().toString(),
                    "bytes", result.bytes(), "maxBytes", MAX_BYTES));
        } catch (IOException failure) {
            report.put("failureSnapshotStorage", Map.of("written", false, "errorType", failure.getClass().getSimpleName()));
            original.addSuppressed(new IOException("Failure snapshot persistence failed")); // No raw paths/messages in the report or suppressed message.
        }
        return original;
    }
}
