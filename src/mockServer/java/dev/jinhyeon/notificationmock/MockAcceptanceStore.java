package dev.jinhyeon.notificationmock;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** One embedded store per Mock process. The endpoint serializes reads and writes. */
@Component
final class MockAcceptanceStore {
    record Acceptance(byte[] fingerprint, UUID providerId) { }

    private final Connection connection;
    private final boolean persistent;
    private volatile int count;

    MockAcceptanceStore(HttpMockProperties properties) throws IOException, SQLException {
        String file = properties.idempotencyFile();
        persistent = !":memory:".equals(file);
        connection = DriverManager.getConnection(persistent ? fileUrl(file) : "jdbc:h2:mem:", "sa", "");
        try (var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS mock_acceptance (
                        notification_id UUID PRIMARY KEY,
                        fingerprint BINARY(32) NOT NULL,
                        provider_id UUID NOT NULL
                    )
                    """);
            try (var rows = statement.executeQuery("SELECT COUNT(*) FROM mock_acceptance")) {
                rows.next();
                count = rows.getInt(1);
            }
        } catch (SQLException failure) {
            try {
                connection.close();
            } catch (SQLException closing) {
                failure.addSuppressed(closing);
            }
            throw failure;
        }
    }

    private static String fileUrl(String file) throws IOException {
        Path path = Path.of(file).toAbsolutePath().normalize();
        if (path.toString().contains(";")) throw new IllegalArgumentException("Invalid Mock idempotency file path");
        Files.createDirectories(path.getParent());
        return "jdbc:h2:file:" + path + ";WRITE_DELAY=0";
    }

    Acceptance find(UUID key) throws SQLException {
        try (var query = connection.prepareStatement(
                "SELECT fingerprint, provider_id FROM mock_acceptance WHERE notification_id = ?")) {
            query.setObject(1, key);
            try (var row = query.executeQuery()) {
                return row.next() ? new Acceptance(row.getBytes(1), row.getObject(2, UUID.class)) : null;
            }
        }
    }

    void save(UUID key, byte[] fingerprint, UUID providerId) throws SQLException {
        try (var insert = connection.prepareStatement("INSERT INTO mock_acceptance VALUES (?, ?, ?)")) {
            insert.setObject(1, key);
            insert.setBytes(2, fingerprint);
            insert.setObject(3, providerId);
            insert.executeUpdate();
        }
        if (persistent) {
            try (var sync = connection.createStatement()) {
                sync.execute("CHECKPOINT SYNC");
            }
        }
        count++;
    }

    int count() { return count; }

    @PreDestroy
    void close() throws SQLException {
        connection.close();
    }
}
