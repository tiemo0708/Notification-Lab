package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import dev.jinhyeon.notificationlab.notification.Notification;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Profile("!memory & !sync")
public class DeliveryTaskStore {
    private final JdbcTemplate jdbc;
    private final DeliveryProperties properties;

    public DeliveryTaskStore(JdbcTemplate jdbc, DeliveryProperties properties) {
        this.jdbc = jdbc;
        this.properties = properties;
    }

    @Transactional
    public UUID create(Notification notification) {
        UUID taskId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO delivery_task (task_id, notification_id, target_id, max_attempts, created_at)
                VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6))
                """, taskId.toString(), notification.notificationId().toString(),
                notification.recipientId().getBytes(StandardCharsets.UTF_8), properties.maxAttempts());
        return taskId;
    }

    public record Attempt(int attemptNo, String outcome, String errorCode, Instant startedAt, Instant completedAt) { }
    public record Delivery(UUID taskId, String status, int attemptCount, int maxAttempts, List<Attempt> attempts) { }

    @Transactional(readOnly = true)
    public Optional<Delivery> find(UUID notificationId) {
        var tasks = jdbc.query("""
                SELECT t.task_id, n.status, t.attempt_count, t.max_attempts
                FROM delivery_task t JOIN notification n ON n.notification_id = t.notification_id
                WHERE t.notification_id = ?
                """, (rs, row) -> new Delivery(UUID.fromString(rs.getString(1)), rs.getString(2),
                rs.getInt(3), rs.getInt(4), List.of()), notificationId.toString());
        if (tasks.isEmpty()) return Optional.empty();
        var task = tasks.getFirst();
        var attempts = jdbc.query("""
                SELECT attempt_no, outcome, error_code, started_at, completed_at FROM delivery_attempt
                WHERE task_id = ? ORDER BY attempt_no
                """, (rs, row) -> new Attempt(rs.getInt(1), rs.getString(2), rs.getString(3),
                instant(rs.getObject(4, LocalDateTime.class)), instant(rs.getObject(5, LocalDateTime.class))),
                task.taskId().toString());
        return Optional.of(new Delivery(task.taskId(), task.status(), task.attemptCount(), task.maxAttempts(), attempts));
    }

    private static Instant instant(LocalDateTime time) {
        return time == null ? null : time.toInstant(ZoneOffset.UTC);
    }
}
