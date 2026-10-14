package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import dev.jinhyeon.notificationlab.notification.NotificationStore;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Profile("!memory & !sync")
public class WorkerStore {
    private final JdbcTemplate jdbc;
    private final NotificationStore notifications;

    public WorkerStore(JdbcTemplate jdbc, NotificationStore notifications) {
        this.jdbc = jdbc;
        this.notifications = notifications;
    }

    public record Claim(Notification notification, boolean send) { }
    private record Task(String id, int attempts, int limit) { }

    @Transactional
    public Optional<Claim> claim(UUID id, UUID eventId, String token) {
        return claim(id, eventId, token, null);
    }

    @Transactional
    public Optional<Claim> claim(UUID id, UUID eventId, String token, UUID expectedTaskId) {
        // One lock order for every claim/completion; no provider I/O while this lock is held.
        if (jdbc.queryForList("SELECT notification_id FROM notification WHERE notification_id = ? FOR UPDATE",
                String.class, id.toString()).isEmpty()) throw new IllegalStateException("Unknown notification");
        Notification notification = notifications.find(id).orElseThrow();
        int events = jdbc.queryForObject("""
                SELECT COUNT(*) FROM outbox_event WHERE event_id = ? AND notification_id = ?
                    AND event_type = 'NOTIFICATION_REQUESTED'
                """, Integer.class, eventId.toString(), id.toString());
        if (events != 1) throw new IllegalStateException("Unknown notification event");
        Task task = jdbc.queryForObject("""
                SELECT task_id, attempt_count, max_attempts FROM delivery_task WHERE notification_id = ? FOR UPDATE
                """, (rs, row) -> new Task(rs.getString(1), rs.getInt(2), rs.getInt(3)), id.toString());
        if (expectedTaskId != null && !expectedTaskId.toString().equals(task.id())) {
            throw new IllegalStateException("Task identity mismatch");
        }
        if (notification.status() != NotificationStatus.QUEUED && notification.status() != NotificationStatus.PROCESSING) {
            return Optional.empty(); // Completed replay never consumes an attempt.
        }
        if (notification.status() == NotificationStatus.PROCESSING) {
            Boolean expired = jdbc.queryForObject("""
                    SELECT worker_lease_until <= UTC_TIMESTAMP(6) FROM notification WHERE notification_id = ?
                    """, Boolean.class, id.toString());
            if (!Boolean.TRUE.equals(expired)) throw new IllegalStateException("Worker lease is active or absent");
            jdbc.update("""
                    UPDATE delivery_attempt SET outcome = 'UNKNOWN', error_code = 'OUTCOME_UNKNOWN',
                        completed_at = UTC_TIMESTAMP(6) WHERE task_id = ? AND outcome = 'STARTED'
                    """, task.id());
        }
        if (task.attempts() >= task.limit()) {
            jdbc.update("""
                    UPDATE notification SET status = 'INTERNAL_FAILURE', error_code = 'ATTEMPTS_EXHAUSTED',
                        completed_at = UTC_TIMESTAMP(6), worker_token = NULL, worker_lease_until = NULL
                    WHERE notification_id = ?
                    """, id.toString());
            return Optional.of(new Claim(notifications.find(id).orElseThrow(), false));
        }
        jdbc.update("""
                UPDATE notification SET status = 'PROCESSING', worker_token = ?,
                    worker_lease_until = TIMESTAMPADD(SECOND, 30, UTC_TIMESTAMP(6)) WHERE notification_id = ?
                """, token, id.toString());
        jdbc.update("UPDATE delivery_task SET attempt_count = attempt_count + 1 WHERE task_id = ?", task.id());
        jdbc.update("""
                INSERT INTO delivery_attempt (task_id, attempt_no, lease_token, outcome, started_at)
                VALUES (?, ?, ?, 'STARTED', UTC_TIMESTAMP(6))
                """, task.id(), task.attempts() + 1, token);
        return Optional.of(new Claim(notifications.find(id).orElseThrow(), true));
    }

    @Transactional
    public void complete(Notification result, String token) {
        int changed = jdbc.update("""
                UPDATE notification SET status = ?, provider_message_id = ?, error_code = ?,
                    completed_at = ?, worker_token = NULL, worker_lease_until = NULL
                WHERE notification_id = ? AND status = 'PROCESSING' AND worker_token = ?
                """, result.status().name(), result.providerMessageId(), result.errorCode(),
                LocalDateTime.ofInstant(result.completedAt(), ZoneOffset.UTC), result.notificationId().toString(), token);
        if (changed != 1) throw new IllegalStateException("Worker ownership was lost");
        int recorded = jdbc.update("""
                UPDATE delivery_attempt SET outcome = ?, error_code = ?, completed_at = ?
                WHERE task_id IN (SELECT task_id FROM delivery_task WHERE notification_id = ?)
                    AND lease_token = ? AND outcome = 'STARTED'
                """, result.status().name(), result.errorCode(), LocalDateTime.ofInstant(result.completedAt(), ZoneOffset.UTC),
                result.notificationId().toString(), token);
        if (recorded != 1) throw new IllegalStateException("Attempt history was not recorded");
    }
}
