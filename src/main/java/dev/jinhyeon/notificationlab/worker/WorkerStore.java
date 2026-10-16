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
    private final RetryPolicy retries;

    public WorkerStore(JdbcTemplate jdbc, NotificationStore notifications, RetryPolicy retries) {
        this.jdbc = jdbc;
        this.notifications = notifications;
        this.retries = retries;
    }

    public enum Disposition { SEND, EXHAUSTED, NEEDS_PERMIT }
    public record Claim(Notification notification, Disposition disposition) {
        public boolean send() { return disposition == Disposition.SEND; }
        public boolean needsPermit() { return disposition == Disposition.NEEDS_PERMIT; }
    }
    private record Task(String id, int attempts, int limit, int generation) { }

    @Transactional
    public Optional<Claim> claim(UUID id, UUID eventId, String token) {
        return claim(id, eventId, token, null);
    }

    @Transactional
    public Optional<Claim> claim(UUID id, UUID eventId, String token, UUID expectedTaskId) {
        return claim(id, eventId, token, expectedTaskId, true);
    }

    @Transactional
    public Optional<Claim> claim(UUID id, UUID eventId, String token, UUID expectedTaskId, boolean sendPermitted) {
        // One lock order for every claim/completion; no provider I/O while this lock is held.
        if (jdbc.queryForList("SELECT notification_id FROM notification WHERE notification_id = ? FOR UPDATE",
                String.class, id.toString()).isEmpty()) throw new IllegalStateException("Unknown notification");
        Notification notification = notifications.find(id).orElseThrow();
        var events = jdbc.queryForList("""
                SELECT retry_generation FROM outbox_event WHERE event_id = ? AND notification_id = ?
                    AND event_type IN ('NOTIFICATION_REQUESTED', 'NOTIFICATION_RETRY')
                """, Integer.class, eventId.toString(), id.toString());
        if (events.size() != 1) throw new IllegalStateException("Unknown notification event");
        Task task = task(id);
        if (expectedTaskId != null && !expectedTaskId.toString().equals(task.id())) {
            throw new IllegalStateException("Task identity mismatch");
        }
        if (events.getFirst() < task.generation()) return Optional.empty(); // A durable successor owns this work.
        if (events.getFirst() > task.generation()) throw new IllegalStateException("Future retry generation");
        if (notification.status() != NotificationStatus.QUEUED && notification.status() != NotificationStatus.PROCESSING
                && notification.status() != NotificationStatus.WAITING_RETRY) {
            return Optional.empty(); // Completed replay never consumes an attempt.
        }
        if (notification.status() == NotificationStatus.WAITING_RETRY) {
            Boolean due = jdbc.queryForObject("""
                    SELECT next_attempt_at <= UTC_TIMESTAMP(6) FROM delivery_task WHERE task_id = ?
                    """, Boolean.class, task.id());
            if (!Boolean.TRUE.equals(due)) throw new IllegalStateException("Retry is not due");
        }
        if (notification.status() == NotificationStatus.PROCESSING) {
            Boolean expired = jdbc.queryForObject("""
                    SELECT worker_lease_until <= UTC_TIMESTAMP(6) FROM notification WHERE notification_id = ?
                    """, Boolean.class, id.toString());
            if (!Boolean.TRUE.equals(expired)) throw new IllegalStateException("Worker lease is active or absent");
        }
        // Return before any writes. Redis admission happens after this transaction releases its locks.
        // The next claim revalidates identity/generation/status/lease under the same lock order.
        if (task.attempts() < task.limit() && !sendPermitted) {
            return Optional.of(new Claim(notification, Disposition.NEEDS_PERMIT));
        }
        if (notification.status() == NotificationStatus.PROCESSING) {
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
            jdbc.update("UPDATE delivery_task SET next_attempt_at = NULL WHERE task_id = ?", task.id());
            Notification exhausted = notifications.find(id).orElseThrow();
            deadLetter(exhausted, task);
            return Optional.of(new Claim(exhausted, Disposition.EXHAUSTED));
        }
        jdbc.update("""
                UPDATE notification SET status = 'PROCESSING', worker_token = ?, error_code = NULL,
                    worker_lease_until = TIMESTAMPADD(SECOND, 30, UTC_TIMESTAMP(6)) WHERE notification_id = ?
                """, token, id.toString());
        jdbc.update("UPDATE delivery_task SET attempt_count = attempt_count + 1, next_attempt_at = NULL WHERE task_id = ?", task.id());
        jdbc.update("""
                INSERT INTO delivery_attempt (task_id, attempt_no, lease_token, outcome, started_at)
                VALUES (?, ?, ?, 'STARTED', UTC_TIMESTAMP(6))
                """, task.id(), task.attempts() + 1, token);
        return Optional.of(new Claim(notifications.find(id).orElseThrow(), Disposition.SEND));
    }

    private Task task(UUID id) {
        return jdbc.queryForObject("""
                SELECT task_id, attempt_count, max_attempts, retry_generation
                FROM delivery_task WHERE notification_id = ? FOR UPDATE
                """, (rs, row) -> new Task(rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getInt(4)), id.toString());
    }

    /** True means the failed attempt and its durable successor were committed together. */
    @Transactional
    public boolean complete(Notification result, String token) {
        var owners = jdbc.queryForList("""
                SELECT worker_token FROM notification WHERE notification_id = ? AND status = 'PROCESSING' FOR UPDATE
                """, String.class, result.notificationId().toString());
        if (owners.size() != 1 || !token.equals(owners.getFirst())) {
            throw new IllegalStateException("Worker ownership was lost");
        }
        Task task = task(result.notificationId());
        boolean retry = retries.shouldRetry(result.status(), task.attempts(), task.limit());
        int changed = jdbc.update("""
                UPDATE notification SET status = ?, provider_message_id = ?, error_code = ?,
                    completed_at = ?, worker_token = NULL, worker_lease_until = NULL
                WHERE notification_id = ? AND status = 'PROCESSING' AND worker_token = ?
                """, retry ? "WAITING_RETRY" : result.status().name(), result.providerMessageId(), result.errorCode(),
                retry ? null : LocalDateTime.ofInstant(result.completedAt(), ZoneOffset.UTC),
                result.notificationId().toString(), token);
        if (changed != 1) throw new IllegalStateException("Worker ownership was lost");
        int recorded = jdbc.update("""
                UPDATE delivery_attempt SET outcome = ?, error_code = ?, completed_at = ?
                WHERE task_id = ? AND lease_token = ? AND outcome = 'STARTED'
                """, result.status().name(), result.errorCode(), LocalDateTime.ofInstant(result.completedAt(), ZoneOffset.UTC),
                task.id(), token);
        if (recorded != 1) throw new IllegalStateException("Attempt history was not recorded");
        if (retry) {
            jdbc.update("""
                    UPDATE delivery_task SET retry_generation = retry_generation + 1,
                        next_attempt_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)) WHERE task_id = ?
                    """, retries.delayMillis(task.attempts()) * 1000, task.id());
            String eventId = UUID.randomUUID().toString();
            jdbc.update("""
                    INSERT INTO outbox_event (event_id, notification_id, event_type, payload,
                        retry_generation, created_at, available_at)
                    SELECT ?, notification_id, 'NOTIFICATION_RETRY',
                        JSON_OBJECT('schemaVersion', 2, 'eventId', ?, 'notificationId', notification_id, 'taskId', task_id),
                        retry_generation, UTC_TIMESTAMP(6), next_attempt_at FROM delivery_task WHERE task_id = ?
                    """, eventId, eventId, task.id());
        } else if (result.status() != NotificationStatus.PROVIDER_ACCEPTED) {
            deadLetter(result, task);
        }
        return retry;
    }

    // Called only inside the claim/completion transaction, before acknowledging the source record.
    private void deadLetter(Notification result, Task task) {
        String eventId = UUID.randomUUID().toString();
        int inserted = jdbc.update("""
                INSERT INTO outbox_event (event_id, notification_id, event_type, payload, created_at, available_at)
                SELECT ?, notification_id, 'NOTIFICATION_DEAD_LETTER',
                    JSON_OBJECT('schemaVersion', 1, 'eventType', 'NOTIFICATION_DEAD_LETTER',
                        'eventId', ?, 'notificationId', notification_id, 'taskId', ?,
                        'sourceEventId', event_id, 'retryGeneration', retry_generation,
                        'status', ?, 'errorCode', ?, 'attemptCount', ?, 'maxAttempts', ?),
                    UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)
                FROM outbox_event WHERE notification_id = ? AND retry_generation = ?
                    AND event_type IN ('NOTIFICATION_REQUESTED', 'NOTIFICATION_RETRY')
                """, eventId, eventId, task.id(), result.status().name(), result.errorCode(),
                task.attempts(), task.limit(), result.notificationId().toString(), task.generation());
        if (inserted != 1) throw new IllegalStateException("Dead-letter handoff was not recorded");
    }
}
