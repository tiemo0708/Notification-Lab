package dev.jinhyeon.notificationlab.deadletter;

import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import dev.jinhyeon.notificationlab.notification.NotificationStore;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import dev.jinhyeon.notificationlab.outbox.SubmissionService;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

@Service
@Profile("!memory & !sync")
public class DeadLetterService {
    private final JdbcTemplate jdbc;
    private final NotificationStore notifications;
    private final SubmissionService submissions;

    public DeadLetterService(JdbcTemplate jdbc, NotificationStore notifications, SubmissionService submissions) {
        this.jdbc = jdbc;
        this.notifications = notifications;
        this.submissions = submissions;
    }

    public record Redrive(UUID sourceEventId, UUID sourceNotificationId, UUID rootNotificationId,
                          UUID targetNotificationId, int redriveNo, String reason,
                          boolean acknowledgedPossibleDuplicate, Instant createdAt) { }
    public record Result(Redrive redrive, boolean replayed) { }
    public record DeadLetter(UUID eventId, UUID notificationId, UUID taskId, String publicationStatus,
                             String status, String errorCode, int attemptCount, int maxAttempts, Redrive redrive) { }
    private record Source(String notificationId, String publicationStatus) { }
    private record Parent(String root, int number) { }

    @Transactional(readOnly = true)
    public DeadLetter find(UUID eventId) {
        var rows = jdbc.query("""
                SELECT e.notification_id, t.task_id, e.status, n.status, n.error_code, t.attempt_count, t.max_attempts
                FROM outbox_event e JOIN notification n USING (notification_id)
                JOIN delivery_task t USING (notification_id)
                WHERE e.event_id = ? AND e.event_type = 'NOTIFICATION_DEAD_LETTER'
                """, (rs, row) -> new DeadLetter(eventId, UUID.fromString(rs.getString(1)), UUID.fromString(rs.getString(2)),
                rs.getString(3), rs.getString(4), rs.getString(5), rs.getInt(6), rs.getInt(7), null), eventId.toString());
        if (rows.isEmpty()) throw new DeadLetterNotFoundException();
        var item = rows.getFirst();
        var audit = audit(eventId, false);
        return new DeadLetter(item.eventId(), item.notificationId(), item.taskId(), item.publicationStatus(),
                item.status(), item.errorCode(), item.attemptCount(), item.maxAttempts(), audit.isEmpty() ? null : audit.getFirst());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Result redrive(UUID eventId, RedriveRequest request) {
        if (request.reason() == null || !request.acknowledgePossibleDuplicate()) {
            throw new RedriveConflictException("Redrive requires a reason and duplicate-risk acknowledgement");
        }
        // Lock the selected source, serializing concurrent redrives without resetting its task or history.
        var sources = jdbc.query("""
                SELECT e.notification_id, e.status FROM outbox_event e
                JOIN notification n USING (notification_id)
                WHERE e.event_id = ? AND e.event_type = 'NOTIFICATION_DEAD_LETTER' FOR UPDATE
                """, (rs, row) -> new Source(rs.getString(1), rs.getString(2)), eventId.toString());
        if (sources.isEmpty()) throw new DeadLetterNotFoundException();
        var existing = audit(eventId, true); // Current locking read, including after waiting on another redrive.
        if (!existing.isEmpty()) {
            if (!existing.getFirst().reason().equals(request.reason().name())) {
                throw new RedriveConflictException("Dead-letter event was already redriven with a different reason");
            }
            return new Result(existing.getFirst(), true);
        }
        var source = sources.getFirst();
        if (!"PUBLISHED".equals(source.publicationStatus())) {
            throw new RedriveConflictException("Dead-letter publication must be confirmed before redrive");
        }
        // This read also works when invoked from a caller with an older repeatable-read snapshot.
        var parents = jdbc.query("""
                SELECT root_notification_id, redrive_no FROM dead_letter_redrive
                WHERE target_notification_id = ? FOR UPDATE
                """, (rs, row) -> new Parent(rs.getString(1), rs.getInt(2)), source.notificationId());
        String root = parents.isEmpty() ? source.notificationId() : parents.getFirst().root();
        int number = parents.isEmpty() ? 1 : parents.getFirst().number() + 1;
        if (number > 3) throw new RedriveConflictException("Manual redrive limit exhausted");
        var original = notifications.find(UUID.fromString(source.notificationId())).orElseThrow();
        if (original.status() != NotificationStatus.RETRYABLE_FAILURE
                && original.status() != NotificationStatus.PERMANENT_FAILURE
                && original.status() != NotificationStatus.INTERNAL_FAILURE) {
            throw new RedriveConflictException("Only terminal failures can be redriven");
        }
        int limit = jdbc.queryForObject("SELECT max_attempts FROM delivery_task WHERE notification_id = ? FOR UPDATE",
                Integer.class, source.notificationId());
        var target = submissions.accept(new NotificationRequest(original.recipientId(), original.title(), original.body()), null);
        // Inherit the original budget, even if today's configuration differs.
        jdbc.update("UPDATE delivery_task SET max_attempts = ? WHERE notification_id = ?", limit, target.notificationId().toString());
        jdbc.update("""
                INSERT INTO dead_letter_redrive (source_event_id, source_notification_id, root_notification_id,
                    target_notification_id, redrive_no, reason, acknowledged_possible_duplicate, created_at)
                VALUES (?, ?, ?, ?, ?, ?, TRUE, UTC_TIMESTAMP(6))
                """, eventId.toString(), source.notificationId(), root, target.notificationId().toString(), number, request.reason().name());
        return new Result(audit(eventId, true).getFirst(), false);
    }

    private List<Redrive> audit(UUID eventId, boolean lock) {
        return jdbc.query("""
                SELECT source_event_id, source_notification_id, root_notification_id, target_notification_id,
                    redrive_no, reason, acknowledged_possible_duplicate, created_at
                FROM dead_letter_redrive WHERE source_event_id = ?
                """ + (lock ? " FOR UPDATE" : ""), (rs, row) -> new Redrive(
                UUID.fromString(rs.getString(1)), UUID.fromString(rs.getString(2)), UUID.fromString(rs.getString(3)),
                UUID.fromString(rs.getString(4)), rs.getInt(5), rs.getString(6), rs.getBoolean(7),
                rs.getObject(8, LocalDateTime.class).toInstant(ZoneOffset.UTC)), eventId.toString());
    }
}
