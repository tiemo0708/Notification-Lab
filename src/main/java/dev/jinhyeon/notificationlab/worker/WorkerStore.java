package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import dev.jinhyeon.notificationlab.notification.NotificationStore;
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

    @Transactional
    public Optional<Notification> claim(UUID id, UUID eventId, String token) {
        int events = jdbc.queryForObject("""
                SELECT COUNT(*) FROM outbox_event WHERE event_id = ? AND notification_id = ?
                    AND event_type = 'NOTIFICATION_REQUESTED'
                """, Integer.class, eventId.toString(), id.toString());
        if (events != 1) throw new IllegalStateException("Unknown notification event");
        int changed = jdbc.update("""
                UPDATE notification SET status = 'PROCESSING', worker_token = ?,
                    worker_lease_until = TIMESTAMPADD(SECOND, 30, UTC_TIMESTAMP(6))
                WHERE notification_id = ? AND (status = 'QUEUED'
                    OR (status = 'PROCESSING' AND worker_lease_until <= UTC_TIMESTAMP(6)))
                """, token, id.toString());
        Notification notification = notifications.find(id).orElseThrow();
        if (changed == 1) return Optional.of(notification);
        if (notification.status() == NotificationStatus.QUEUED
                || notification.status() == NotificationStatus.PROCESSING) {
            throw new IllegalStateException("Notification is owned by an active worker");
        }
        return Optional.empty(); // Durable terminal result: replay only commits the offset.
    }

    @Transactional
    public void complete(Notification result, String token) {
        int changed = jdbc.update("""
                UPDATE notification SET status = ?, provider_message_id = ?, error_code = ?,
                    completed_at = ?, worker_token = NULL, worker_lease_until = NULL
                WHERE notification_id = ? AND status = 'PROCESSING' AND worker_token = ?
                """, result.status().name(), result.providerMessageId(), result.errorCode(),
                java.time.LocalDateTime.ofInstant(result.completedAt(), java.time.ZoneOffset.UTC),
                result.notificationId().toString(), token);
        if (changed != 1) throw new IllegalStateException("Worker ownership was lost");
    }
}
