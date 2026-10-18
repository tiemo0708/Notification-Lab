package dev.jinhyeon.notificationlab.outbox;

import dev.jinhyeon.notificationlab.config.TrafficProperties;
import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import dev.jinhyeon.notificationlab.notification.JdbcNotificationStore;
import java.time.Clock;
import dev.jinhyeon.notificationlab.worker.DeliveryTaskStore;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("!memory & !sync")
public class SubmissionService {
    private final JdbcNotificationStore notifications;
    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final DeliveryTaskStore tasks;
    private final TrafficProperties traffic;

    public SubmissionService(JdbcNotificationStore notifications, JdbcTemplate jdbc, Clock clock,
                             DeliveryTaskStore tasks, TrafficProperties traffic) {
        this.notifications = notifications;
        this.jdbc = jdbc;
        this.clock = clock;
        this.tasks = tasks;
        this.traffic = traffic;
    }

    // The proxy commits the notification, task and outbox rows before the controller can return HTTP 202.
    @Transactional
    public Notification accept(NotificationRequest request, String idempotencyKey) {
        var notification = new Notification(UUID.randomUUID(), request.recipientId(), request.title(),
                request.body(), NotificationStatus.QUEUED, null, null, clock.instant(), null);
        notifications.create(notification, idempotencyKey);
        jdbc.update("UPDATE notification SET traffic_class = ? WHERE notification_id = ?",
                traffic.lane().name(), notification.notificationId().toString());
        UUID taskId = tasks.create(notification);
        String eventId = UUID.randomUUID().toString();
        jdbc.update("""
                INSERT INTO outbox_event (event_id, notification_id, event_type, payload, traffic_class, created_at, available_at)
                VALUES (?, ?, 'NOTIFICATION_REQUESTED', JSON_OBJECT('schemaVersion', 2,
                    'eventId', ?, 'notificationId', ?, 'taskId', ?), ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """, eventId, notification.notificationId().toString(), eventId,
                notification.notificationId().toString(), taskId.toString(), traffic.lane().name());
        return notification;
    }
}
