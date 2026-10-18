package dev.jinhyeon.notificationlab.outbox;

import dev.jinhyeon.notificationlab.config.TrafficProperties;
import dev.jinhyeon.notificationlab.notification.IdempotencyConflictException;
import dev.jinhyeon.notificationlab.notification.InvalidIdempotencyKeyException;
import dev.jinhyeon.notificationlab.notification.JdbcNotificationStore;
import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@Profile("!memory & !sync")
public class IdempotentSubmissionService {
    private final SubmissionService submissions;
    private final JdbcNotificationStore notifications;
    private final JdbcTemplate jdbc;
    private final TrafficProperties traffic;

    public IdempotentSubmissionService(SubmissionService submissions, JdbcNotificationStore notifications,
                                       JdbcTemplate jdbc,
                                       TrafficProperties traffic) {
        this.submissions = submissions;
        this.notifications = notifications;
        this.jdbc = jdbc;
        this.traffic = traffic;
    }

    public record Result(Notification notification, boolean replayed) { }

    // The failed INSERT must finish rolling back before we read the winning transaction's row.
    // Suspending a caller transaction also avoids a stale repeatable-read snapshot on replay.
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public Result submit(NotificationRequest request, String key) {
        if (key != null && !key.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new InvalidIdempotencyKeyException();
        }
        try {
            return new Result(submissions.accept(request, key), false);
        } catch (DuplicateKeyException duplicate) {
            if (key == null) throw duplicate;
            Notification existing = notifications.findByIdempotencyKey(key).orElseThrow(() -> duplicate);
            // Compare decoded field values exactly; DB text collation must not hide case/space changes.
            if (!traffic.lane().name().equals(jdbc.queryForObject(
                    "SELECT traffic_class FROM notification WHERE notification_id = ?", String.class, existing.notificationId().toString()))
                    || !existing.recipientId().equals(request.recipientId())
                    || !existing.title().equals(request.title()) || !existing.body().equals(request.body())) {
                throw new IdempotencyConflictException();
            }
            return new Result(existing, true);
        }
    }
}
