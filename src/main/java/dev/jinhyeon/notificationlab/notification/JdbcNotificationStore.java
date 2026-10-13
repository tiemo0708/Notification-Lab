package dev.jinhyeon.notificationlab.notification;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Profile("!memory")
public class JdbcNotificationStore implements NotificationStore {
    private static final RowMapper<Notification> ROW_MAPPER = (rs, rowNum) -> {
        LocalDateTime completedAt = rs.getObject("completed_at", LocalDateTime.class);
        return new Notification(UUID.fromString(rs.getString("notification_id")),
                rs.getString("recipient_id"), rs.getString("title"), rs.getString("body"),
                NotificationStatus.valueOf(rs.getString("status")), rs.getString("provider_message_id"),
                rs.getString("error_code"),
                rs.getObject("created_at", LocalDateTime.class).toInstant(ZoneOffset.UTC),
                completedAt == null ? null : completedAt.toInstant(ZoneOffset.UTC));
    };

    private final JdbcTemplate jdbc;

    public JdbcNotificationStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional
    public void create(Notification notification) {
        if (notification.status() != NotificationStatus.PROCESSING) {
            throw new IllegalArgumentException("New notification must be PROCESSING");
        }
        jdbc.update("""
                INSERT INTO notification
                    (notification_id, recipient_id, title, body, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """, notification.notificationId().toString(), notification.recipientId(),
                notification.title(), notification.body(), notification.status().name(),
                utc(notification.createdAt()));
    }

    @Override
    @Transactional
    public void update(Notification notification) {
        if (notification.status() == NotificationStatus.PROCESSING) {
            throw new IllegalArgumentException("Completion must have a terminal status");
        }
        int changed = jdbc.update("""
                UPDATE notification
                SET status = ?, provider_message_id = ?, error_code = ?, completed_at = ?
                WHERE notification_id = ? AND status = 'PROCESSING'
                """, notification.status().name(), notification.providerMessageId(), notification.errorCode(),
                utc(notification.completedAt()), notification.notificationId().toString());
        if (changed != 1) {
            throw new IllegalStateException("Notification is missing or already completed");
        }
    }

    @Override
    public Optional<Notification> find(UUID notificationId) {
        return jdbc.query("SELECT * FROM notification WHERE notification_id = ?",
                ROW_MAPPER, notificationId.toString()).stream().findFirst();
    }

    private static LocalDateTime utc(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
