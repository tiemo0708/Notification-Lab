package dev.jinhyeon.notificationlab.notification;

import dev.jinhyeon.notificationlab.config.StoreProperties;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

/** Bounded, volatile storage for the synchronous baseline, not the final outbox store. */
@Repository
@Profile("memory")
public class InMemoryNotificationStore implements NotificationStore {
    private final ConcurrentHashMap<UUID, Notification> notifications = new ConcurrentHashMap<>();
    private final int maxRecords;

    public InMemoryNotificationStore(StoreProperties properties) {
        maxRecords = properties.maxRecords();
    }

    @Override
    public synchronized void create(Notification notification) {
        if (notifications.size() >= maxRecords) {
            throw new StoreCapacityExceededException();
        }
        if (notifications.putIfAbsent(notification.notificationId(), notification) != null) {
            throw new IllegalStateException("Duplicate notification ID");
        }
    }

    @Override
    public void update(Notification notification) {
        if (notifications.replace(notification.notificationId(), notification) == null) {
            throw new IllegalStateException("Unknown notification ID");
        }
    }

    @Override
    public Optional<Notification> find(UUID notificationId) {
        return Optional.ofNullable(notifications.get(notificationId));
    }
}
