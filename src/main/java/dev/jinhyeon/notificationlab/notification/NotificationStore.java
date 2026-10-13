package dev.jinhyeon.notificationlab.notification;

import java.util.Optional;
import java.util.UUID;

public interface NotificationStore {
    void create(Notification notification);
    void update(Notification notification);
    Optional<Notification> find(UUID notificationId);
}
