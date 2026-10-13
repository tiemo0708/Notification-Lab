package dev.jinhyeon.notificationlab.notification;

import java.time.Instant;
import java.util.UUID;

public record Notification(
        UUID notificationId,
        String recipientId,
        String title,
        String body,
        NotificationStatus status,
        String providerMessageId,
        String errorCode,
        Instant createdAt,
        Instant completedAt) {

    public Notification completed(NotificationStatus nextStatus, String messageId,
                                  String nextErrorCode, Instant now) {
        if (status != NotificationStatus.PROCESSING || nextStatus == NotificationStatus.PROCESSING) {
            throw new IllegalStateException("Invalid notification transition");
        }
        return new Notification(notificationId, recipientId, title, body, nextStatus,
                messageId, nextErrorCode, createdAt, now);
    }
}
