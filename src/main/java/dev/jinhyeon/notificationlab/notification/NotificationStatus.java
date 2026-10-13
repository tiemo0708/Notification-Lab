package dev.jinhyeon.notificationlab.notification;

public enum NotificationStatus {
    PROCESSING,
    PROVIDER_ACCEPTED,
    RETRYABLE_FAILURE,
    PERMANENT_FAILURE,
    INTERNAL_FAILURE
}
