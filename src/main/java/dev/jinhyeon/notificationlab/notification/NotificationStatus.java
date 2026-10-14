package dev.jinhyeon.notificationlab.notification;

public enum NotificationStatus {
    QUEUED,
    PROCESSING,
    WAITING_RETRY,
    PROVIDER_ACCEPTED,
    RETRYABLE_FAILURE,
    PERMANENT_FAILURE,
    INTERNAL_FAILURE
}
