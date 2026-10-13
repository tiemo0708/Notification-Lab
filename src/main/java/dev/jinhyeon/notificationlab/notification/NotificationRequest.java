package dev.jinhyeon.notificationlab.notification;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record NotificationRequest(
        @NotBlank @Size(max = 100) String recipientId,
        @NotBlank @Size(max = 100) String title,
        @NotBlank @Size(max = 500) String body) {
}
