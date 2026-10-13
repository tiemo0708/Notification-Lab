package dev.jinhyeon.notificationlab.provider;

import java.util.UUID;

public record PushRequest(UUID notificationId, String recipientId, String title, String body) {
}
