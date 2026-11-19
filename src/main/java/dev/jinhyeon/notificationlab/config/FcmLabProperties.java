package dev.jinhyeon.notificationlab.config;

import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.provider.fcm")
public record FcmLabProperties(String projectId, Path deviceTokenFile, Path accessTokenFile, Path sendMarker) {
    public FcmLabProperties {
        if (projectId == null || !projectId.matches("[a-z][a-z0-9-]{4,28}[a-z0-9]")
                || deviceTokenFile == null || accessTokenFile == null || sendMarker == null) {
            throw new IllegalArgumentException("FCM lab requires a project, two private token files, and a send marker");
        }
    }
}
