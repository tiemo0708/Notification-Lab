package dev.jinhyeon.notificationlab.provider;

public interface PushProvider {
    PushResult send(PushRequest request);
}
