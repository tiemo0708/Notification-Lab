package dev.jinhyeon.notificationlab.provider;

public record PushResult(Outcome outcome, String providerMessageId, String errorCode) {
    public enum Outcome {
        ACCEPTED, RETRYABLE_FAILURE, PERMANENT_FAILURE
    }
}
