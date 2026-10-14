package dev.jinhyeon.notificationlab.notification;

public class InvalidIdempotencyKeyException extends RuntimeException {
    public InvalidIdempotencyKeyException() {
        super("Invalid or unsupported Idempotency-Key");
    }
}
