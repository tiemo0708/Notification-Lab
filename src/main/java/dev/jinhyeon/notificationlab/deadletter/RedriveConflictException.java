package dev.jinhyeon.notificationlab.deadletter;

public class RedriveConflictException extends RuntimeException {
    public RedriveConflictException(String message) {
        super(message);
    }
}
