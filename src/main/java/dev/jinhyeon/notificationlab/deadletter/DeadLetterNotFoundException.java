package dev.jinhyeon.notificationlab.deadletter;

public class DeadLetterNotFoundException extends RuntimeException {
    public DeadLetterNotFoundException() {
        super("Dead-letter event not found");
    }
}
