package dev.jinhyeon.notificationlab.notification;

public class StoreCapacityExceededException extends RuntimeException {
    public StoreCapacityExceededException() {
        super("Local demo storage is full; this version has no durable storage or retention worker");
    }
}
