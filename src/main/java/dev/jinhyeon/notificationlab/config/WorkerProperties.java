package dev.jinhyeon.notificationlab.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.worker")
public record WorkerProperties(int consumers, int maxActive, int queueCapacity, int queueWaitMs) {
    public WorkerProperties {
        // Request and retry topics each have three partitions with the default RangeAssignor.
        if (consumers < 1 || consumers > 3 || maxActive < 1 || maxActive > consumers
                || queueCapacity < 0 || queueCapacity > 3 || queueWaitMs < 1 || queueWaitMs > 5000) {
            throw new IllegalArgumentException("Invalid bounded Worker settings");
        }
    }
}
