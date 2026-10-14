package dev.jinhyeon.notificationlab.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.async")
public record AsyncProperties(@NotBlank String topic, @NotBlank String retryTopic,
                              @NotBlank String deadLetterTopic, @NotBlank String group,
                              @Min(1) @Max(100) int maxPublishAttempts) {
    @AssertTrue(message = "request, retry and dead-letter topics must be distinct")
    public boolean isTopicSeparationValid() {
        return topic != null && retryTopic != null && deadLetterTopic != null
                && !topic.equals(retryTopic) && !topic.equals(deadLetterTopic)
                && !retryTopic.equals(deadLetterTopic);
    }

    public String destination(String eventType) {
        return switch (eventType) {
            case "NOTIFICATION_REQUESTED" -> topic;
            case "NOTIFICATION_RETRY" -> retryTopic;
            case "NOTIFICATION_DEAD_LETTER" -> deadLetterTopic;
            default -> throw new IllegalArgumentException("Unsupported outbox event type");
        };
    }
}
