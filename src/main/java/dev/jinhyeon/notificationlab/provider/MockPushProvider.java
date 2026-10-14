package dev.jinhyeon.notificationlab.provider;

import dev.jinhyeon.notificationlab.config.MockProperties;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** Deterministic in-process fake. It does not contact FCM, APNs, or an HTTP server. */
@Component
@ConditionalOnProperty(name = "app.provider.mode", havingValue = "MOCK", matchIfMissing = true)
public class MockPushProvider implements PushProvider {
    private final MockProperties properties;

    public MockPushProvider(MockProperties properties) {
        this.properties = properties;
    }

    @Override
    public PushResult send(PushRequest request) {
        try {
            Thread.sleep(properties.delayMs());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return new PushResult(PushResult.Outcome.RETRYABLE_FAILURE, null, "MOCK_INTERRUPTED");
        }
        return switch (properties.outcome()) {
            case SUCCESS -> new PushResult(PushResult.Outcome.ACCEPTED,
                    "mock-" + request.notificationId(), null);
            case TRANSIENT_FAILURE -> new PushResult(PushResult.Outcome.RETRYABLE_FAILURE,
                    null, "MOCK_UNAVAILABLE");
            case PERMANENT_FAILURE -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE,
                    null, "MOCK_INVALID_RECIPIENT");
        };
    }
}
