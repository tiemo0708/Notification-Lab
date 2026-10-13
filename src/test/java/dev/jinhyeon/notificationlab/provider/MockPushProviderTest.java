package dev.jinhyeon.notificationlab.provider;

import dev.jinhyeon.notificationlab.config.MockProperties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MockPushProviderTest {
    private final PushRequest request = new PushRequest(UUID.randomUUID(), "user-1", "title", "body");

    @Test
    void successMeansProviderAcceptedNotDeviceDelivered() {
        var result = provider(MockProperties.Outcome.SUCCESS).send(request);
        assertThat(result.outcome()).isEqualTo(PushResult.Outcome.ACCEPTED);
        assertThat(result.providerMessageId()).isEqualTo("mock-" + request.notificationId());
        assertThat(result.errorCode()).isNull();
    }

    @Test
    void transientFailureIsClassifiedWithoutAutomaticRetry() {
        var result = provider(MockProperties.Outcome.TRANSIENT_FAILURE).send(request);
        assertThat(result.outcome()).isEqualTo(PushResult.Outcome.RETRYABLE_FAILURE);
        assertThat(result.errorCode()).isEqualTo("MOCK_UNAVAILABLE");
        assertThat(result.providerMessageId()).isNull();
    }

    @Test
    void permanentFailureIsClassified() {
        var result = provider(MockProperties.Outcome.PERMANENT_FAILURE).send(request);
        assertThat(result.outcome()).isEqualTo(PushResult.Outcome.PERMANENT_FAILURE);
        assertThat(result.errorCode()).isEqualTo("MOCK_INVALID_RECIPIENT");
    }

    @Test
    void interruptionIsPreservedAndNeverReportedAsSuccess() {
        Thread.currentThread().interrupt();
        try {
            var result = provider(MockProperties.Outcome.SUCCESS).send(request);
            assertThat(result.outcome()).isEqualTo(PushResult.Outcome.RETRYABLE_FAILURE);
            assertThat(result.errorCode()).isEqualTo("MOCK_INTERRUPTED");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private MockPushProvider provider(MockProperties.Outcome outcome) {
        return new MockPushProvider(new MockProperties(0, outcome));
    }
}
