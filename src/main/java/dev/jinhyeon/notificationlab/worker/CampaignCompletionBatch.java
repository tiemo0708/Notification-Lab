package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.notification.Notification;
import io.micrometer.core.instrument.Timer;

/** Scoped to one grouped Kafka record. Closing must persist all successes or fail the listener. */
public interface CampaignCompletionBatch extends AutoCloseable {
    interface Factory {
        CampaignCompletionBatch open();
    }

    void check();
    void accepted(Notification result, String workerToken, Timer.Sample processing);

    @Override
    void close();
}
