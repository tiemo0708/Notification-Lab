package dev.jinhyeon.notificationlab.notification;

import dev.jinhyeon.notificationlab.config.StoreProperties;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryNotificationStoreTest {
    @Test
    void concurrentAdmissionCannotExceedCapacity() throws Exception {
        var store = new InMemoryNotificationStore(new StoreProperties(10));
        var tasks = IntStream.range(0, 100).mapToObj(i -> (Callable<Boolean>) () -> {
            try {
                store.create(notification());
                return true;
            } catch (StoreCapacityExceededException exception) {
                return false;
            }
        }).toList();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = executor.invokeAll(tasks);
            int admitted = 0;
            for (var result : results) {
                if (result.get()) admitted++;
            }
            assertThat(admitted).isEqualTo(10);
        }
    }

    @Test
    void unknownIdIsAbsentAndCannotBeUpdated() {
        var store = new InMemoryNotificationStore(new StoreProperties(10));
        var record = notification();
        assertThat(store.find(record.notificationId())).isEmpty();
        assertThatThrownBy(() -> store.update(record)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aCompletedRecordCannotBeCompletedAgain() {
        var completed = notification().completed(NotificationStatus.PROVIDER_ACCEPTED,
                "mock-id", null, Instant.now());
        assertThatThrownBy(() -> completed.completed(NotificationStatus.PROVIDER_ACCEPTED,
                "other-id", null, Instant.now())).isInstanceOf(IllegalStateException.class);
    }

    private Notification notification() {
        return new Notification(UUID.randomUUID(), "user-1", "title", "body",
                NotificationStatus.PROCESSING, null, null, Instant.now(), null);
    }
}
