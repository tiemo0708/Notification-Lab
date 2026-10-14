package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("!memory & !sync")
public class NotificationWorker {
    private final WorkerStore store;
    private final NotificationService service;
    private final ObjectMapper mapper;
    private final MeterRegistry registry;

    public NotificationWorker(WorkerStore store, NotificationService service,
                              ObjectMapper mapper, MeterRegistry registry) {
        this.store = store;
        this.service = service;
        this.mapper = mapper;
        this.registry = registry;
    }

    @KafkaListener(id = "notification-worker", topics = "${app.async.topic}",
            groupId = "${app.async.group}", concurrency = "1",
            autoStartup = "${app.async.worker-enabled:true}")
    public void receive(ConsumerRecord<String, String> record) {
        try {
            var event = mapper.readTree(record.value());
            int version = event.path("schemaVersion").asInt();
            if (version != 1 && version != 2) throw new IllegalArgumentException();
            UUID taskId = version == 2 ? UUID.fromString(event.path("taskId").asText()) : null;
            UUID id = UUID.fromString(event.path("notificationId").asText());
            UUID eventId = UUID.fromString(event.path("eventId").asText());
            if (!id.toString().equals(record.key())) throw new IllegalArgumentException();
            String token = UUID.randomUUID().toString();
            var claimed = store.claim(id, eventId, token, taskId);
            if (claimed.isPresent()) {
                Timer.Sample sample = Timer.start(registry);
                var claim = claimed.get();
                if (claim.send()) {
                    var completed = service.attempt(claim.notification()); // Outside all DB transactions.
                    if (store.complete(completed, token)) { // History and retry outbox commit before the original offset.
                        registry.counter("notification.retry.scheduled").increment();
                        sample.stop(Timer.builder("notification.processing").tag("status", "WAITING_RETRY")
                                .publishPercentileHistogram().register(registry));
                    } else {
                        service.recordCompletion(completed, sample);
                    }
                } else {
                    registry.counter("notification.worker.exhausted").increment();
                    service.recordCompletion(claim.notification(), sample);
                }
            } else {
                registry.counter("notification.worker.replays").increment();
            }
        } catch (Exception failure) {
            registry.counter("notification.worker.failures").increment();
            // Do not attach SQL/JSON exceptions: they can contain recipient data or payloads.
            throw new IllegalStateException("Notification processing failed; offset must remain uncommitted");
        }
    }
}
