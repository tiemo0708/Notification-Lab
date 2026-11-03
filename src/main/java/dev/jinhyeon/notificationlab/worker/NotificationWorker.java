package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.provider.ProviderCircuitBreaker;
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
    private final WorkerAdmission admission;
    private final WorkerQuota quota;
    private final ProviderCircuitBreaker circuit;
    private final WorkerStore store;
    private final NotificationService service;
    private final ObjectMapper mapper;
    private final MeterRegistry registry;
    private final Timer handlerTime;
    private final Timer initialClaimTime;
    private final Timer admittedClaimTime;
    private final Timer providerTime;
    private final Timer completionTime;

    public NotificationWorker(WorkerStore store, NotificationService service,
                              ObjectMapper mapper, MeterRegistry registry, WorkerAdmission admission,
                              WorkerQuota quota, ProviderCircuitBreaker circuit) {
        this.circuit = circuit;
        this.admission = admission;
        this.quota = quota;
        this.store = store;
        this.service = service;
        this.mapper = mapper;
        this.registry = registry;
        this.handlerTime = registry.timer("notification.pipeline.stage", "stage", "worker.handler");
        this.initialClaimTime = registry.timer("notification.pipeline.stage", "stage", "worker.claim.initial");
        this.admittedClaimTime = registry.timer("notification.pipeline.stage", "stage", "worker.claim.admitted");
        this.providerTime = registry.timer("notification.pipeline.stage", "stage", "worker.provider");
        this.completionTime = registry.timer("notification.pipeline.stage", "stage", "worker.complete");
    }

    @KafkaListener(id = "notification-worker", topics = {"#{@trafficRouting.topic()}", "#{@trafficRouting.retryTopic()}"},
            groupId = "#{@trafficRouting.group()}", concurrency = "${app.worker.consumers:1}",
            autoStartup = "${app.async.worker-enabled:true}")
    public void receive(ConsumerRecord<String, String> record) {
        var handler = Timer.start(registry);
        try {
            admission.execute(() -> process(record));
        } catch (WorkerQuota.QuotaDeferred | WorkerQuota.QuotaUnavailableDeferred | ProviderCircuitBreaker.CircuitDeferred deferred) {
            throw deferred;
        } catch (RuntimeException failure) {
            registry.counter("notification.worker.failures").increment();
            // Includes admission failure: never return successfully for an unprocessed record.
            throw new IllegalStateException("Notification processing failed; offset must remain uncommitted");
        } finally {
            handler.stop(handlerTime);
        }
    }

    private void process(ConsumerRecord<String, String> record) {
        try {
            var event = mapper.readTree(record.value());
            int version = event.path("schemaVersion").asInt();
            if (version != 1 && version != 2) throw new IllegalArgumentException();
            UUID taskId = version == 2 ? UUID.fromString(event.path("taskId").asText()) : null;
            UUID id = UUID.fromString(event.path("notificationId").asText());
            UUID eventId = UUID.fromString(event.path("eventId").asText());
            if (!id.toString().equals(record.key())) throw new IllegalArgumentException();
            String token = UUID.randomUUID().toString();
            var claimed = initialClaimTime.record(() -> store.claim(id, eventId, token, taskId, !quota.enabled() && !circuit.enabled()));
            try (var permit = claimed.isPresent() && claimed.get().needsPermit() ? circuit.acquire() : null) {
                if (claimed.isPresent() && claimed.get().needsPermit()) {
                    quota.awaitPermit();
                    claimed = admittedClaimTime.record(() -> store.claim(id, eventId, token, taskId)); // Revalidate after admission, then start the attempt.
                }
                if (claimed.isPresent()) {
                    Timer.Sample sample = Timer.start(registry);
                    var claim = claimed.get();
                    if (claim.send()) {
                        var completed = providerTime.record(() -> service.attempt(claim.notification())); // Outside all DB transactions.
                        if (permit != null) permit.complete(completed); // Observe HTTP outcome, independently of DB completion.
                        if (completionTime.record(() -> store.complete(completed, token))) { // History and retry outbox commit before the original offset.
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
            }
        } catch (WorkerQuota.QuotaDeferred | WorkerQuota.QuotaUnavailableDeferred | ProviderCircuitBreaker.CircuitDeferred deferred) {
            throw deferred;
        } catch (Exception failure) {
            // Do not attach SQL/JSON exceptions: they can contain recipient data or payloads.
            throw new IllegalStateException("Notification processing failed; offset must remain uncommitted");
        }
    }
}
