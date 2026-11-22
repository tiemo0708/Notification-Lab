package dev.jinhyeon.notificationlab.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.campaign.CampaignChunk;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.provider.ProviderCircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

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
    private final long chunkBudgetNanos;

    public NotificationWorker(WorkerStore store, NotificationService service,
                              ObjectMapper mapper, MeterRegistry registry, WorkerAdmission admission,
                              WorkerQuota quota, ProviderCircuitBreaker circuit,
                              @Value("${spring.kafka.consumer.properties.max.poll.interval.ms:60000}") long pollIntervalMs) {
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
        this.chunkBudgetNanos = TimeUnit.MILLISECONDS.toNanos(pollIntervalMs / 2);
    }

    @KafkaListener(id = "notification-worker", topics = {"#{@trafficRouting.topic()}", "#{@trafficRouting.retryTopic()}"},
            groupId = "#{@trafficRouting.group()}", concurrency = "${app.worker.consumers:1}",
            autoStartup = "${app.async.worker-enabled:true}")
    public void receive(ConsumerRecord<String, String> record) {
        var handler = Timer.start(registry);
        try {
            admission.execute(() -> process(record));
        } catch (WorkerQuota.QuotaDeferred | WorkerQuota.QuotaUnavailableDeferred | ProviderCircuitBreaker.CircuitDeferred
                 | WorkerDatabaseUnavailable | ChunkDeferred deferred) {
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
        JsonNode event;
        try {
            event = mapper.readTree(record.value());
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Invalid notification event");
        }
        int version = event.path("schemaVersion").asInt();
        UUID eventId = UUID.fromString(event.path("eventId").asText());
        if (CampaignChunk.EVENT_TYPE.equals(event.path("eventType").asText())) {
            if (version != 1 || !eventId.toString().equals(record.key())) throw new IllegalArgumentException();
            processChunk(eventId, new CampaignChunk(UUID.fromString(event.path("campaignId").asText()),
                    event.path("firstOrdinal").asLong(), event.path("targetCount").asInt()));
            return;
        }
        if (version != 1 && version != 2) throw new IllegalArgumentException();
        UUID taskId = version == 2 ? UUID.fromString(event.path("taskId").asText()) : null;
        UUID id = UUID.fromString(event.path("notificationId").asText());
        if (!id.toString().equals(record.key())) throw new IllegalArgumentException();
        processTarget(id, eventId, taskId, false);
    }

    private void processChunk(UUID eventId, CampaignChunk chunk) {
        if (!eventId.equals(chunk.eventId())) throw new IllegalArgumentException("Invalid campaign chunk identity");
        long start = System.nanoTime();
        List<WorkerStore.ChunkTarget> targets;
        try {
            targets = store.campaignTargets(chunk);
        } catch (DataAccessException | CannotCreateTransactionException failure) {
            if (WorkerDatabaseUnavailable.isConnectionFailure(failure)) throw new WorkerDatabaseUnavailable();
            throw failure;
        }
        boolean processed = false;
        for (var target : targets) {
            if (!target.pending()) continue;
            // Allow a poll between portions of a slow chunk. Results already committed per target are skipped on replay.
            if (admission.isStopping() || (processed && System.nanoTime() - start >= chunkBudgetNanos)) {
                throw new ChunkDeferred();
            }
            processTarget(target.notificationId(), eventId, target.taskId(), true);
            processed = true;
        }
    }

    static final class ChunkDeferred extends RuntimeException { }

    private void processTarget(UUID id, UUID eventId, UUID taskId, boolean grouped) {
        String token = UUID.randomUUID().toString();
        var claimed = initialClaimTime.record(() -> claim(id, eventId, token, taskId, !quota.enabled() && !circuit.enabled(), grouped));
        try (var permit = claimed.isPresent() && claimed.get().needsPermit() ? circuit.acquire() : null) {
            if (claimed.isPresent() && claimed.get().needsPermit()) {
                quota.awaitPermit();
                claimed = admittedClaimTime.record(() -> claim(id, eventId, token, taskId, true, grouped));
            }
            if (claimed.isEmpty()) {
                registry.counter("notification.worker.replays").increment();
                return;
            }
            Timer.Sample sample = Timer.start(registry);
            var claim = claimed.get();
            if (!claim.send()) {
                registry.counter("notification.worker.exhausted").increment();
                service.recordCompletion(claim.notification(), sample);
                return;
            }
            var completed = providerTime.record(() -> service.attempt(claim.notification())); // Outside all DB transactions.
            if (permit != null) permit.complete(completed);
            if (completionTime.record(() -> store.complete(completed, token))) {
                registry.counter("notification.retry.scheduled").increment();
                sample.stop(Timer.builder("notification.processing").tag("status", "WAITING_RETRY")
                        .publishPercentileHistogram().register(registry));
            } else {
                service.recordCompletion(completed, sample);
            }
        }
    }

    private Optional<WorkerStore.Claim> claim(UUID id, UUID eventId, String token, UUID taskId, boolean sendPermitted,
                                              boolean grouped) {
        try {
            return grouped ? store.claim(id, eventId, token, taskId, sendPermitted, true)
                    : store.claim(id, eventId, token, taskId, sendPermitted);
        } catch (CannotCreateTransactionException failure) {
            if (WorkerDatabaseUnavailable.isConnectionFailure(failure)) throw new WorkerDatabaseUnavailable();
            throw failure;
        }
    }
}
