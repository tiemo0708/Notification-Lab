package dev.jinhyeon.notificationlab.worker;

import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** One success buffer per active grouped Kafka record. All provider calls precede result persistence. */
public final class CampaignCompletionBatches implements CampaignCompletionBatch.Factory, AutoCloseable {
    public static final int MAX_SIZE = 5;
    public static final int MAX_WAIT_MS = 100;
    private final int waitMs;
    private final WorkerStore store;
    private final NotificationService service;
    private final TransactionTemplate transactions;
    private final Timer completionTime;
    private final Counter[] commits = new Counter[MAX_SIZE];
    private final Counter deadlineFlushes;
    private final ScheduledThreadPoolExecutor deadlines;

    public CampaignCompletionBatches(int workers, int waitMs, WorkerStore store, NotificationService service,
                                     PlatformTransactionManager manager, MeterRegistry registry) {
        this.waitMs = waitMs;
        this.store = store;
        this.service = service;
        transactions = new TransactionTemplate(manager);
        completionTime = registry.timer("notification.pipeline.stage", "stage", "worker.complete");
        for (int size = 1; size <= MAX_SIZE; size++) {
            commits[size - 1] = registry.counter("notification.worker.campaign.completion.commits", "size", String.valueOf(size));
        }
        deadlineFlushes = registry.counter("notification.worker.campaign.completion.deadlines");
        deadlines = new ScheduledThreadPoolExecutor(workers,
                Thread.ofPlatform().daemon().name("campaign-completion-timer-", 0).factory());
        deadlines.setRemoveOnCancelPolicy(true);
    }

    private record Accepted(Notification result, String token, Timer.Sample processing) { }

    @Override
    public CampaignCompletionBatch open() { return new Buffer(); }

    private final class Buffer implements CampaignCompletionBatch {
        private final List<Accepted> pending = new ArrayList<>(MAX_SIZE);
        private long generation;
        private ScheduledFuture<?> deadline;
        private Throwable failure;

        @Override
        public synchronized void check() {
            if (failure != null) throw new IllegalStateException("Campaign result commit failed", failure);
        }

        @Override
        public synchronized void accepted(Notification result, String token, Timer.Sample processing) {
            check();
            if (result.status() != NotificationStatus.PROVIDER_ACCEPTED) {
                throw new IllegalArgumentException("Campaign result batch requires a provider acceptance");
            }
            pending.add(new Accepted(result, token, processing));
            if (pending.size() == MAX_SIZE) {
                flush();
            } else if (pending.size() == 1) {
                long scheduledGeneration = ++generation;
                deadline = deadlines.schedule(() -> onDeadline(scheduledGeneration), waitMs, TimeUnit.MILLISECONDS);
            }
        }

        private synchronized void onDeadline(long scheduledGeneration) {
            // A cancelled timer may already be waiting for this buffer's monitor.
            if (generation == scheduledGeneration) {
                deadlineFlushes.increment();
                flush();
            }
        }

        private void flush() {
            if (pending.isEmpty()) return;
            generation++;
            if (deadline != null) {
                deadline.cancel(false);
                deadline = null;
            }
            try {
                completionTime.record(() -> transactions.executeWithoutResult(transaction -> {
                    if (!transaction.isNewTransaction()) throw new IllegalStateException("Expected an independent result commit");
                    for (var accepted : pending) store.complete(accepted.result(), accepted.token());
                }));
                commits[pending.size() - 1].increment();
                for (var accepted : pending) service.recordCompletion(accepted.result(), accepted.processing());
            } catch (RuntimeException | Error problem) {
                // The listener must observe a timer-thread failure before acknowledging the record.
                failure = problem;
                throw problem;
            } finally {
                pending.clear();
            }
        }

        @Override
        public synchronized void close() {
            check();
            flush();
        }
    }

    @Override
    public void close() { deadlines.shutdownNow(); }
}
