package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.campaign.Campaign;
import dev.jinhyeon.notificationlab.campaign.CampaignAudiencePage.Target;
import dev.jinhyeon.notificationlab.campaign.CampaignNotFoundException;
import dev.jinhyeon.notificationlab.campaign.CampaignStore;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import javax.sql.DataSource;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

public class CampaignBatchService {
    public static final String JOB_NAME = "campaign-expand-v1";
    private final CampaignStore campaigns;
    private final DataSourceProperties database;
    private final DataSource sequences;
    private final int maxAttempts;
    private final int chunkSize;
    private final Semaphore slot = new Semaphore(1);

    public CampaignBatchService(CampaignStore campaigns, DataSourceProperties database, DataSource sequences,
                                int maxAttempts, int chunkSize) {
        if (chunkSize < 1 || chunkSize > 1000) throw new IllegalArgumentException("Invalid campaign chunk size");
        this.campaigns = campaigns;
        this.database = database;
        this.sequences = sequences;
        this.maxAttempts = maxAttempts;
        this.chunkSize = chunkSize;
    }

    public JobExecution execute(UUID id) throws Exception {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Launch campaign jobs outside a caller transaction");
        }
        if (!slot.tryAcquire()) throw new IllegalStateException("Campaign executor is busy");
        try {
            Campaign campaign = campaigns.find(id).orElseThrow(CampaignNotFoundException::new);
            if (campaign.status() != Campaign.Status.START_REQUESTED) {
                throw new IllegalStateException("Campaign start must be requested before expansion");
            }
            try (var session = CampaignBatchSession.open(database, sequences, id)) {
                var parameters = new JobParametersBuilder().addString("campaignId", id.toString())
                        .addString("ownershipProtocol", CampaignBatchSession.PROTOCOL, false).toJobParameters();
                recoverOrphan(session, parameters);
                var step = new StepBuilder("expand-targets-v1", session.repository)
                        .<Target, Target>chunk(chunkSize, session.transactions)
                        .reader(new CampaignTargetReader(campaign, chunkSize))
                        .writer(new CampaignTargetWriter(campaign, session.jdbc, maxAttempts))
                        .build();
                var job = new JobBuilder(JOB_NAME, session.repository).start(step).build();
                try {
                    return session.launcher.run(job, parameters);
                } catch (JobInstanceAlreadyCompleteException complete) {
                    return session.repository.getLastJobExecution(JOB_NAME, parameters);
                }
            }
        } finally {
            slot.release();
        }
    }

    private void recoverOrphan(CampaignBatchSession session, JobParameters parameters) {
        var last = session.repository.getLastJobExecution(JOB_NAME, parameters);
        if (last == null) return;
        var execution = session.explorer.getJobExecution(last.getId());
        boolean uncertainStep = execution.getStepExecutions().stream().anyMatch(step -> uncertain(step.getStatus()));
        if (!uncertain(execution.getStatus()) && !uncertainStep) return;
        // An older binary never held this session lock; absence of the lock proves nothing about its liveness.
        if (!CampaignBatchSession.PROTOCOL.equals(execution.getJobParameters().getString("ownershipProtocol"))) {
            throw new IllegalStateException("Legacy execution ownership cannot be verified; recovery refused");
        }
        // Exclusive ownership is already held. Old writes cannot reconnect, including JobRepository updates.
        new TransactionTemplate(session.transactions).executeWithoutResult(transaction -> {
            var now = LocalDateTime.now(ZoneOffset.UTC);
            var recovered = ExitStatus.FAILED.addExitDescription("Recovered after exclusive MySQL session ownership");
            for (var step : execution.getStepExecutions()) {
                if (uncertain(step.getStatus())) {
                    step.setStatus(BatchStatus.FAILED);
                    step.setExitStatus(recovered);
                    step.setEndTime(now);
                    session.repository.update(step);
                }
            }
            if (uncertain(execution.getStatus())) {
                execution.setStatus(BatchStatus.FAILED);
                execution.setExitStatus(recovered);
                execution.setEndTime(now);
                session.repository.update(execution);
            }
        });
    }

    private static boolean uncertain(BatchStatus status) {
        return status.isRunning() || status == BatchStatus.UNKNOWN;
    }
}
