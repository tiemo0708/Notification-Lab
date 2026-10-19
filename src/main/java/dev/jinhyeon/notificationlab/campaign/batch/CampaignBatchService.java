package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.campaign.Campaign;
import dev.jinhyeon.notificationlab.campaign.CampaignAudiencePage.Target;
import dev.jinhyeon.notificationlab.campaign.CampaignNotFoundException;
import dev.jinhyeon.notificationlab.campaign.CampaignStore;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class CampaignBatchService {
    public static final String JOB_NAME = "campaign-expand-v1";
    private final CampaignStore campaigns;
    private final JobRepository repository;
    private final JobLauncher launcher;
    private final PlatformTransactionManager transactionManager;
    private final JdbcTemplate jdbc;
    private final int maxAttempts;
    private final int chunkSize;
    private final Semaphore slot = new Semaphore(1);

    public CampaignBatchService(CampaignStore campaigns, JobRepository repository, JobLauncher launcher,
                                PlatformTransactionManager transactionManager, JdbcTemplate jdbc,
                                int maxAttempts, int chunkSize) {
        if (chunkSize < 1 || chunkSize > 1000) throw new IllegalArgumentException("Invalid campaign chunk size");
        this.campaigns = campaigns;
        this.repository = repository;
        this.launcher = launcher;
        this.transactionManager = transactionManager;
        this.jdbc = jdbc;
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
            var parameters = new JobParametersBuilder().addString("campaignId", id.toString()).toJobParameters();
            var step = new StepBuilder("expand-targets-v1", repository)
                    .<Target, Target>chunk(chunkSize, transactionManager)
                    .reader(new CampaignTargetReader(campaign, chunkSize))
                    .writer(new CampaignTargetWriter(campaign, jdbc, maxAttempts))
                    .build();
            var job = new JobBuilder(JOB_NAME, repository).start(step).build();
            try {
                return launcher.run(job, parameters);
            } catch (JobInstanceAlreadyCompleteException complete) {
                // Same identifying parameters: a completed job is a replay, never a new timestamped instance.
                return repository.getLastJobExecution(JOB_NAME, parameters);
            }
        } finally {
            slot.release();
        }
    }
}
