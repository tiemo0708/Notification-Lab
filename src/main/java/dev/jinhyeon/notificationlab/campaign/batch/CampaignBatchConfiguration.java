package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.campaign.CampaignStore;
import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import javax.sql.DataSource;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Profile("!memory & !sync")
@ConditionalOnProperty(name = "app.campaign.batch.enabled", havingValue = "true")
@DependsOnDatabaseInitialization
public class CampaignBatchConfiguration extends DefaultBatchConfiguration {
    private final DataSource dataSource;
    private final PlatformTransactionManager transactionManager;

    public CampaignBatchConfiguration(DataSource dataSource, PlatformTransactionManager transactionManager,
                                      Environment environment) {
        if (!"CAMPAIGN".equalsIgnoreCase(environment.getProperty("app.traffic.lane"))) {
            throw new IllegalArgumentException("Campaign Batch requires the CAMPAIGN lane");
        }
        if (!ZoneId.systemDefault().normalized().equals(ZoneOffset.UTC)) {
            throw new IllegalArgumentException("Campaign Batch metadata requires JVM timezone UTC");
        }
        this.dataSource = dataSource;
        this.transactionManager = transactionManager;
    }

    @Override
    protected DataSource getDataSource() { return dataSource; }

    @Override
    protected PlatformTransactionManager getTransactionManager() { return transactionManager; }

    @Bean
    CampaignBatchService campaignBatchService(CampaignStore campaigns, JobRepository repository,
                                             JobLauncher launcher, JdbcTemplate jdbc,
                                             DeliveryProperties delivery, Environment environment) {
        int chunkSize = environment.getProperty("app.campaign.batch.chunk-size", Integer.class, 100);
        return new CampaignBatchService(campaigns, repository, launcher, transactionManager, jdbc,
                delivery.maxAttempts(), chunkSize);
    }
}
