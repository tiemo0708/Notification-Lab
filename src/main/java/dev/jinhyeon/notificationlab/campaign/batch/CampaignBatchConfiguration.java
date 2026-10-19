package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.campaign.CampaignStore;
import dev.jinhyeon.notificationlab.config.DeliveryProperties;
import java.time.ZoneId;
import java.time.ZoneOffset;
import javax.sql.DataSource;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
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
    CampaignBatchService campaignBatchService(CampaignStore campaigns, DataSourceProperties database,
                                             DeliveryProperties delivery, Environment environment) {
        int chunkSize = environment.getProperty("app.campaign.batch.chunk-size", Integer.class, 100);
        return new CampaignBatchService(campaigns, database, dataSource,
                delivery.maxAttempts(), chunkSize);
    }
}
