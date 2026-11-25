package dev.jinhyeon.notificationlab.config;

import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.worker.CampaignCompletionBatches;
import dev.jinhyeon.notificationlab.worker.WorkerStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration(proxyBeanMethods = false)
@Profile("campaign-flexible & !memory & !sync")
@ConditionalOnProperty(name = "app.traffic.lane", havingValue = "CAMPAIGN")
@ConditionalOnProperty(name = "app.provider.mode", havingValue = "HTTP")
@ConditionalOnProperty(name = "app.worker.campaign-completion-batch-enabled", havingValue = "true")
public class CampaignCompletionConfiguration {
    @Bean
    CampaignCompletionBatches campaignCompletionBatches(WorkerProperties workers, WorkerStore store,
            NotificationService service, PlatformTransactionManager manager, MeterRegistry registry) {
        return new CampaignCompletionBatches(workers.maxActive(), CampaignCompletionBatches.MAX_WAIT_MS,
                store, service, manager, registry);
    }
}
