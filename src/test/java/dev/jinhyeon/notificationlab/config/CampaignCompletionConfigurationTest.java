package dev.jinhyeon.notificationlab.config;

import dev.jinhyeon.notificationlab.worker.CampaignCompletionBatch;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class CampaignCompletionConfigurationTest {
    @ParameterizedTest
    @CsvSource({
            "default, CAMPAIGN, HTTP, true",
            "campaign-flexible, REALTIME, HTTP, true",
            "campaign-flexible, CAMPAIGN, FCM, true",
            "campaign-flexible, CAMPAIGN, MOCK, true",
            "campaign-flexible, CAMPAIGN, HTTP, false"
    })
    void otherRoutesUseSingleCompletions(String profile, String lane, String provider, boolean enabled) {
        new ApplicationContextRunner().withUserConfiguration(CampaignCompletionConfiguration.class)
                .withPropertyValues("spring.profiles.active=" + profile, "app.traffic.lane=" + lane,
                        "app.provider.mode=" + provider, "app.worker.campaign-completion-batch-enabled=" + enabled)
                .run(app -> assertThat(app).hasNotFailed().doesNotHaveBean(CampaignCompletionBatch.Factory.class));
    }
}
