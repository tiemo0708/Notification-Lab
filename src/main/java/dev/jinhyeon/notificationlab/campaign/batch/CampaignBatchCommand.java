package dev.jinhyeon.notificationlab.campaign.batch;

import dev.jinhyeon.notificationlab.NotificationLabApplication;
import java.util.UUID;
import java.util.TimeZone;
import org.springframework.batch.core.BatchStatus;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Explicit one-campaign command. Normal application startup never dispatches pending campaigns. */
public final class CampaignBatchCommand {
    private CampaignBatchCommand() { }

    public static void main(String[] args) {
        int code = 1;
        try {
            if (args.length != 1) throw new IllegalArgumentException("Exactly one campaign UUID is required");
            UUID id = UUID.fromString(args[0]);
            // Spring Batch metadata uses LocalDateTime.now(); normalize this dedicated JVM before boot.
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            try (var context = new SpringApplicationBuilder(NotificationLabApplication.class)
                    .web(WebApplicationType.NONE).run(
                            "--app.campaign.batch.enabled=true", "--app.traffic.lane=CAMPAIGN",
                            "--app.traffic.split-enabled=true", "--app.rate-limit.client-enabled=true",
                            "--app.async.worker-enabled=false", "--app.async.relay-enabled=false")) {
                var execution = context.getBean(CampaignBatchService.class).execute(id);
                System.out.println("CAMPAIGN_BATCH executionId=" + execution.getId() + " status=" + execution.getStatus());
                code = execution.getStatus() == BatchStatus.COMPLETED ? 0 : 1;
            }
        } catch (Exception failure) {
            System.err.println("CAMPAIGN_BATCH failed exceptionType=" + failure.getClass().getSimpleName());
        }
        System.exit(code);
    }
}
