package dev.jinhyeon.notificationlab.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Deployment role, never selected by an untrusted message payload. */
@ConfigurationProperties("app.traffic")
public record TrafficProperties(@DefaultValue("REALTIME") Lane lane,
                                @DefaultValue("false") boolean splitEnabled,
                                @DefaultValue("2") int campaignRate,
                                @DefaultValue("2") int campaignBurst) {
    public enum Lane { REALTIME, CAMPAIGN }

    public TrafficProperties {
        if (lane == null || campaignRate < 1 || campaignRate > 9999
                || campaignBurst < 1 || campaignBurst > 9999 || (lane == Lane.CAMPAIGN && !splitEnabled)) {
            throw new IllegalArgumentException("Invalid traffic allocation settings");
        }
    }

    public String topic(String base) { return lane == Lane.REALTIME ? base : base + ".campaign"; }
}
