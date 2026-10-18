package dev.jinhyeon.notificationlab.config;

import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

@Configuration
@EnableConfigurationProperties({MockProperties.class, StoreProperties.class, TrafficProperties.class})
public class LabConfiguration {
    @Bean
    InitializingBean validateTrafficProfile(TrafficProperties traffic, Environment environment) {
        return () -> {
            if (traffic.splitEnabled() && environment.acceptsProfiles(Profiles.of("memory", "sync"))) {
                throw new IllegalArgumentException("Traffic split requires the asynchronous profile");
            }
        };
    }

    @Bean
    Clock clock() {
        return Clock.tick(Clock.systemUTC(), Duration.ofNanos(1000));
    }
}
