package dev.jinhyeon.notificationlab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@Profile("!memory & !sync")
@EnableScheduling
@EnableConfigurationProperties(AsyncProperties.class)
public class AsyncConfiguration {
    @Bean
    NewTopic notificationTopic(AsyncProperties properties) {
        return TopicBuilder.name(properties.topic()).partitions(3).replicas(1).build();
    }

    @Bean
    CommonContainerStoppingErrorHandler stoppingErrorHandler() {
        // Never skip or commit a failed record. Fix the cause, then explicitly restart the worker.
        return new CommonContainerStoppingErrorHandler();
    }
}
