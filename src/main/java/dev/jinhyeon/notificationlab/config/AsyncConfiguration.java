package dev.jinhyeon.notificationlab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.context.WebServerGracefulShutdownLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@Profile("!memory & !sync")
@EnableScheduling
@EnableConfigurationProperties({AsyncProperties.class, DeliveryProperties.class, WorkerProperties.class})
public class AsyncConfiguration {
    @Bean
    NewTopic notificationTopic(TrafficRouting properties) {
        return TopicBuilder.name(properties.topic()).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic notificationRetryTopic(TrafficRouting properties) {
        return TopicBuilder.name(properties.retryTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic notificationDeadLetterTopic(TrafficRouting properties) {
        return TopicBuilder.name(properties.deadLetterTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> shutdownOrder() {
        return container -> {
            // Stop HTTP acceptance before waiting for the current Kafka record, but keep DB beans alive.
            container.setPhase(WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE - 1);
            container.getContainerProperties().setStopImmediate(true);
            container.getContainerProperties().setPollTimeout(500);
            container.getContainerProperties().setPollTimeoutWhilePaused(java.time.Duration.ofMillis(100));
        };
    }

}
