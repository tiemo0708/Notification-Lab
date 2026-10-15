package dev.jinhyeon.notificationlab.config;

import dev.jinhyeon.notificationlab.worker.WorkerAdmission;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.context.WebServerGracefulShutdownLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@Profile("!memory & !sync")
@EnableScheduling
@EnableConfigurationProperties({AsyncProperties.class, DeliveryProperties.class, WorkerProperties.class})
public class AsyncConfiguration {
    @Bean
    NewTopic notificationTopic(AsyncProperties properties) {
        return TopicBuilder.name(properties.topic()).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic notificationRetryTopic(AsyncProperties properties) {
        return TopicBuilder.name(properties.retryTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic notificationDeadLetterTopic(AsyncProperties properties) {
        return TopicBuilder.name(properties.deadLetterTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> shutdownOrder() {
        return container -> {
            // Stop HTTP acceptance before waiting for the current Kafka record, but keep DB beans alive.
            container.setPhase(WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE - 1);
            container.getContainerProperties().setStopImmediate(true);
        };
    }

    @Bean
    CommonContainerStoppingErrorHandler stoppingErrorHandler(WorkerAdmission admission) {
        // Never skip or commit a failed record. Fix the cause, then explicitly restart the worker.
        return new CommonContainerStoppingErrorHandler() {
            @Override
            public void handleRemaining(Exception failure, List<ConsumerRecord<?, ?>> records,
                                        Consumer<?, ?> consumer, MessageListenerContainer container) {
                if (admission.isStopping()) {
                    // Context shutdown must own stop(): a competing stop can mark the container stopped
                    // before lifecycle shutdown waits for active listeners, allowing DB destruction early.
                    throw new IllegalStateException("Worker is shutting down; record remains uncommitted");
                }
                super.handleRemaining(failure, records, consumer, container);
            }

            @Override
            public void handleOtherException(Exception failure, Consumer<?, ?> consumer,
                                             MessageListenerContainer container, boolean batchListener) {
                if (admission.isStopping()) {
                    throw new IllegalStateException("Worker is shutting down; record remains uncommitted");
                }
                super.handleOtherException(failure, consumer, container, batchListener);
            }
        };
    }
}
