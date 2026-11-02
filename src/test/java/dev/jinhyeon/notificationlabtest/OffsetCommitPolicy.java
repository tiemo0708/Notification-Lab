package dev.jinhyeon.notificationlabtest;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;

/** Explicit test sources only; no application/JAR configuration or extra commit thread. */
public final class OffsetCommitPolicy {
    public static final int ACK_COUNT = 10;
    public static final long ACK_TIME_MS = 100;
    public enum Mode { RECORD, COUNT_TIME }

    private OffsetCommitPolicy() { }

    public static Mode mode(String value) {
        if ("RECORD".equals(value)) return Mode.RECORD;
        if ("COUNT_TIME".equals(value)) return Mode.COUNT_TIME;
        throw new IllegalArgumentException("Offset mode must be RECORD or COUNT_TIME");
    }

    public static void apply(ContainerProperties properties, Mode mode) {
        if (!properties.isSyncCommits() || properties.isAsyncAcks()) {
            throw new IllegalArgumentException("This pilot requires synchronous commits and ordered acknowledgments");
        }
        properties.setAckMode(mode == Mode.RECORD ? ContainerProperties.AckMode.RECORD : ContainerProperties.AckMode.COUNT_TIME);
        properties.setAckCount(ACK_COUNT);
        properties.setAckTime(ACK_TIME_MS);
    }

    public static void requireApplied(ConfigurableApplicationContext app) {
        var expected = mode(app.getEnvironment().getProperty("notification.test.offset.mode", "RECORD"));
        var properties = app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker").getContainerProperties();
        var configuration = app.getBean(ConsumerFactory.class).getConfigurationProperties();
        if (!properties.getAckMode().name().equals(expected.name()) || properties.getAckCount() != ACK_COUNT
                || properties.getAckTime() != ACK_TIME_MS || !properties.isSyncCommits() || properties.isAsyncAcks()
                || properties.getPollTimeout() != 500 || !"1".equals(String.valueOf(configuration.get("max.poll.records")))
                || !"false".equals(String.valueOf(configuration.get("enable.auto.commit")))) {
            throw new IllegalStateException("Offset test policy was not applied with the expected bounded settings");
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class Wiring {
        @Bean
        static BeanPostProcessor offsetCommitPostProcessor(Environment environment) {
            var selected = mode(environment.getProperty("notification.test.offset.mode", "RECORD"));
            return new BeanPostProcessor() {
                @Override public Object postProcessAfterInitialization(Object bean, String name) {
                    if (bean instanceof ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
                        // Modify the template before containers exist; preserve the production shutdown customizer.
                        apply(factory.getContainerProperties(), selected);
                    }
                    return bean;
                }
            };
        }
    }
}
