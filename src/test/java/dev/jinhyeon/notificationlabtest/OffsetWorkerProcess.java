package dev.jinhyeon.notificationlabtest;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Child JVM gate after multiple listener returns, before their native commit RPC. */
public class OffsetWorkerProcess {
    public static void main(String[] args) throws IOException {
        var builder = new SpringApplicationBuilder(dev.jinhyeon.notificationlab.NotificationLabApplication.class,
                OffsetCommitPolicy.Wiring.class);
        if (System.getenv("NOTIFICATION_LAB_OFFSET_MARKER") != null) builder.sources(CommitGate.class);
        var context = (ServletWebServerApplicationContext) builder.run(args);
        OffsetCommitPolicy.requireApplied(context);
        Files.writeString(Path.of(System.getenv("NOTIFICATION_LAB_WORKER_PORT")),
                Integer.toString(context.getWebServer().getPort()), StandardOpenOption.CREATE_NEW);
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class CommitGate {
        @Bean
        static BeanPostProcessor offsetCommitGate(Environment environment) {
            var armed = Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_OFFSET_ARM"));
            var marker = Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_OFFSET_MARKER"));
            var partition = new TopicPartition(environment.getRequiredProperty("app.async.topic"), 0);
            return new BeanPostProcessor() {
                @Override public Object postProcessAfterInitialization(Object bean, String name) {
                    if (bean instanceof DefaultKafkaConsumerFactory<?, ?> factory) addGate(factory, partition, armed, marker);
                    return bean;
                }
            };
        }
    }

    private static <K, V> void addGate(DefaultKafkaConsumerFactory<K, V> factory, TopicPartition partition, Path armed, Path marker) {
        factory.addPostProcessor(target -> gated(target, partition, armed, marker));
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Consumer<K, V> gated(Consumer<K, V> target, TopicPartition partition, Path armed, Path marker) {
        long[] previous = {0}; // Only this consumer thread accesses its successful commit watermark.
        return (Consumer<K, V>) Proxy.newProxyInstance(Consumer.class.getClassLoader(), new Class<?>[] {Consumer.class}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "Offset test gate";
                };
            }
            long attempted = -1;
            if (method.getName().equals("commitSync") && args != null && args.length > 0 && args[0] instanceof Map<?, ?> offsets
                    && offsets.get(partition) instanceof OffsetAndMetadata offset) attempted = offset.offset();
            if (attempted - previous[0] >= 2 && Files.exists(armed)) {
                if (TransactionSynchronizationManager.isActualTransactionActive()) throw new IllegalStateException("Commit gate must be outside JDBC transaction");
                // Aggregate offsets only; no record identifiers, payloads or SQL in this private marker.
                Files.writeString(marker, previous[0] + ":" + attempted, StandardOpenOption.CREATE_NEW);
                try {
                    new CountDownLatch(1).await(Duration.ofSeconds(60).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("Offset test gate ended without the expected process kill");
            }
            try {
                Object result = method.invoke(target, args);
                if (attempted >= 0) previous[0] = attempted;
                return result;
            } catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
    }
}
