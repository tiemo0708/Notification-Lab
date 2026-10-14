package dev.jinhyeon.notificationlabtest;

import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.notification.Notification;
import dev.jinhyeon.notificationlab.notification.NotificationService;
import dev.jinhyeon.notificationlab.notification.NotificationStore;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Test-only child JVM, outside the application's component scan and executable JAR. */
public class WorkerProcess {
    public static void main(String[] args) throws IOException {
        var builder = new SpringApplicationBuilder(NotificationLabApplication.class);
        if (System.getenv("NOTIFICATION_LAB_COMPLETION_MARKER") != null) {
            builder.sources(CompletionBoundaryConfiguration.class);
        }
        var context = (ServletWebServerApplicationContext) builder.run(args);
        Files.writeString(Path.of(System.getenv("NOTIFICATION_LAB_WORKER_PORT")),
                Integer.toString(context.getWebServer().getPort()), StandardOpenOption.CREATE_NEW);
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class CompletionBoundaryConfiguration {
        @Bean
        @Primary
        NotificationService completionBoundaryService(NotificationStore store, PushProvider provider,
                Clock clock, MeterRegistry registry, Environment environment) {
            return new CompletionBoundaryService(store, provider, clock, registry,
                    Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_COMPLETION_MARKER")));
        }
    }

    public static class CompletionBoundaryService extends NotificationService {
        private final Path marker;

        public CompletionBoundaryService(NotificationStore store, PushProvider provider,
                Clock clock, MeterRegistry registry, Path marker) {
            super(store, provider, clock, registry);
            this.marker = marker;
        }

        @Override
        public void recordCompletion(Notification completed, Timer.Sample sample) {
            // Production WorkerStore.complete() has returned through its transaction proxy.
            // The production Kafka listener is still on this stack and cannot return/commit yet.
            if (TransactionSynchronizationManager.isActualTransactionActive()) {
                throw new IllegalStateException("Completion boundary must be outside the DB transaction");
            }
            super.recordCompletion(completed, sample);
            try {
                Files.writeString(marker, completed.notificationId().toString(), StandardOpenOption.CREATE_NEW);
                new CountDownLatch(1).await(90, TimeUnit.SECONDS);
                throw new IllegalStateException("Test completion boundary was not interrupted within 90 seconds");
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Test completion boundary interrupted", interrupted);
            }
        }
    }
}
