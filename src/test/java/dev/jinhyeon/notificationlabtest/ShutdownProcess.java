package dev.jinhyeon.notificationlabtest;

import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.outbox.OutboxStore;
import dev.jinhyeon.notificationlab.provider.MockPushProvider;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Test-only SIGTERM target. Files coordinate the boundary; production still owns lifecycle/transactions. */
public class ShutdownProcess {
    public static void main(String[] args) throws IOException {
        var builder = new SpringApplicationBuilder(NotificationLabApplication.class, Probes.class);
        if ("relay".equals(System.getenv("NOTIFICATION_LAB_SHUTDOWN_GATE"))) builder.sources(RelayGate.class);
        var context = (ServletWebServerApplicationContext) builder.run(args);
        Files.writeString(Path.of(System.getenv("NOTIFICATION_LAB_PROCESS_DIRECTORY")).resolve("port"),
                Integer.toString(context.getWebServer().getPort()), StandardOpenOption.CREATE_NEW);
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class Probes {
        private final Path directory;

        public Probes(Environment environment) {
            directory = Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_PROCESS_DIRECTORY"));
        }

        @EventListener(ContextClosedEvent.class)
        public void closing() throws IOException {
            Files.writeString(directory.resolve("closing"), "closing", StandardOpenOption.CREATE_NEW);
        }

        @Bean
        @Primary
        PushProvider recordedProvider(MockPushProvider delegate, Environment environment) {
            Path calls = Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_PROVIDER_CALLS"));
            boolean gated = "worker".equals(environment.getProperty("NOTIFICATION_LAB_SHUTDOWN_GATE"));
            var first = new AtomicBoolean(true);
            return request -> {
                var result = delegate.send(request);
                try {
                    Files.writeString(calls, request.notificationId() + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                }
                // Real Mock already accepted; DB completion has not started. Timeout is an uncertain outcome.
                if (gated && first.compareAndSet(true, false)) gate(directory, request.notificationId().toString());
                return result;
            };
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class RelayGate {
        @Bean
        @Primary
        OutboxStore gatedOutboxStore(JdbcTemplate jdbc, @Qualifier("outboxStore") OutboxStore delegate,
                                    Environment environment) {
            return new GatedOutboxStore(jdbc, delegate,
                    Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_PROCESS_DIRECTORY")));
        }
    }

    public static class GatedOutboxStore extends OutboxStore {
        private final OutboxStore delegate;
        private final Path directory;
        private final AtomicBoolean first = new AtomicBoolean(true);

        public GatedOutboxStore(JdbcTemplate jdbc, OutboxStore delegate, Path directory) {
            super(jdbc, new dev.jinhyeon.notificationlab.config.TrafficProperties(
                    dev.jinhyeon.notificationlab.config.TrafficProperties.Lane.REALTIME, false, 2, 2));
            this.delegate = delegate;
            this.directory = directory;
        }

        @Override
        @Transactional(propagation = Propagation.NOT_SUPPORTED)
        public boolean published(Claim claim) {
            if (first.compareAndSet(true, false)) gate(directory, claim.eventId());
            // Re-enter the actual production store through its transaction proxy after the test gate.
            return delegate.published(claim);
        }
    }

    private static void gate(Path directory, String id) {
        try {
            Files.writeString(directory.resolve("entered"), id, StandardOpenOption.CREATE_NEW);
            long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
            while (!Files.exists(directory.resolve("release"))) {
                if (System.nanoTime() >= deadline) throw new IllegalStateException("Shutdown gate timed out");
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ignored) {
                    // Model a call that cannot be cancelled: SIGTERM must still have a bounded process exit.
                }
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}
