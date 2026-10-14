package dev.jinhyeon.notificationlabtest;

import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.outbox.OutboxStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Test-only child JVM, outside the application's component scan and executable JAR. */
public class RelayProcess {
    public static void main(String[] args) {
        var builder = new SpringApplicationBuilder(NotificationLabApplication.class);
        if (System.getenv("NOTIFICATION_LAB_ACK_MARKER") != null) {
            builder.sources(AckBoundaryConfiguration.class);
        }
        builder.run(args);
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class AckBoundaryConfiguration {
        @Bean
        @Primary
        OutboxStore ackBoundaryStore(JdbcTemplate jdbc, Environment environment) {
            return new AckBoundaryStore(jdbc, Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_ACK_MARKER")));
        }
    }

    public static class AckBoundaryStore extends OutboxStore {
        private final Path marker;

        public AckBoundaryStore(JdbcTemplate jdbc, Path marker) {
            super(jdbc);
            this.marker = marker;
        }

        @Override
        @Transactional(propagation = Propagation.NOT_SUPPORTED)
        public boolean published(Claim claim) {
            // The production Relay calls this only AFTER send(...).get(...) succeeds.
            // No UPDATE or DB transaction has started. The parent now kills this JVM.
            try {
                Files.writeString(marker, claim.eventId(), StandardOpenOption.CREATE_NEW);
                new CountDownLatch(1).await(90, TimeUnit.SECONDS);
                throw new IllegalStateException("Test ACK boundary was not interrupted within 90 seconds");
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Test ACK boundary interrupted", interrupted);
            }
        }
    }
}
