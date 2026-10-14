package dev.jinhyeon.notificationlabtest;

import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.provider.MockPushProvider;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

/** Pauses only the first provider call so a second real consumer can join during processing. */
public class RebalanceWorkerProcess {
    public static void main(String[] args) throws IOException {
        var builder = new SpringApplicationBuilder(NotificationLabApplication.class);
        if (System.getenv("NOTIFICATION_LAB_PROVIDER_GATE") != null) {
            builder.sources(ProviderGateConfiguration.class);
        }
        var context = (ServletWebServerApplicationContext) builder.run(args);
        Files.writeString(Path.of(System.getenv("NOTIFICATION_LAB_WORKER_PORT")),
                Integer.toString(context.getWebServer().getPort()), StandardOpenOption.CREATE_NEW);
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class ProviderGateConfiguration {
        @Bean
        @Primary
        PushProvider gatedProvider(MockPushProvider delegate, Environment environment) {
            Path gate = Path.of(environment.getRequiredProperty("NOTIFICATION_LAB_PROVIDER_GATE"));
            var first = new AtomicBoolean(true);
            return request -> {
                if (first.compareAndSet(true, false)) {
                    try {
                        Files.writeString(gate.resolve("entered"), request.notificationId().toString(),
                                StandardOpenOption.CREATE_NEW);
                        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
                        while (!Files.exists(gate.resolve("release"))) {
                            if (System.nanoTime() >= deadline) {
                                throw new IllegalStateException("Provider gate exceeded its bounded wait");
                            }
                            Thread.sleep(20);
                        }
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Provider gate interrupted", interrupted);
                    }
                }
                return delegate.send(request);
            };
        }
    }
}
