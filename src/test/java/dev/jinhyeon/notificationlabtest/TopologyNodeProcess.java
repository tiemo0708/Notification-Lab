package dev.jinhyeon.notificationlabtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.outbox.OutboxRelay;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/** Only publishes readiness metadata; all application beans and HTTP provider are production code. */
public class TopologyNodeProcess {
    public static void main(String[] args) throws Exception {
        var context = (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class).run(args);
        var registry = context.getBean(KafkaListenerEndpointRegistry.class);
        var metadata = Map.of("port", context.getWebServer().getPort(), "pid", ProcessHandle.current().pid(),
                "listenerRunning", registry.getListenerContainers().stream().allMatch(container -> container.isRunning()),
                "listenerCount", registry.getListenerContainers().size(),
                "relayPresent", !context.getBeansOfType(OutboxRelay.class).isEmpty());
        Path ready = Path.of(System.getenv("NOTIFICATION_LAB_TOPOLOGY_READY"));
        Path temporary = ready.resolveSibling(ready.getFileName() + ".tmp");
        Files.writeString(temporary, new ObjectMapper().writeValueAsString(metadata), StandardOpenOption.CREATE_NEW);
        Files.move(temporary, ready, StandardCopyOption.ATOMIC_MOVE);
    }
}
