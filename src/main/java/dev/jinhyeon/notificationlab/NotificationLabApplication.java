package dev.jinhyeon.notificationlab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;

@SpringBootApplication(exclude = BatchAutoConfiguration.class)
public class NotificationLabApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationLabApplication.class, args);
    }
}
