package dev.jinhyeon.notificationmock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, FlywayAutoConfiguration.class,
        KafkaAutoConfiguration.class})
@EnableConfigurationProperties(HttpMockProperties.class)
public class MockServerApplication {
    public static void main(String[] args) {
        start(args);
    }

    public static ServletWebServerApplicationContext start(String... args) {
        var app = new SpringApplication(MockServerApplication.class);
        app.setDefaultProperties(java.util.Map.of("spring.config.name", "mock-server"));
        return (ServletWebServerApplicationContext) app.run(args);
    }
}
