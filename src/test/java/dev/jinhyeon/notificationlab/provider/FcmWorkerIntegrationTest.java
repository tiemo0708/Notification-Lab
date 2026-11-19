package dev.jinhyeon.notificationlab.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.sun.net.httpserver.HttpServer;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.config.FcmLabProperties;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class FcmWorkerIntegrationTest {
    private static final String RECEIPT = "projects/synthetic-fcm-test/messages/0:100%test";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4.8")
            .withDatabaseName("notification_lab").withUsername("notification_test")
            .withPassword(UUID.randomUUID().toString())
            .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(Arrays.stream(cmd.getExposedPorts())
                    .map(port -> new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.2"))
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(Arrays.stream(cmd.getExposedPorts())
                    .map(port -> new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    @TempDir Path directory;

    @Test
    void workerCorrelatesFcmDataWithDatabaseBeforeCommittingKafkaAndReplayingApiKey() throws Exception {
        var calls = new AtomicInteger();
        var receivedId = new AtomicReference<String>();
        var stub = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stub.createContext("/v1/send", exchange -> {
            try (exchange) {
                calls.incrementAndGet();
                receivedId.set(MAPPER.readTree(exchange.getRequestBody()).at("/message/data/smokeId").asText());
                byte[] response = ("{\"name\":\"" + RECEIPT + "\"}").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        stub.start();
        try {
            String topic = "fcm-worker-" + UUID.randomUUID();
            URI stubEndpoint = URI.create("http://127.0.0.1:" + stub.getAddress().getPort() + "/v1/send");
            try (var app = start(topic, stubEndpoint);
                 var http = HttpClient.newHttpClient();
                 var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                assertThat(app.getBean(PushProvider.class)).isInstanceOf(FcmPushProvider.class);
                var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.getWebServer().getPort() + "/notifications"))
                        .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                        .header("Idempotency-Key", UUID.randomUUID().toString()).POST(HttpRequest.BodyPublishers.ofString("""
                                {"recipientId":"fcm-smoke-emulator","title":"Synthetic","body":"Synthetic"}
                                """)).build();
                var response = http.send(request, HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(202);
                String id = MAPPER.readTree(response.body()).path("notificationId").asText();
                var jdbc = app.getBean(JdbcTemplate.class);
                await().atMost(Duration.ofSeconds(35)).untilAsserted(() -> {
                    assertThat(jdbc.queryForObject("SELECT provider_message_id FROM notification WHERE notification_id = ?", String.class, id))
                            .isEqualTo(RECEIPT);
                    var offsets = admin.listConsumerGroupOffsets(topic).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                    assertThat(offsets.entrySet().stream().filter(e -> e.getKey().topic().equals(topic))
                            .mapToLong(e -> e.getValue().offset()).sum()).isEqualTo(1);
                });
                assertThat(receivedId.get()).isEqualTo(id);
                assertThat(jdbc.queryForList("""
                        SELECT a.outcome FROM delivery_attempt a JOIN delivery_task t USING (task_id) WHERE t.notification_id = ?
                        """, String.class, id)).containsExactly("PROVIDER_ACCEPTED");
                assertThat(directory.resolve("used")).hasContent(id);
                var replay = http.send(request, HttpResponse.BodyHandlers.ofString());
                assertThat(replay.statusCode()).isEqualTo(200);
                assertThat(MAPPER.readTree(replay.body()).path("notificationId").asText()).isEqualTo(id);
                assertThat(calls.get()).isEqualTo(1);
            }
        } finally {
            stub.stop(0);
        }
    }

    private ServletWebServerApplicationContext start(String topic, URI stubEndpoint) throws Exception {
        Path device = Files.writeString(directory.resolve("device"), "synthetic_emulator_registration_token");
        Path access = Files.writeString(directory.resolve("access"), "synthetic.oauth.access.token");
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class)
                .initializers(context -> context.addBeanFactoryPostProcessor(factory -> factory.addBeanPostProcessor(new BeanPostProcessor() {
                    @Override
                    public Object postProcessAfterInitialization(Object bean, String name) {
                        if (!(bean instanceof FcmPushProvider original)) return bean;
                        original.close();
                        return new FcmPushProvider(factory.getBean(FcmLabProperties.class), factory.getBean(ObjectMapper.class), stubEndpoint);
                    }
                }))).run("--server.port=0", "--app.provider.mode=FCM", "--app.delivery.max-attempts=1",
                        "--app.provider.fcm.project-id=synthetic-fcm-test", "--app.provider.fcm.device-token-file=" + device,
                        "--app.provider.fcm.access-token-file=" + access, "--app.provider.fcm.send-marker=" + directory.resolve("used"),
                        "--spring.datasource.url=" + MYSQL.getJdbcUrl(), "--spring.datasource.username=" + MYSQL.getUsername(),
                        "--spring.datasource.password=" + MYSQL.getPassword(), "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--app.async.topic=" + topic, "--app.async.group=" + topic, "--logging.level.root=WARN",
                        "--spring.main.banner-mode=off");
    }
}
