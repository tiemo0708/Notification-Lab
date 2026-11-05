package dev.jinhyeon.notificationlab.monitoring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.config.TrafficProperties;
import dev.jinhyeon.notificationlab.outbox.PipelineMetrics;
import dev.jinhyeon.notificationmock.MockPushEndpoint;
import dev.jinhyeon.notificationmock.MockServerApplication;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.yaml.snakeyaml.Yaml;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class MonitoringIntegrationTest {
    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4.8")
            .withDatabaseName("notification_lab").withUsername("notification_test")
            .withPassword(UUID.randomUUID().toString()).withCommand("--default-time-zone=+00:00")
            .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.2"))
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));
    @Container
    static final GenericContainer<?> REDIS = localContainer("redis:7.4.11-alpine", 6379, 128);

    private final ObjectMapper mapper = new ObjectMapper();
    private final String topic = "monitoring-" + UUID.randomUUID();
    private final String quotaKey = "test:monitoring:" + UUID.randomUUID();
    private ServletWebServerApplicationContext realtime;
    private ServletWebServerApplicationContext campaign;
    private ServletWebServerApplicationContext mock;
    private String prometheusUrl;

    @Test
    @Timeout(240)
    void provisionsDashboardAndObservesPausedLagRecoveryAndTargetLossWithoutZeroFallback() throws Exception {
        var output = Files.createTempDirectory(Files.createDirectories(Path.of("build/reports/monitoring")), "observation-");
        var evidence = new LinkedHashMap<String, Object>();
        var phases = new ArrayList<Map<String, Object>>();
        evidence.put("startedAtUtc", Instant.now().toString());
        evidence.put("phases", phases);
        evidence.put("scope", "eight synthetic HTTP Mock acceptances; not a performance or physical-device experiment");
        JsonNode services = mapper.valueToTree(new Yaml().load(Files.readString(Path.of("compose.monitoring.yml"))));
        var prometheusSettings = services.path("services").path("prometheus");
        var grafanaSettings = services.path("services").path("grafana");
        Throwable primaryFailure = null;
        try (var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
             var network = Network.newNetwork();
             var prometheus = localContainer(prometheusSettings.path("image").asText(), 9090, memoryMiB(prometheusSettings));
             var grafana = localContainer(grafanaSettings.path("image").asText(), 3000, memoryMiB(grafanaSettings))) {
            mock = MockServerApplication.start(
                    "--server.port=0", "--mock.http.response-delay-ms=1", "--logging.level.root=WARN", "--spring.main.banner-mode=off");
            try (var redis = new Jedis("127.0.0.1", REDIS.getMappedPort(6379), 3000)) {
                redis.eval(new ClassPathResource("redis/initialize-split-quota.lua").getContentAsString(StandardCharsets.UTF_8),
                        List.of(quotaKey), List.of("10", "10", "2", "2"));
            }
            realtime = application("REALTIME");
            campaign = application("CAMPAIGN");
            await().atMost(Duration.ofSeconds(20)).until(() -> assigned(realtime) == 6 && assigned(campaign) == 6);
            assertThat(realtime.getBean(TrafficProperties.class).lane()).isEqualTo(TrafficProperties.Lane.REALTIME);
            assertThat(campaign.getBean(TrafficProperties.class).lane()).isEqualTo(TrafficProperties.Lane.CAMPAIGN);
            org.testcontainers.Testcontainers.exposeHostPorts(port(realtime), port(campaign));
            String config = Files.readString(Path.of("monitoring/prometheus/prometheus.yml"))
                    .replace("host.docker.internal:8080", "host.testcontainers.internal:" + port(realtime))
                    .replace("host.docker.internal:8082", "host.testcontainers.internal:" + port(campaign));
            Files.writeString(output.resolve("prometheus.yml"), config);
            var commands = new ArrayList<String>();
            prometheusSettings.path("command").forEach(command -> commands.add(command.asText()));
            prometheusSettings.path("environment").properties().forEach(entry -> prometheus.withEnv(entry.getKey(), entry.getValue().asText()));
            prometheus.withNetwork(network).withNetworkAliases("prometheus").withWorkingDirectory("/etc/prometheus")
                    .withCopyFileToContainer(MountableFile.forHostPath(output.resolve("prometheus.yml")), "/etc/prometheus/prometheus.yml")
                    .withCopyFileToContainer(MountableFile.forHostPath("monitoring/prometheus/rules.yml"), "/etc/prometheus/rules.yml")
                    .withCopyFileToContainer(MountableFile.forHostPath("monitoring/prometheus/rules-test.yml"), "/etc/prometheus/rules-test.yml")
                    .withCommand(commands.toArray(String[]::new))
                    .waitingFor(Wait.forHttp("/-/ready").forPort(9090)).start();
            prometheusUrl = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
            var check = prometheus.execInContainer("/bin/promtool", "check", "config", "/etc/prometheus/prometheus.yml");
            assertThat(check.getExitCode()).as(check.getStdout() + check.getStderr()).isZero();
            var rules = prometheus.execInContainer("/bin/promtool", "test", "rules", "/etc/prometheus/rules-test.yml");
            assertThat(rules.getExitCode()).as(rules.getStdout() + rules.getStderr()).isZero();
            evidence.put("promtoolRuleCases", 8);
            grafanaSettings.path("environment").properties().forEach(entry -> grafana.withEnv(entry.getKey(), entry.getValue().asText()));
            grafana.withNetwork(network)
                    .withCopyFileToContainer(MountableFile.forHostPath("monitoring/grafana/provisioning"), "/etc/grafana/provisioning")
                    .withCopyFileToContainer(MountableFile.forHostPath("monitoring/grafana/dashboards"), "/var/lib/grafana/dashboards")
                    .waitingFor(Wait.forHttp("/api/health").forPort(3000)).start();
            String grafanaUrl = "http://" + grafana.getHost() + ":" + grafana.getMappedPort(3000);
            var expected = mapper.readTree(Files.readString(Path.of("monitoring/grafana/dashboards/notification-lab.json")));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                var actual = get(http, grafanaUrl + "/api/dashboards/uid/notification-lab-s8").path("dashboard");
                assertThat(actual.path("title")).isEqualTo(expected.path("title"));
                assertThat(actual.path("panels").size()).isEqualTo(22);
                for (int i = 0; i < 22; i++) {
                    assertThat(actual.path("panels").get(i).path("targets")).isEqualTo(expected.path("panels").get(i).path("targets"));
                    assertThat(actual.path("panels").get(i).path("description")).isEqualTo(expected.path("panels").get(i).path("description"));
                    assertThat(actual.path("panels").get(i).path("gridPos")).isEqualTo(expected.path("panels").get(i).path("gridPos"));
                    assertThat(actual.path("panels").get(i).path("options")).isEqualTo(expected.path("panels").get(i).path("options"));
                }
            });
            assertThat(get(http, grafanaUrl + "/api/datasources/uid/nl-prometheus").path("url").asText()).isEqualTo("http://prometheus:9090");
            evidence.put("dashboardPanels", 22);
            // Introduce counter series before the following scrapes so rate() has an actual baseline.
            create(http, realtime); create(http, campaign);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(values(http, "up{job=\"notification-lab\"}")).containsExactlyInAnyOrder(1.0, 1.0);
                assertThat(values(http, "notification_provider_attempts_total{job=\"notification-lab\"}")).containsExactlyInAnyOrder(1.0, 1.0);
            });
            container(campaign).pause();
            await().atMost(Duration.ofSeconds(10)).until(() -> container(campaign).isContainerPaused());
            for (int i = 0; i < 3; i++) { create(http, campaign); create(http, realtime); }
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED'", Integer.class)).isEqualTo(8);
                assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED' AND traffic_class='REALTIME'", Integer.class)).isEqualTo(4);
                realtime.getBean(PipelineMetrics.class).refresh(); campaign.getBean(PipelineMetrics.class).refresh();
                assertThat(values(http, "max by(lane) (notification_consumer_lag{job=\"notification-lab\",lane=\"REALTIME\"})")).containsExactly(0.0);
                assertThat(values(http, "max by(lane) (notification_consumer_lag{job=\"notification-lab\",lane=\"CAMPAIGN\"})")).containsExactly(3.0);
                assertThat(values(http, "notification_worker_running{job=\"notification-lab\",lane=\"CAMPAIGN\"}")).containsExactly(1.0);
            });
            phases.add(snapshot(http, "campaignPausedRealtimeCompleted"));
            container(campaign).resume();
            await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(8);
                assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM delivery_attempt WHERE outcome='PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(8);
                realtime.getBean(PipelineMetrics.class).refresh(); campaign.getBean(PipelineMetrics.class).refresh();
                assertThat(values(http, "notification_consumer_lag{job=\"notification-lab\"}")).containsExactlyInAnyOrder(0.0, 0.0);
                assertThat(values(http, "notification_completed_total{job=\"notification-lab\",status=\"PROVIDER_ACCEPTED\"}")).containsExactlyInAnyOrder(4.0, 4.0);
            });
            assertThat(mock.getBean(MockPushEndpoint.class).stats().accepted()).isEqualTo(8);
            for (var panel : expected.path("panels")) {
                for (var target : panel.path("targets")) {
                    String expression = target.path("expr").asText().replace("$__rate_interval", "30s").replace("$lane", ".*");
                    query(http, expression); // Optional/lazy series may be empty, but every PromQL must execute successfully.
                }
            }
            assertThat(values(http, "sum by(lane) (rate(notification_provider_attempts_total{job=\"notification-lab\"}[30s]))"))
                    .hasSize(2).allMatch(value -> Double.isFinite(value) && value > 0);
            assertThat(values(http, "histogram_quantile(0.95, sum by(lane,status,le) (rate(notification_processing_seconds_bucket{job=\"notification-lab\"}[30s])))"))
                    .hasSize(2).allMatch(value -> Double.isFinite(value) && value > 0);
            var proxied = get(http, grafanaUrl + "/api/datasources/proxy/uid/nl-prometheus/api/v1/query?query="
                    + URLEncoder.encode("up{job=\"notification-lab\"}", StandardCharsets.UTF_8));
            assertThat(proxied.path("status").asText()).isEqualTo("success");
            assertThat(proxied.path("data").path("result").size()).isEqualTo(2);
            phases.add(snapshot(http, "bothRecoveredEightAcceptances"));
            campaign.close(); campaign = null;
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(values(http, "up{job=\"notification-lab\",lane=\"CAMPAIGN\"}")).containsExactly(0.0);
                assertThat(values(http, "up{job=\"notification-lab\",lane=\"REALTIME\"}")).containsExactly(1.0);
                assertThat(values(http, "notification_consumer_lag{job=\"notification-lab\",lane=\"CAMPAIGN\"}")).isEmpty();
            });
            phases.add(snapshot(http, "campaignTargetLostWithoutZeroLagFallback"));
            evidence.put("httpAccepted", 8); evidence.put("logicalTasks", 8);
            evidence.put("dashboardQueriesExecuted", expected.path("panels").findValues("expr").size());
            evidence.put("result", "PASS");
            System.out.println("MONITORING panels=22 rules=8 httpAccepted=8 pausedCampaignLag=3 recoveredLag=0 lostTargetUp=0 lostTargetLag=absent");
        } catch (Exception | AssertionError failure) {
            primaryFailure = failure; evidence.put("result", "FAIL");
            evidence.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            try {
                RuntimeException cleanup = null;
                for (var context : Arrays.asList(campaign, realtime, mock)) {
                    if (context == null) continue;
                    try { context.close(); }
                    catch (RuntimeException failure) {
                        if (cleanup == null) cleanup = failure; else cleanup.addSuppressed(failure);
                    }
                }
                if (cleanup != null) throw cleanup;
            } catch (RuntimeException failure) {
                if (primaryFailure != null) primaryFailure.addSuppressed(failure);
                else { primaryFailure = failure; evidence.put("result", "FAIL"); throw failure; }
            } finally {
                evidence.put("finishedAtUtc", Instant.now().toString());
                try { mapper.writerWithDefaultPrettyPrinter().writeValue(output.resolve("result.json").toFile(), evidence); }
                catch (java.io.IOException failure) {
                    if (primaryFailure != null) primaryFailure.addSuppressed(failure); else throw failure;
                }
            }
        }
    }

    private static GenericContainer<?> localContainer(String image, int port, int memoryMiB) {
        return new GenericContainer<>(image).withExposedPorts(port)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(memoryMiB * 1024L * 1024)
                        .withPortBindings(new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), new com.github.dockerjava.api.model.ExposedPort(port))));
    }

    private int memoryMiB(JsonNode service) {
        String limit = service.path("mem_limit").asText();
        assertThat(limit).matches("[0-9]+m");
        int value = Integer.parseInt(limit.substring(0, limit.length() - 1));
        assertThat(value).isBetween(128, 512);
        return value;
    }

    private ServletWebServerApplicationContext application(String lane) {
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(NotificationLabApplication.class).run(
                "--server.port=0", "--app.provider.mode=HTTP", "--app.traffic.split-enabled=true", "--app.traffic.lane=" + lane,
                "--app.provider.http.base-url=http://127.0.0.1:" + port(mock),
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(), "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(), "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + topic,
                "--app.rate-limit.client-enabled=true", "--app.rate-limit.port=" + REDIS.getMappedPort(6379), "--app.rate-limit.key=" + quotaKey,
                "--logging.level.root=WARN", "--spring.main.banner-mode=off");
    }

    private JsonNode get(HttpClient http, String url) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("GET %s", URI.create(url).getPath()).isEqualTo(200);
        return mapper.readTree(response.body());
    }

    private JsonNode query(HttpClient http, String expression) throws Exception {
        var response = get(http, prometheusUrl + "/api/v1/query?query=" + URLEncoder.encode(expression, StandardCharsets.UTF_8));
        assertThat(response.path("status").asText()).as(expression).isEqualTo("success");
        return response.path("data").path("result");
    }

    private List<Double> values(HttpClient http, String expression) throws Exception {
        var result = new ArrayList<Double>();
        for (var series : query(http, expression)) result.add(Double.parseDouble(series.path("value").get(1).asText()));
        return result;
    }

    private Map<String, Object> snapshot(HttpClient http, String phase) throws Exception {
        var snapshot = new LinkedHashMap<String, Object>(); snapshot.put("phase", phase);
        for (String name : List.of("up", "notification_worker_running", "notification_consumer_lag", "outbox_unpublished",
                "outbox_sample_age_seconds", "notification_consumer_sample_age_seconds", "notification_completed_total")) {
            snapshot.put(name, query(http, name + "{job=\"notification-lab\"}"));
        }
        return snapshot;
    }

    private void create(HttpClient http, ServletWebServerApplicationContext app) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(app) + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"recipientId\":\"synthetic\",\"title\":\"test\",\"body\":\"synthetic\"}"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
    }

    private JdbcTemplate jdbc() { return realtime.getBean(JdbcTemplate.class); }
    private int port(ServletWebServerApplicationContext app) { return app.getWebServer().getPort(); }
    private ConcurrentMessageListenerContainer<?, ?> container(ServletWebServerApplicationContext app) {
        return (ConcurrentMessageListenerContainer<?, ?>) app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
    }
    private int assigned(ServletWebServerApplicationContext app) {
        var assigned = container(app).getAssignedPartitions(); return assigned == null ? 0 : assigned.size();
    }
}
