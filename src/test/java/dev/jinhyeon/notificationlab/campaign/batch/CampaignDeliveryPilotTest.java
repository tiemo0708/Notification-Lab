package dev.jinhyeon.notificationlab.campaign.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.campaign.CampaignRequest;
import dev.jinhyeon.notificationlab.campaign.CampaignService;
import dev.jinhyeon.notificationlab.config.TrafficRouting;
import dev.jinhyeon.notificationmock.MockPushEndpoint;
import dev.jinhyeon.notificationmock.MockServerApplication;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
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
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.batch.core.BatchStatus;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Bounded calibration before the 100k/1m delivery experiments. All data is synthetic. */
@Tag("campaign-delivery-pilot")
class CampaignDeliveryPilotTest {
    private static final int MAX_PROBES = 5000;
    private static final int BASELINE_PROBES = 50;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Probe> probes = new ArrayList<>();
    private final Map<String, Object> report = new LinkedHashMap<>();
    private final String quotaKey = "test:delivery-pilot:" + UUID.randomUUID();
    private String mockUrl;

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void reconcilesHttpDeliveryAndComparesRealtimeLatencyDuringCampaignDrain() throws Exception {
        int targets = Integer.getInteger("campaign.delivery.targets", 10_000);
        if (targets < 1000 || targets > 10_000) throw new IllegalArgumentException("Pilot targets must be 1000..10000");
        var originalZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        report.put("startedAtUtc", Instant.now().toString());
        report.put("scope", "preloaded campaign HTTP delivery pilot; same JVM independent app contexts");
        report.put("targets", targets);
        report.put("redisConnectionsPerLane", 3);
        report.put("settings", Map.of("totalRate", 1000, "totalBurst", 100, "campaignRate", 200,
                "campaignBurst", 20, "workersPerLane", 3, "httpConnectionsPerLane", 3,
                "probeDelayMillis", 200, "baselineProbes", BASELINE_PROBES, "mockDelayMillis", 1));
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"));
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        report.put("infra", Map.of("mysql", "8.4.8", "mysqlCpu", 2, "mysqlMemoryMiB", 2048,
                "mysqlBufferPoolMiB", 512, "kafka", "4.0.2", "kafkaCpu", 1, "kafkaMemoryMiB", 1024,
                "redis", "7.4.11", "redisMemoryMiB", 256));
        try (var mysql = new MySQLContainer<>("mysql:8.4.8")
                     .withDatabaseName("notification_lab").withUsername("notification_test").withPassword(UUID.randomUUID().toString())
                     .withCommand("--default-time-zone=+00:00", "--innodb-buffer-pool-size=536870912")
                     .withUrlParam("connectionTimeZone", "UTC").withUrlParam("forceConnectionTimeZoneToSession", "true")
                     .withUrlParam("connectTimeout", "3000").withUrlParam("socketTimeout", "60000")
                     .withCreateContainerCmdModifier(cmd -> limit(cmd, 2048, 2));
             var kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.2"))
                     .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
                     .withCreateContainerCmdModifier(cmd -> limit(cmd, 1024, 1));
             var redis = new GenericContainer<>("redis:7.4.11-alpine").withExposedPorts(6379)
                     .withCommand("redis-server", "--maxmemory", "128mb", "--maxmemory-policy", "noeviction")
                     .withCreateContainerCmdModifier(cmd -> limit(cmd, 256, 1));
             var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            mysql.start(); kafka.start(); redis.start();
            UUID campaignId;
            try (var batch = application(mysql, kafka, redis, "CAMPAIGN", false)) {
                var service = batch.getBean(CampaignService.class);
                campaignId = service.create(new CampaignRequest("delivery pilot", "CAMPAIGN", "synthetic body", targets), UUID.randomUUID().toString())
                        .campaign().campaignId();
                service.start(campaignId);
                long begin = System.nanoTime();
                var execution = batch.getBean(CampaignBatchService.class).execute(campaignId);
                assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
                report.put("preloadSeconds", seconds(begin));
                assertThat(batch.getBean(JdbcTemplate.class).queryForObject("SELECT COUNT(*) FROM delivery_attempt", Long.class)).isZero();
            }
            try (var connection = new Jedis("127.0.0.1", redis.getMappedPort(6379))) {
                assertThat(connection.eval(new ClassPathResource("redis/initialize-split-quota.lua")
                        .getContentAsString(StandardCharsets.UTF_8), List.of(quotaKey), List.of("1000", "100", "200", "20")))
                        .isEqualTo("QUOTA_INITIALIZED_EMPTY");
            }
            var audit = new Audit(campaignId, targets);
            var mockApplication = new SpringApplication(MockServerApplication.class, AuditConfiguration.class);
            mockApplication.setDefaultProperties(Map.of("spring.config.name", "mock-server"));
            mockApplication.addInitializers(context -> context.getBeanFactory().registerSingleton("pilotAudit", audit));
            try (var mock = (ServletWebServerApplicationContext) mockApplication.run("--server.port=0",
                         "--mock.http.mode=SUCCESS", "--mock.http.response-delay-ms=1", "--mock.http.max-in-flight=8",
                         "--mock.http.requests-per-window=10000", "--mock.http.window-ms=1000",
                         "--logging.level.root=WARN", "--spring.main.banner-mode=off")) {
                mockUrl = "http://127.0.0.1:" + mock.getWebServer().getPort();
                try (var realtime = application(mysql, kafka, redis, "REALTIME", true);
                     var admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
                    ready(realtime);
                    var jdbc = realtime.getBean(JdbcTemplate.class);
                    long baselineStart = System.nanoTime();
                    for (int i = 0; i < BASELINE_PROBES; i++) {
                        probe(http, realtime, "BASELINE");
                        Thread.sleep(200); // Sequential offered load: no unbounded/catch-up submission queue.
                    }
                    await().atMost(Duration.ofSeconds(30)).until(() -> accepted(jdbc, "REALTIME") == BASELINE_PROBES);
                    awaitOffsets(admin, realtime, BASELINE_PROBES);
                    report.put("baselineSeconds", seconds(baselineStart));
                    report.put("baselineLatencyMs", latency(jdbc, "title='BASELINE' AND traffic_class='REALTIME'"));
                    long drainStart = System.nanoTime(); // Includes campaign app startup; excludes preload and baseline.
                    var samples = new ArrayList<Map<String, Object>>();
                    report.put("samples", samples);
                    try (var campaign = application(mysql, kafka, redis, "CAMPAIGN", true)) {
                        ready(campaign);
                        long nextSample = 0;
                        while (completed(campaign) < targets) {
                            if (seconds(drainStart) > 900) throw new IllegalStateException("Pilot drain exceeded 15 minutes");
                            checkRunning(campaign, "campaign");
                            checkRunning(realtime, "realtime");
                            probe(http, realtime, "MIXED");
                            if (System.nanoTime() >= nextSample) {
                                samples.add(sample(mysql, campaign, realtime, drainStart));
                                nextSample = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                                System.out.printf("DELIVERY_PILOT targets=%d completed=%.0f realtimeProbes=%d elapsedSeconds=%.3f%n",
                                        targets, completed(campaign), probes.size() - BASELINE_PROBES, seconds(drainStart));
                            }
                            Thread.sleep(200);
                        }
                        report.put("campaignDrainSeconds", seconds(drainStart));
                        report.put("campaignPerSecond", targets / seconds(drainStart));
                        samples.add(sample(mysql, campaign, realtime, drainStart));
                        int mixed = probes.size() - BASELINE_PROBES;
                        assertThat(mixed).as("overlapping realtime observations").isGreaterThanOrEqualTo(10);
                        await().atMost(Duration.ofSeconds(30)).until(() -> accepted(jdbc, "REALTIME") == probes.size());
                        awaitOffsets(admin, campaign, targets);
                        awaitOffsets(admin, realtime, probes.size());
                        report.put("mixedProbes", mixed);
                        report.put("mixedLatencyMs", latency(jdbc, "title='MIXED' AND traffic_class='REALTIME'"));
                        report.put("campaignCreatedToCompletedMs", latency(jdbc, "traffic_class='CAMPAIGN'"));
                        reconcile(jdbc, targets, audit, mock.getBean(MockPushEndpoint.class));
                        report.put("campaignOffsets", offsets(admin, campaign));
                        report.put("realtimeOffsets", offsets(admin, realtime));
                        report.put("mockStats", mock.getBean(MockPushEndpoint.class).stats());
                        report.put("campaignAllowed", counter(campaign, "notification.rate.limit.decisions", "outcome", "allowed"));
                        report.put("realtimeAllowed", counter(realtime, "notification.rate.limit.decisions", "outcome", "allowed"));
                        assertThat(counter(campaign, "notification.rate.limit.decisions", "outcome", "allowed")).isEqualTo(targets);
                        assertThat(counter(realtime, "notification.rate.limit.decisions", "outcome", "allowed")).isEqualTo(probes.size());
                        report.put("result", "PASS");
                    }
                }
            }
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            TimeZone.setDefault(originalZone);
            report.put("finishedAtUtc", Instant.now().toString());
            Path directory = Path.of("build/reports/campaign-delivery-pilot");
            Files.createDirectories(directory);
            mapper.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("pilot-" + targets + ".json").toFile(), report);
        }
    }

    private ConfigurableApplicationContext application(MySQLContainer<?> mysql, KafkaContainer kafka,
                                                       GenericContainer<?> redis, String lane, boolean live) {
        return new SpringApplicationBuilder(NotificationLabApplication.class)
                .web(live ? WebApplicationType.SERVLET : WebApplicationType.NONE).run(
                        "--server.port=0", "--app.provider.mode=HTTP", "--app.provider.http.base-url=" + (mockUrl == null ? "http://127.0.0.1:18081" : mockUrl),
                        "--app.provider.http.max-connections=3", "--app.worker.consumers=3", "--app.worker.max-active=3",
                        "--app.worker.queue-capacity=0", "--app.campaign.batch.enabled=" + !live,
                        "--app.campaign.batch.chunk-size=100", "--app.async.relay-enabled=" + live, "--app.async.worker-enabled=" + live,
                        "--app.traffic.split-enabled=true", "--app.traffic.lane=" + lane,
                        "--app.traffic.campaign-rate=200", "--app.traffic.campaign-burst=20",
                        "--app.rate-limit.client-enabled=true", "--app.rate-limit.pause-enabled=true", "--app.rate-limit.max-connections=3",
                        "--app.rate-limit.rate-per-second=1000", "--app.rate-limit.burst=100",
                        "--app.rate-limit.port=" + redis.getMappedPort(6379), "--app.rate-limit.key=" + quotaKey,
                        "--spring.datasource.url=" + mysql.getJdbcUrl(), "--spring.datasource.username=" + mysql.getUsername(),
                        "--spring.datasource.password=" + mysql.getPassword(), "--spring.datasource.hikari.maximum-pool-size=4",
                        "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "--logging.level.root=WARN", "--spring.main.banner-mode=off");
    }

    private void probe(HttpClient http, ConfigurableApplicationContext app, String phase) throws Exception {
        int ordinal = probes.size() + 1;
        if (ordinal > MAX_PROBES) throw new IllegalStateException("Probe bound exceeded");
        String body = mapper.writeValueAsString(Map.of("recipientId", "pilot-realtime-" + ordinal, "title", phase, "body", "synthetic body"));
        long start = System.nanoTime();
        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + ((ServletWebServerApplicationContext) app).getWebServer().getPort() + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        probes.add(new Probe(mapper.readTree(response.body()).path("notificationId").asText(), seconds(start) * 1000));
    }

    private void reconcile(JdbcTemplate jdbc, int targets, Audit audit, MockPushEndpoint mock) {
        int total = targets + probes.size();
        for (String table : List.of("notification", "delivery_task", "outbox_event", "delivery_attempt")) {
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class)).as(table).isEqualTo(total);
        }
        assertThat(accepted(jdbc, "CAMPAIGN")).isEqualTo(targets);
        assertThat(accepted(jdbc, "REALTIME")).isEqualTo(probes.size());
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM campaign_target", Long.class)).isEqualTo(targets);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM delivery_task WHERE attempt_count=1", Long.class)).isEqualTo(total);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM delivery_attempt WHERE attempt_no=1 AND outcome='PROVIDER_ACCEPTED'", Long.class)).isEqualTo(total);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED' AND event_type='NOTIFICATION_REQUESTED'", Long.class)).isEqualTo(total);
        await().atMost(Duration.ofSeconds(5)).until(() -> audit.count() == total);
        assertThat(audit.invalid).isZero(); assertThat(audit.duplicates).isZero();
        assertThat(audit.campaignSeen.cardinality()).isEqualTo(targets);
        assertThat(audit.realtimeSeen.cardinality()).isEqualTo(probes.size());
        for (int i = 0; i < probes.size(); i++) assertThat(audit.realtimeIds[i + 1]).isEqualTo(probes.get(i).id());
        assertThat(mock.stats().requests()).isEqualTo(total);
        assertThat(mock.stats().accepted()).isEqualTo(total);
        assertThat(mock.stats().active()).isZero();
        assertThat(mock.stats().rateLimited() + mock.stats().serverErrors() + mock.stats().busy() + mock.stats().permanentFailures()).isZero();
        report.put("httpAudit", Map.of("uniqueCampaign", audit.campaignSeen.cardinality(), "uniqueRealtime", audit.realtimeSeen.cardinality(), "duplicates", audit.duplicates, "invalid", audit.invalid));
        report.put("baselineApiLatencyMs", percentiles(probes.subList(0, BASELINE_PROBES).stream().map(Probe::apiMillis).toList()));
        report.put("mixedApiLatencyMs", percentiles(probes.subList(BASELINE_PROBES, probes.size()).stream().map(Probe::apiMillis).toList()));
    }

    private Map<String, Object> sample(MySQLContainer<?> mysql, ConfigurableApplicationContext campaign,
                                       ConfigurableApplicationContext realtime, long start) throws Exception {
        var result = new LinkedHashMap<String, Object>();
        result.put("seconds", seconds(start)); result.put("campaignCompleted", completed(campaign));
        result.put("heapUsedBytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        result.put("processCpuNanos", ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime());
        result.put("gcMillis", ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(gc -> gc.getCollectionTime()).sum());
        result.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
        for (var entry : Map.of("campaign", campaign, "realtime", realtime).entrySet()) {
            var registry = entry.getValue().getBean(MeterRegistry.class);
            var gauges = new LinkedHashMap<String, Double>();
            for (String name : List.of("notification.consumer.lag", "outbox.unpublished", "outbox.oldest.age.seconds",
                    "outbox.sample.age.seconds", "notification.consumer.sample.age.seconds", "notification.worker.active", "notification.worker.waiting")) {
                var gauge = registry.find(name).gauge();
                gauges.put(name, gauge == null ? null : gauge.value());
            }
            result.put(entry.getKey(), gauges);
        }
        var memory = mysql.execInContainer("cat", "/sys/fs/cgroup/memory.current");
        assertThat(memory.getExitCode()).isZero();
        result.put("mysqlCgroupMemoryBytes", Long.parseLong(memory.getStdout().strip()));
        return result;
    }

    private Map<String, Object> latency(JdbcTemplate jdbc, String condition) {
        return percentiles(jdbc.queryForList("SELECT TIMESTAMPDIFF(MICROSECOND,created_at,completed_at)/1000.0 FROM notification WHERE " + condition, Double.class));
    }

    private Map<String, Object> percentiles(List<Double> values) {
        assertThat(values).isNotEmpty();
        var sorted = values.stream().sorted().toList();
        assertThat(sorted.getFirst()).isGreaterThanOrEqualTo(0);
        return Map.of("count", sorted.size(), "p50", sorted.get((int) Math.ceil(sorted.size() * .50) - 1),
                "p95", sorted.get((int) Math.ceil(sorted.size() * .95) - 1), "p99", sorted.get((int) Math.ceil(sorted.size() * .99) - 1), "max", sorted.getLast());
    }

    private long accepted(JdbcTemplate jdbc, String lane) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM notification WHERE traffic_class=? AND status='PROVIDER_ACCEPTED'", Long.class, lane);
    }

    private double completed(ConfigurableApplicationContext app) { return counter(app, "notification.completed", "status", "PROVIDER_ACCEPTED"); }
    private double counter(ConfigurableApplicationContext app, String name, String key, String value) {
        var counter = app.getBean(MeterRegistry.class).find(name).tag(key, value).counter();
        return counter == null ? 0 : counter.count();
    }
    private void checkRunning(ConfigurableApplicationContext app, String lane) {
        if (!running(app)) {
            var diagnostics = new LinkedHashMap<String, Double>();
            diagnostics.put("quotaUnavailable", counter(app, "notification.rate.limit.decisions", "outcome", "unavailable"));
            diagnostics.put("quotaRejectedUnavailable", counter(app, "notification.worker.quota.rejected", "reason", "unavailable"));
            diagnostics.put("admissionFull", counter(app, "notification.worker.admission.rejected", "reason", "full"));
            report.put(lane + "StopDiagnostics", diagnostics);
            throw new IllegalStateException("Worker stopped: " + lane);
        }
    }
    private boolean running(ConfigurableApplicationContext app) {
        return app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker").isRunning();
    }
    private void ready(ConfigurableApplicationContext app) {
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            var partitions = app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker").getAssignedPartitions();
            return partitions != null && partitions.size() == 6;
        });
    }
    private List<Map<String, Object>> offsets(Admin admin, ConfigurableApplicationContext app) throws Exception {
        var routing = app.getBean(TrafficRouting.class);
        var topics = admin.describeTopics(List.of(routing.topic(), routing.retryTopic(), routing.deadLetterTopic())).allTopicNames().get(5, TimeUnit.SECONDS);
        var requested = new LinkedHashMap<TopicPartition, OffsetSpec>();
        topics.forEach((name, topic) -> topic.partitions().forEach(p -> requested.put(new TopicPartition(name, p.partition()), OffsetSpec.latest())));
        var ends = admin.listOffsets(requested).all().get(5, TimeUnit.SECONDS);
        var committed = admin.listConsumerGroupOffsets(routing.group()).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        var rows = new ArrayList<Map<String, Object>>();
        for (var entry : ends.entrySet()) {
            var partition = entry.getKey();
            long offset = committed.containsKey(partition) ? committed.get(partition).offset() : 0;
            rows.add(Map.of("topic", partition.topic(), "partition", partition.partition(), "end", entry.getValue().offset(), "committed", offset));
        }
        return rows;
    }
    private void awaitOffsets(Admin admin, ConfigurableApplicationContext app, long count) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var rows = offsets(admin, app);
            assertThat(rows.stream().mapToLong(row -> ((Number) row.get("end")).longValue()).sum()).isEqualTo(count);
            rows.forEach(row -> assertThat(row.get("committed")).isEqualTo(row.get("end")));
        });
    }
    private static double seconds(long start) { return (System.nanoTime() - start) / 1_000_000_000.0; }
    private static void limit(CreateContainerCmd cmd, long mib, int cpus) {
        cmd.getHostConfig().withMemory(mib * 1024 * 1024).withNanoCPUs(cpus * 1_000_000_000L)
                .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port -> new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList());
    }
    private record Probe(String id, double apiMillis) { }

    static class Audit {
        private final ObjectMapper mapper = new ObjectMapper();
        private final UUID campaignId;
        private final int targets;
        private final BitSet campaignSeen = new BitSet(10_001);
        private final BitSet realtimeSeen = new BitSet(MAX_PROBES + 1);
        private final String[] realtimeIds = new String[MAX_PROBES + 1];
        private int invalid;
        private int duplicates;
        Audit(UUID campaignId, int targets) { this.campaignId = campaignId; this.targets = targets; }
        synchronized int count() { return campaignSeen.cardinality() + realtimeSeen.cardinality() + duplicates + invalid; }
        synchronized void observe(byte[] body, int status) {
            try {
                var value = mapper.readTree(body);
                String recipient = value.path("recipientId").asText();
                boolean campaign = recipient.startsWith("synthetic-v1-");
                String prefix = campaign ? "synthetic-v1-" : "pilot-realtime-";
                if (!recipient.startsWith(prefix) || status != 202) { invalid++; return; }
                int ordinal = Integer.parseInt(recipient.substring(prefix.length()));
                if (ordinal < 1 || ordinal > (campaign ? targets : MAX_PROBES)) { invalid++; return; }
                String id = value.path("notificationId").asText();
                if (campaign && !id.equals(CampaignTargetIds.of(campaignId, ordinal).notificationId().toString())) { invalid++; return; }
                var seen = campaign ? campaignSeen : realtimeSeen;
                if (seen.get(ordinal)) duplicates++;
                else { seen.set(ordinal); if (!campaign) realtimeIds[ordinal] = id; }
            } catch (Exception failure) { invalid++; }
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class AuditConfiguration {
        @Bean
        FilterRegistrationBean<OncePerRequestFilter> pilotHttpAudit(Audit audit) {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>();
            registration.setFilter(new OncePerRequestFilter() {
                @Override
                protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
                        throws ServletException, IOException {
                    var cached = new ContentCachingRequestWrapper(request, 8192);
                    try { chain.doFilter(cached, response); }
                    finally { audit.observe(cached.getContentAsByteArray(), response.getStatus()); }
                }
            });
            registration.addUrlPatterns("/pushes");
            return registration;
        }
    }
}
