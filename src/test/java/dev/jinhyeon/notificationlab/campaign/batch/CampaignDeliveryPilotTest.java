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
import dev.jinhyeon.notificationlabtest.OffsetCommitPolicy;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.sql.DriverManager;
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

/** Shared bounded harness for the pilot and the explicit 100k/1m delivery experiments. */
@Tag("campaign-delivery-pilot")
class CampaignDeliveryPilotTest {
    private static final int BASELINE_PROBES = 50;
    private static final int QUERY_PAGE = 1000;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Probe> probes = new ArrayList<>();
    private final Map<String, Object> report = new LinkedHashMap<>();
    private final String quotaKey = "test:delivery-pilot:" + UUID.randomUUID();
    private String mockUrl;
    private int maxProbes;
    private int relayInstances;
    private ApiRequestTiming apiRequestTiming;
    private ApiDbObservation apiDbObservation;
    private ApiFailurePilotSupport failureSupport;
    private final Map<String, DeliveryTiming> timings = new LinkedHashMap<>();

    @Test
    void reconcilesHttpDeliveryAndComparesRealtimeLatencyDuringCampaignDrain() throws Exception {
        boolean load = Boolean.getBoolean("campaign.delivery.load");
        boolean scaling = Boolean.getBoolean("campaign.delivery.scaling");
        boolean fineTiming = Boolean.getBoolean("campaign.delivery.fineTiming");
        boolean offsetComparison = Boolean.getBoolean("campaign.delivery.offsetComparison");
        boolean apiTiming = Boolean.getBoolean("campaign.delivery.apiTiming");
        boolean dbObservation = Boolean.getBoolean("campaign.delivery.dbObservation");
        boolean connectionPilot = Boolean.getBoolean("campaign.delivery.connectionPilot");
        boolean failureWindow = Boolean.getBoolean("campaign.delivery.failureWindow");
        if (connectionPilot && (!apiTiming || dbObservation || load)) throw new IllegalArgumentException("Connection pilot requires only bounded API timing");
        if (dbObservation && !apiTiming) throw new IllegalArgumentException("DB observation requires API timing");
        if (apiTiming && (load || !offsetComparison)) throw new IllegalArgumentException("API timing requires the small offset pilot");
        var offsetMode = offsetComparison ? OffsetCommitPolicy.mode(System.getProperty("campaign.delivery.offsetMode", "RECORD")) : OffsetCommitPolicy.Mode.RECORD;
        if (offsetComparison && !fineTiming) throw new IllegalArgumentException("Offset comparison requires fine timing");
        if (fineTiming && !scaling) throw new IllegalArgumentException("Fine timing requires the bounded scaling pilot");
        if (scaling && load && !offsetComparison) throw new IllegalArgumentException("Relay scaling load requires the explicit offset comparison");
        relayInstances = scaling ? CampaignRelayGroup.validate(Integer.parseInt(System.getProperty("campaign.delivery.relayInstances", "1"))) : 0;
        int targets = Integer.getInteger("campaign.delivery.targets", 10_000);
        ApiFailureObservationPlan observation = failureWindow ? new ApiFailureObservationPlan(
                ApiFailureObservationPlan.Profile.valueOf(System.getProperty("campaign.delivery.failureProfile", "PILOT")),
                ApiFailurePilotSupport.Mode.valueOf(System.getProperty("campaign.delivery.failureMode", "NONE"))) : null;
        if (observation != null) observation.validate(load, targets, fineTiming, offsetComparison, scaling,
                apiTiming || dbObservation || connectionPilot);
        var bounds = offsetComparison ? CampaignDeliveryBounds.forOffsetRun(load, targets) : CampaignDeliveryBounds.forRun(load, targets);
        if (offsetComparison && relayInstances != 2) throw new IllegalArgumentException("Offset comparison requires exactly two campaign relays");
        maxProbes = bounds.maxRealtimeProbes();
        int drainLimitSeconds = bounds.drainLimitSeconds();
        var originalZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        report.put("startedAtUtc", Instant.now().toString());
        report.put("scope", "preloaded campaign HTTP delivery " + (load ? targets + " target load" : "pilot") + "; same JVM independent app contexts");
        report.put("targets", targets);
        if (observation != null) {
            report.put("failureWindowMode", observation.mode().name());
            report.put("failureObservationProfile", observation.profile().name());
        }
        report.put("bounds", Map.of("maxRealtimeProbes", maxProbes, "drainLimitSeconds", drainLimitSeconds,
                "queryPageRows", QUERY_PAGE, "campaignAuditBytes", bounds.campaignAuditBytes(),
                "latencyArrayBytes", bounds.latencyArrayBytes(), "maxResourceSamples", bounds.maxResourceSamples()));
        report.put("verificationVersion", "paged-latency-and-target-mapping-v2");
        report.put("diagnosticsVersion", "live-pools-and-cgroup-counters-v1");
        report.put("redisConnectionsPerLane", 3);
        report.put("settings", Map.of("totalRate", 1000, "totalBurst", 100, "campaignRate", 200,
                "campaignBurst", 20, "workersPerLane", 3, "httpConnectionsPerLane", 3,
                "probeDelayMillis", 200, "baselineProbes", BASELINE_PROBES, "mockDelayMillis", 1));
        report.put("publishBatchSize", Integer.getInteger("campaign.delivery.publishBatchSize", 1));
        report.put("campaignRelayInstances", relayInstances == 0 ? 1 : relayInstances);
        report.put("relayScheduling", scaling ? "test-only-fixed-delay-group-v1" : "application-scheduled-bean");
        report.put("fineTimingEnabled", fineTiming);
        if (apiTiming) report.put("apiTimingEnabled", true);
        if (connectionPilot) report.put("connectionCallsEnabled", Boolean.getBoolean("campaign.delivery.connectionCalls"));
        if (dbObservation) report.put("dbObservationEnabled", Boolean.parseBoolean(System.getProperty("campaign.delivery.dbObserveEnabled", "true")));
        report.put("offsetComparison", offsetComparison);
        report.put("campaignOffsetMode", offsetMode.name());
        if (offsetComparison) report.put("offsetSettings", Map.of("ackCount", OffsetCommitPolicy.ACK_COUNT,
                "ackTimeMillis", OffsetCommitPolicy.ACK_TIME_MS, "pollTimeoutMillis", 500, "maxPollRecords", 1,
                "syncCommits", true, "realtimeMode", "RECORD"));
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"));
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        report.put("infra", Map.of("mysql", "8.4.8", "mysqlCpu", 2, "mysqlMemoryMiB", 2048,
                "mysqlBufferPoolMiB", 512, "kafka", "4.0.2", "kafkaCpu", 1, "kafkaMemoryMiB", 1024,
                "redis", "7.4.11", "redisMemoryMiB", 256));
        Throwable primaryFailure = null;
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
            if (observation != null) failureSupport = ApiFailurePilotSupport.create(maxProbes, observation.mode());
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
            var audit = new Audit(campaignId, targets, maxProbes);
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
                    try {
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
                        report.put("baselineLatencyMs", latency(jdbc, "title='BASELINE' AND traffic_class='REALTIME'", BASELINE_PROBES));
                        long drainStart = System.nanoTime(); // Includes campaign app startup; excludes preload and baseline.
                        if (apiRequestTiming != null) apiRequestTiming.markDrainStart(drainStart);
                        var samples = new ArrayList<Map<String, Object>>();
                        report.put("samples", samples);
                        try (var campaign = application(mysql, kafka, redis, "CAMPAIGN", true);
                             var relays = scaling ? CampaignRelayGroup.start(campaign, relayInstances) : null) {
                            try {
                                ready(campaign);
                                long nextSample = 0;
                                while (completed(campaign) < targets) {
                                    if (seconds(drainStart) > drainLimitSeconds) {
                                        var exceeded = new IllegalStateException("Delivery drain exceeded " + drainLimitSeconds + " seconds");
                                        captureFailure(exceeded, ApiFailureWindow.Stop.RUN_LIMIT, realtime); throw exceeded;
                                    }
                                    checkRunning(campaign, "campaign");
                                    checkRunning(realtime, "realtime");
                                    if (relays != null) relays.checkHealthy();
                                    probe(http, realtime, "MIXED");
                                    if (System.nanoTime() >= nextSample) {
                                        if (samples.size() >= bounds.maxResourceSamples()) throw new IllegalStateException("Resource sample bound exceeded");
                                        samples.add(sample(mysql, campaign, realtime, drainStart));
                                        nextSample = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                                        System.out.printf("DELIVERY_PILOT targets=%d completed=%.0f realtimeProbes=%d elapsedSeconds=%.3f%n",
                                                targets, completed(campaign), probes.size() - BASELINE_PROBES, seconds(drainStart));
                                    }
                                    Thread.sleep(200);
                                }
                                double drainSeconds = seconds(drainStart);
                                report.put("campaignDrainSeconds", drainSeconds);
                                report.put("campaignPerSecond", targets / drainSeconds);
                                if (samples.size() >= bounds.maxResourceSamples()) throw new IllegalStateException("Resource sample bound exceeded");
                                samples.add(sample(mysql, campaign, realtime, drainStart));
                                int mixed = probes.size() - BASELINE_PROBES;
                                assertThat(mixed).as("overlapping realtime observations").isGreaterThanOrEqualTo(10);
                                await().atMost(Duration.ofSeconds(30)).until(() -> accepted(jdbc, "REALTIME") == probes.size());
                                long offsetWaitStart = System.nanoTime();
                                awaitOffsets(admin, campaign, targets);
                                if (offsetComparison) {
                                    report.put("campaignOffsetWaitSeconds", seconds(offsetWaitStart));
                                    // Includes the final resource sample/realtime completion wait after campaign completion.
                                    report.put("campaignCompletionToOffsetsSeconds", seconds(drainStart) - drainSeconds);
                                }
                                awaitOffsets(admin, realtime, probes.size());
                                if (relays != null) {
                                    relays.checkHealthy();
                                    relays.close(); // Complete partial ACK flushes before the final audit and app teardown.
                                    report.put("campaignRelayGroup", relays.snapshot());
                                    assertThat(relays.snapshot()).containsEntry("maxConcurrentRuns", relayInstances)
                                            .containsEntry("activeRuns", 0).containsEntry("queuedPeriodicTasks", 0).containsEntry("terminated", true);
                                }
                                report.put("mixedProbes", mixed);
                                long verificationStart = System.nanoTime();
                                report.put("mixedLatencyMs", latency(jdbc, "title='MIXED' AND traffic_class='REALTIME'", mixed));
                                report.put("campaignCreatedToCompletedMs", latency(jdbc, "traffic_class='CAMPAIGN'", targets));
                                verifyTargetMappings(jdbc, campaignId, targets);
                                reconcile(jdbc, targets, audit, mock.getBean(MockPushEndpoint.class));
                                report.put("verificationSeconds", seconds(verificationStart));
                                report.put("campaignOffsets", offsets(admin, campaign));
                                report.put("realtimeOffsets", offsets(admin, realtime));
                                report.put("mockStats", mock.getBean(MockPushEndpoint.class).stats());
                                report.put("campaignAllowed", counter(campaign, "notification.rate.limit.decisions", "outcome", "allowed"));
                                report.put("realtimeAllowed", counter(realtime, "notification.rate.limit.decisions", "outcome", "allowed"));
                                report.put("stageTimingVersion", "caller-side-transaction-and-ack-v1");
                                report.put("campaignStageTimings", stageTimings(campaign));
                                report.put("realtimeStageTimings", stageTimings(realtime));
                                assertThat(counter(campaign, "notification.rate.limit.decisions", "outcome", "allowed")).isEqualTo(targets);
                                assertThat(counter(realtime, "notification.rate.limit.decisions", "outcome", "allowed")).isEqualTo(probes.size());
                                report.put("result", "PASS");
                            } catch (Exception | AssertionError failure) {
                                captureFailure(failure, ApiFailureWindow.Stop.INFRA_FAILURE, realtime);
                                try {
                                    report.put("failureSnapshot", Map.of("observedAtUtc", Instant.now().toString(),
                                            "elapsedSeconds", seconds(drainStart), "campaign", CampaignDeliveryDiagnostics.application(campaign),
                                            "realtime", CampaignDeliveryDiagnostics.application(realtime),
                                            "httpAudit", audit.summary(), "mockStats", mock.getBean(MockPushEndpoint.class).stats(),
                                            "acceptedProbeResponses", probes.size()));
                                } catch (RuntimeException diagnosticFailure) {
                                    report.put("failureSnapshotErrorType", diagnosticFailure.getClass().getSimpleName());
                                }
                                throw failure;
                            }
                        }
                    } catch (Exception | AssertionError failure) {
                        captureFailure(failure, ApiFailureWindow.Stop.INFRA_FAILURE, realtime); throw failure;
                    } finally { if (failureSupport != null) failureSupport.release(); }
                }
            }
            if (fineTiming) {
                var snapshots = new LinkedHashMap<String, Object>();
                timings.forEach((lane, timing) -> {
                    var snapshot = timing.snapshot();
                    assertThat(snapshot).containsEntry("pools", 1).containsEntry("poolsClosed", true)
                            .containsEntry("activeCalls", 0).containsEntry("wrappedConsumers", 3);
                    verifyTiming(timing, lane.equals("CAMPAIGN") ? targets : probes.size(),
                            lane.equals("CAMPAIGN") ? offsetMode : OffsetCommitPolicy.Mode.RECORD);
                    snapshots.put(lane, snapshot);
                });
                assertThat(snapshots).hasSize(2);
                report.put("fineTimings", snapshots);
            }
        } catch (Exception | AssertionError failure) {
            primaryFailure = failure;
            captureFailure(failure, ApiFailureWindow.Stop.INFRA_FAILURE, null);
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            PilotFinalization.run(primaryFailure, report, () -> {
                TimeZone.setDefault(originalZone);
                if (apiDbObservation != null) {
                    apiDbObservation.close();
                    report.put("apiDbObservation", apiDbObservation.snapshot());
                }
                report.put("finishedAtUtc", Instant.now().toString());
                if (apiRequestTiming != null) report.put("apiRequestTiming", apiRequestTiming.snapshot());
                if (failureSupport != null) {
                    failureSupport.release(); report.put("apiFailureWindow", failureSupport.window.snapshot());
                    report.put("failureWindowControl", failureSupport.control());
                }
                Path directory = failureSupport != null ? failureSupport.directory() : Path.of(connectionPilot ? "build/reports/api-connection-pilot" : dbObservation ? "build/reports/api-db-observation-pilot" : apiTiming ? "build/reports/api-admission-pilot" : offsetComparison ? (load ? "build/reports/offset-commit-load" : "build/reports/offset-commit-pilot") : fineTiming ? "build/reports/delivery-timing-pilot" : scaling ? "build/reports/relay-scaling-pilot" :
                        load ? "build/reports/campaign-delivery-load" : "build/reports/campaign-delivery-pilot");
                Files.createDirectories(directory);
                String name = failureSupport != null ? "result" : connectionPilot ? (Boolean.getBoolean("campaign.delivery.connectionCalls") ? "ON" : "OFF") + "-targets-" + targets : dbObservation ? (Boolean.parseBoolean(System.getProperty("campaign.delivery.dbObserveEnabled", "true")) ? "ON" : "OFF") + "-min-" + Long.getLong("campaign.delivery.dbMinCallMillis", 0) + "-targets-" + targets
                        : offsetComparison ? offsetMode.name() + "-targets-" + targets : scaling ? "relays-" + relayInstances + "-targets-" + targets : (load ? "load-" : "pilot-") + targets;
                mapper.writerWithDefaultPrettyPrinter().writeValue(directory.resolve(name + ".json").toFile(), report);
            });
        }
    }

    @SuppressWarnings("unchecked")
    private void verifyTiming(DeliveryTiming timing, long expectedJobs, OffsetCommitPolicy.Mode mode) {
        var phases = (Map<String, Map<String, Map<String, Object>>>) timing.snapshot().get("phases");
        for (String phase : List.of("WORKER_INITIAL", "WORKER_ADMITTED", "WORKER_COMPLETE")) {
            long calls = ((Number) phases.get(phase).get("STORE_CALL").get("count")).longValue();
            // A quota pause can retry its read-only initial check without another provider attempt.
            if (mode == OffsetCommitPolicy.Mode.COUNT_TIME && phase.equals("WORKER_INITIAL")) assertThat(calls).isGreaterThanOrEqualTo(expectedJobs);
            else assertThat(calls).as(phase + "/STORE_CALL").isEqualTo(expectedJobs);
            for (String part : List.of("ACQUIRE", "COMMIT")) assertThat(phases.get(phase).get(part).get("count")).as(phase + "/" + part).isEqualTo(calls);
            assertThat(phases.get(phase).get("ROLLBACK").get("count")).isEqualTo(0L);
        }
        assertThat(phases.get("OUTBOX_PUBLISHED").get("EXECUTE").get("count")).isEqualTo(expectedJobs);
        long commits = ((Number) phases.get("CONSUMER").get("COMMIT_SYNC").get("count")).longValue();
        if (mode == OffsetCommitPolicy.Mode.RECORD) assertThat(commits).isGreaterThanOrEqualTo(expectedJobs);
        else assertThat(commits).as("COUNT_TIME actually reduces synchronous calls").isPositive().isLessThan(expectedJobs);
        assertThat(phases.get("CONSUMER").get("COMMIT_ASYNC_SUBMIT").get("count")).isEqualTo(0L);
    }

    private Map<String, Object> stageTimings(ConfigurableApplicationContext app) {
        var result = new LinkedHashMap<String, Object>();
        var registry = app.getBean(MeterRegistry.class);
        for (String stage : List.of("relay.claim", "relay.ack", "relay.mark.published", "relay.run",
                "worker.handler", "worker.claim.initial", "worker.claim.admitted", "worker.provider", "worker.complete")) {
            var timer = registry.find("notification.pipeline.stage").tag("stage", stage).timer();
            assertThat(timer).as(stage).isNotNull();
            assertThat(timer.count()).as(stage + " observations").isPositive();
            double total = timer.totalTime(TimeUnit.MILLISECONDS);
            assertThat(Double.isFinite(total) && total >= 0).as(stage + " total").isTrue();
            result.put(stage, Map.of("count", timer.count(), "totalMillis", total, "meanMillis", total / timer.count()));
        }
        return result;
    }

    private ConfigurableApplicationContext application(MySQLContainer<?> mysql, KafkaContainer kafka,
                                                       GenericContainer<?> redis, String lane, boolean live) {
        var builder = new SpringApplicationBuilder(NotificationLabApplication.class)
                .web(live ? WebApplicationType.SERVLET : WebApplicationType.NONE);
        ApiRequestTiming api = live && lane.equals("REALTIME") && Boolean.getBoolean("campaign.delivery.apiTiming")
                ? new ApiRequestTiming(maxProbes, Boolean.getBoolean("campaign.delivery.connectionCalls")) : null;
        if (api != null) {
            apiRequestTiming = api;
            builder.sources(ApiRequestTiming.Wiring.class)
                    .initializers(context -> context.getBeanFactory().registerSingleton("apiRequestTiming", api));
        }
        var failure = live && lane.equals("REALTIME") ? failureSupport : null;
        if (failure != null) {
            builder.sources(ApiFailurePilotSupport.Wiring.class)
                    .initializers(context -> context.getBeanFactory().registerSingleton("apiFailureSupport", failure));
        }
        if (live && (Boolean.getBoolean("campaign.delivery.fineTiming") || failure != null)) {
            DeliveryTiming.ApiObserver observer = failure != null ? failure.window : api == null ? null : api.observer();
            if (api != null && Boolean.getBoolean("campaign.delivery.dbObservation")) {
                try {
                    apiDbObservation = ApiDbObservation.connect(DriverManager.getConnection(mysql.getJdbcUrl(), "root", mysql.getPassword()),
                            api.observer(), api::currentWindow, Boolean.parseBoolean(System.getProperty("campaign.delivery.dbObserveEnabled", "true")),
                            Long.getLong("campaign.delivery.dbMinCallMillis", 0));
                    observer = apiDbObservation;
                } catch (Exception setupFailure) { throw new IllegalStateException("DB observation setup failed", setupFailure); }
            }
            var timing = failure == null ? new DeliveryTiming(observer) : new DeliveryTiming(observer, failure::dataSource);
            timings.put(lane, timing);
            builder.sources(DeliveryTiming.Wiring.class)
                    .initializers(context -> context.getBeanFactory().registerSingleton("deliveryTiming", timing));
        }
        if (live && Boolean.getBoolean("campaign.delivery.offsetComparison")) {
            builder.sources(OffsetCommitPolicy.Wiring.class);
        }
        return builder.run(
                        "--server.port=0", "--app.provider.mode=HTTP", "--app.provider.http.base-url=" + (mockUrl == null ? "http://127.0.0.1:18081" : mockUrl),
                        "--app.provider.http.max-connections=3", "--app.worker.consumers=3", "--app.worker.max-active=3",
                        "--app.worker.queue-capacity=0", "--app.campaign.batch.enabled=" + !live,
                        "--app.async.relay.publish-batch-size=" + (lane.equals("CAMPAIGN") ? Integer.getInteger("campaign.delivery.publishBatchSize", 1) : 1),
                        "--app.campaign.batch.chunk-size=100", "--app.async.relay-enabled=" + (live && !(lane.equals("CAMPAIGN") && relayInstances > 0)), "--app.async.worker-enabled=" + live,
                        "--app.traffic.split-enabled=true", "--app.traffic.lane=" + lane,
                        "--app.traffic.campaign-rate=200", "--app.traffic.campaign-burst=20",
                        "--app.rate-limit.client-enabled=true", "--app.rate-limit.pause-enabled=true", "--app.rate-limit.max-connections=3",
                        "--app.rate-limit.rate-per-second=1000", "--app.rate-limit.burst=100",
                        "--app.rate-limit.port=" + redis.getMappedPort(6379), "--app.rate-limit.key=" + quotaKey,
                        "--spring.datasource.url=" + mysql.getJdbcUrl(), "--spring.datasource.username=" + mysql.getUsername(),
                        "--spring.datasource.password=" + mysql.getPassword(), "--spring.datasource.hikari.maximum-pool-size=4",
                        "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "--logging.level.root=WARN", "--spring.main.banner-mode=off",
                        "--notification.test.offset.mode=" + (lane.equals("CAMPAIGN") && Boolean.getBoolean("campaign.delivery.offsetComparison")
                                ? System.getProperty("campaign.delivery.offsetMode", "RECORD") : "RECORD"));
    }

    private void probe(HttpClient http, ConfigurableApplicationContext app, String phase) throws Exception {
        int ordinal = probes.size() + 1;
        if (ordinal > maxProbes) {
            var exceeded = new IllegalStateException("Probe bound exceeded");
            captureFailure(exceeded, ApiFailureWindow.Stop.PROBE_LIMIT, app); throw exceeded;
        }
        String body = mapper.writeValueAsString(Map.of("recipientId", "pilot-realtime-" + ordinal, "title", phase, "body", "synthetic body"));
        long start = System.nanoTime();
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + ((ServletWebServerApplicationContext) app).getWebServer().getPort() + "/notifications"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
        var capture = apiRequestTiming == null ? null : apiRequestTiming.open(ApiRequestTiming.Window.valueOf(phase));
        try {
            if (failureSupport != null) failureSupport.open(phase);
            var response = CampaignDeliveryDiagnostics.sendProbe(http, request, report, phase, ordinal, probes.size());
            double clientMillis = seconds(start) * 1000;
            if (failureSupport != null) failureSupport.window.complete(response.statusCode(), clientMillis);
            assertThat(response.statusCode()).isEqualTo(202);
            if (capture != null) apiRequestTiming.complete(capture, clientMillis, response.statusCode());
            probes.add(new Probe(mapper.readTree(response.body()).path("notificationId").asText(), clientMillis));
        } catch (Exception | AssertionError failure) {
            captureFailure(failure, ApiFailurePilotSupport.classify(failure), app); throw failure;
        } finally {
            if (capture != null) apiRequestTiming.abort(capture);
            if (failureSupport != null) failureSupport.release();
        }
    }

    private void captureFailure(Throwable failure, ApiFailureWindow.Stop reason, ConfigurableApplicationContext realtime) {
        if (failureSupport != null && failureSupport.capture(failure, reason, report)) {
            report.put("failureCaptureContext", realtime == null ? Map.of("available", false)
                    : Map.of("available", true, "realtimeActive", realtime.isActive()));
        }
    }

    Map<String, Object> failureReport() { return Map.copyOf(report); }
    Path failureDirectory() {
        if (failureSupport == null) throw new IllegalStateException("Failure window not enabled");
        return failureSupport.directory();
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
        report.put("httpAudit", audit.summary());
        report.put("baselineApiLatencyMs", percentiles(probes.subList(0, BASELINE_PROBES).stream().map(Probe::apiMillis).toList()));
        report.put("mixedApiLatencyMs", percentiles(probes.subList(BASELINE_PROBES, probes.size()).stream().map(Probe::apiMillis).toList()));
        if (apiRequestTiming != null) {
            assertThat(apiRequestTiming.snapshot()).containsEntry("completed", probes.size()).containsEntry("pending", false);
            if (apiDbObservation != null) {
                assertThat(apiDbObservation.snapshot()).containsEntry("errors", 0).containsEntry("openRequests", 0);
                assertThat((List<?>) apiDbObservation.snapshot().get("rows")).hasSize(probes.size());
            }
        }
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
            result.put(entry.getKey() + "State", CampaignDeliveryDiagnostics.application(entry.getValue()));
        }
        var memory = mysql.execInContainer("cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.events", "/sys/fs/cgroup/cpu.stat");
        assertThat(memory.getExitCode()).isZero();
        String[] parts = memory.getStdout().strip().split("\\R", 2);
        result.put("mysqlCgroupMemoryBytes", Long.parseLong(parts[0]));
        result.put("mysqlCgroupCounters", CampaignDeliveryDiagnostics.cgroupCounters(parts[1]));
        return result;
    }

    private Map<String, Object> latency(JdbcTemplate jdbc, String condition, int expected) {
        // Fixed-size primitive storage and at most 1,000 JDBC rows; no full result object graph.
        double[] values = new double[expected];
        int count = 0;
        String after = "";
        while (true) {
            var rows = jdbc.queryForList("""
                    SELECT notification_id, TIMESTAMPDIFF(MICROSECOND,created_at,completed_at)/1000.0 AS latency_ms
                    FROM notification FORCE INDEX (PRIMARY) WHERE
                    """ + condition + " AND notification_id>? ORDER BY notification_id LIMIT " + QUERY_PAGE, after);
            if (rows.isEmpty()) break;
            for (var row : rows) {
                assertThat(count).as("latency row bound").isLessThan(expected);
                assertThat(row.get("latency_ms")).isNotNull();
                double value = ((Number) row.get("latency_ms")).doubleValue();
                assertThat(Double.isFinite(value) && value >= 0).isTrue();
                values[count++] = value;
                after = (String) row.get("notification_id");
            }
        }
        assertThat(count).isEqualTo(expected).isPositive();
        Arrays.sort(values);
        return Map.of("count", count, "p50", values[(int) Math.ceil(count * .50) - 1],
                "p95", values[(int) Math.ceil(count * .95) - 1], "p99", values[(int) Math.ceil(count * .99) - 1], "max", values[count - 1]);
    }

    private void verifyTargetMappings(JdbcTemplate jdbc, UUID campaignId, int targets) {
        // Materialized keyset page, then bounded PK lookups, as in the DB handoff verifier.
        String query = """
                SELECT /*+ NO_BNL(n, t, o, a) */ c.target_ordinal, c.notification_id, c.task_id, c.event_id,
                       n.recipient_id, n.status, n.traffic_class, t.notification_id AS task_notification,
                       t.target_id, t.attempt_count, o.notification_id AS event_notification,
                       o.payload->>'$.taskId' AS payload_task, o.status AS event_status,
                       a.outcome, a.completed_at
                FROM (SELECT target_ordinal, notification_id, task_id, event_id FROM campaign_target
                      WHERE campaign_id=? AND target_ordinal>? ORDER BY target_ordinal LIMIT 1000) c
                STRAIGHT_JOIN notification n FORCE INDEX (PRIMARY) ON n.notification_id=c.notification_id
                STRAIGHT_JOIN delivery_task t FORCE INDEX (PRIMARY) ON t.task_id=c.task_id
                STRAIGHT_JOIN outbox_event o FORCE INDEX (PRIMARY) ON o.event_id=c.event_id
                STRAIGHT_JOIN delivery_attempt a FORCE INDEX (PRIMARY) ON a.task_id=c.task_id AND a.attempt_no=1
                ORDER BY c.target_ordinal
                """;
        var plan = jdbc.queryForList("EXPLAIN " + query, campaignId.toString(), 0);
        for (String table : List.of("n", "t", "o", "a")) {
            var lookup = plan.stream().filter(row -> table.equals(row.get("table"))).findFirst().orElseThrow();
            assertThat(lookup.get("type")).as(table + " lookup").isEqualTo("eq_ref");
            assertThat(lookup.get("key")).isEqualTo("PRIMARY");
        }
        for (int after = 0; after < targets;) {
            var rows = jdbc.queryForList(query, campaignId.toString(), after);
            assertThat(rows).hasSize(Math.min(QUERY_PAGE, targets - after));
            for (var row : rows) {
                int ordinal = ((Number) row.get("target_ordinal")).intValue();
                assertThat(ordinal).isEqualTo(++after);
                var ids = CampaignTargetIds.of(campaignId, ordinal);
                assertThat(row.get("notification_id")).isEqualTo(ids.notificationId().toString());
                assertThat(row.get("task_id")).isEqualTo(ids.taskId().toString());
                assertThat(row.get("event_id")).isEqualTo(ids.eventId().toString());
                assertThat(row.get("recipient_id")).isEqualTo("synthetic-v1-" + ordinal);
                assertThat((byte[]) row.get("target_id")).isEqualTo(("synthetic-v1-" + ordinal).getBytes(StandardCharsets.UTF_8));
                assertThat(row.get("task_notification")).isEqualTo(row.get("notification_id"));
                assertThat(row.get("event_notification")).isEqualTo(row.get("notification_id"));
                assertThat(row.get("payload_task")).isEqualTo(row.get("task_id"));
                assertThat(row.get("traffic_class")).isEqualTo("CAMPAIGN");
                assertThat(row.get("status")).isEqualTo("PROVIDER_ACCEPTED");
                assertThat(row.get("event_status")).isEqualTo("PUBLISHED");
                assertThat(((Number) row.get("attempt_count")).intValue()).isEqualTo(1);
                assertThat(row.get("outcome")).isEqualTo("PROVIDER_ACCEPTED");
                assertThat(row.get("completed_at")).isNotNull();
            }
        }
        report.put("verifiedTargetMappings", targets);
        report.put("mappingJoinPlan", "eq_ref/PRIMARY for n,t,o,a");
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
        if (Boolean.getBoolean("campaign.delivery.fineTiming")) {
            var properties = app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker").getContainerProperties();
            assertThat(properties.isSyncCommits()).isTrue();
            var selected = OffsetCommitPolicy.mode(app.getEnvironment().getProperty("notification.test.offset.mode", "RECORD"));
            assertThat(properties.getAckMode().name()).isEqualTo(selected.name());
            assertThat(properties.getPollTimeout()).isEqualTo(500);
            if (Boolean.getBoolean("campaign.delivery.offsetComparison")) {
                OffsetCommitPolicy.requireApplied(app);
                assertThat(properties.getAckCount()).isEqualTo(OffsetCommitPolicy.ACK_COUNT);
                assertThat(properties.getAckTime()).isEqualTo(OffsetCommitPolicy.ACK_TIME_MS);
            }
        }
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
        private final int maxProbes;
        private final BitSet campaignSeen;
        private final BitSet realtimeSeen;
        private final String[] realtimeIds;
        private int invalid;
        private int duplicates;
        Audit(UUID campaignId, int targets, int maxProbes) {
            this.campaignId = campaignId; this.targets = targets; this.maxProbes = maxProbes;
            campaignSeen = new BitSet(targets + 1);
            realtimeSeen = new BitSet(maxProbes + 1);
            realtimeIds = new String[maxProbes + 1];
        }
        synchronized int count() { return campaignSeen.cardinality() + realtimeSeen.cardinality() + duplicates + invalid; }
        synchronized Map<String, Integer> summary() {
            return Map.of("uniqueCampaign", campaignSeen.cardinality(), "uniqueRealtime", realtimeSeen.cardinality(),
                    "duplicates", duplicates, "invalid", invalid);
        }
        synchronized void observe(byte[] body, int status) {
            try {
                var value = mapper.readTree(body);
                String recipient = value.path("recipientId").asText();
                boolean campaign = recipient.startsWith("synthetic-v1-");
                String prefix = campaign ? "synthetic-v1-" : "pilot-realtime-";
                if (!recipient.startsWith(prefix) || status != 202) { invalid++; return; }
                int ordinal = Integer.parseInt(recipient.substring(prefix.length()));
                if (ordinal < 1 || ordinal > (campaign ? targets : maxProbes)) { invalid++; return; }
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
