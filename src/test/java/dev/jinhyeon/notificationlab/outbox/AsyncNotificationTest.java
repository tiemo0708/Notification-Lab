package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import dev.jinhyeon.notificationlab.NotificationLabApplication;
import dev.jinhyeon.notificationlab.config.AsyncProperties;
import dev.jinhyeon.notificationlab.config.TrafficProperties;
import dev.jinhyeon.notificationlab.notification.JdbcNotificationStore;
import dev.jinhyeon.notificationlab.notification.NotificationStatus;
import dev.jinhyeon.notificationlab.provider.PushProvider;
import dev.jinhyeon.notificationlab.provider.MockPushProvider;
import dev.jinhyeon.notificationlab.provider.PushRequest;
import dev.jinhyeon.notificationlab.provider.PushResult;
import dev.jinhyeon.notificationlab.worker.WorkerStore;
import dev.jinhyeon.notificationlab.worker.DeliveryTaskStore;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import dev.jinhyeon.notificationlab.notification.NotificationRequest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.jdbc.core.RowMapper;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real infrastructure; no skipped Docker tests and no mock Kafka broker. */
@Testcontainers
class AsyncNotificationTest {
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

    private ServletWebServerApplicationContext app;
    private HttpClient http;
    private Admin admin;
    private String topic;
    private String group;
    private static final String PAYLOAD = """
            {"recipientId":"synthetic-user","title":"비동기 테스트","body":"가상 데이터"}
            """;

    @BeforeEach
    void start(TestInfo info) {
        topic = "notification-test-" + UUID.randomUUID();
        group = "worker-test-" + UUID.randomUUID();
        boolean experiment = info.getTags().contains("reconciliation");
        startApplication(experiment);
        jdbc().update("DELETE FROM dead_letter_redrive");
        jdbc().update("DELETE FROM delivery_attempt");
        jdbc().update("DELETE FROM delivery_task");
        jdbc().update("DELETE FROM outbox_event");
        jdbc().update("DELETE FROM notification");
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        admin = Admin.create(java.util.Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    private void startApplication(boolean experiment, String... overrides) {
        app = (ServletWebServerApplicationContext) new SpringApplicationBuilder(
                NotificationLabApplication.class, ProbeConfiguration.class).run(java.util.stream.Stream.concat(java.util.stream.Stream.of(
                "--server.port=0", "--spring.profiles.active=async", "--app.mock.delay-ms=0",
                "--spring.datasource.url=" + MYSQL.getJdbcUrl(),
                "--spring.datasource.username=" + MYSQL.getUsername(),
                "--spring.datasource.password=" + MYSQL.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--app.async.topic=" + topic, "--app.async.group=" + group,
                "--app.async.worker-enabled=" + experiment,
                "--app.delivery.retry-base-delay-ms=" + (experiment ? "1000" : "60000"),
                "--app.async.relay-initial-delay-ms=" + (experiment ? "0" : "86400000"),
                "--logging.level.org.apache.kafka=WARN", "--logging.level.org.springframework.kafka=WARN"),
                Arrays.stream(overrides)).toArray(String[]::new));
        assertThat(Arrays.stream(app.getBeanDefinitionNames())
                .map(name -> app.getBeanFactory().getBeanDefinition(name).getBeanClassName()).toList())
                .doesNotContain("dev.jinhyeon.notificationlab.config.AsyncPropertiesTest$RelayConfiguration");
    }

    @AfterEach
    void close() {
        if (http != null) http.close();
        if (app != null) app.close();
        if (admin != null) admin.close(Duration.ofSeconds(3));
    }

    @Test
    void publishBatchFlushesOnlyAcknowledgedRowsWhenTheNextSendFails() throws Exception {
        post(); post();
        var pending = new CompletableFuture<SendResult<String, String>>();
        var publishing = new CountDownLatch(1);
        var sends = new AtomicInteger();
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String destination, String key, String data) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                if (sends.incrementAndGet() == 1) return CompletableFuture.completedFuture(null);
                publishing.countDown();
                return pending;
            }
        };
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(() -> batchRelay(fake).relay());
            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED'", Integer.class)).isLessThanOrEqualTo(1);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PROCESSING' AND published_at IS NULL", Integer.class)).isPositive();
            pending.completeExceptionally(new IllegalStateException("test broker outage"));
            running.get(5, TimeUnit.SECONDS);
        }
        assertThat(jdbc().queryForList("SELECT status FROM outbox_event ORDER BY status", String.class)).containsExactly("PENDING", "PUBLISHED");
        assertThat(registry().get("outbox.published").counter().count()).isEqualTo(1);
        assertThat(registry().get("notification.pipeline.stage").tag("stage", "relay.ack").timer().count()).isEqualTo(2);
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void publishBatchFencesEveryRowAgainstAReclaimedLease() throws Exception {
        post(); post();
        var store = app.getBean(OutboxStore.class);
        var first = store.claim(10).orElseThrow();
        var second = store.claim(10).orElseThrow();
        jdbc().update("UPDATE outbox_event SET available_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6)) WHERE event_id=?", first.eventId());
        var reclaimed = store.claim(10).orElseThrow();
        assertThat(reclaimed.eventId()).isEqualTo(first.eventId());
        assertThat(reclaimed.token()).isNotEqualTo(first.token());
        assertThat(store.published(List.of(first, second))).isEqualTo(1);
        assertThat(jdbc().queryForObject("SELECT status FROM outbox_event WHERE event_id=?", String.class, first.eventId())).isEqualTo("PROCESSING");
        assertThat(store.published(List.of(reclaimed))).isEqualTo(1);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED'", Integer.class)).isEqualTo(2);
    }

    @Test
    void failedPublishBatchRollsBackAndReplaysAfterLeaseExpiry() throws Exception {
        post(); post();
        var store = app.getBean(OutboxStore.class);
        var claims = List.of(store.claim(10).orElseThrow(), store.claim(10).orElseThrow());
        for (var claim : claims) kafka().send(topic, claim.notificationId(), claim.payload()).get(10, TimeUnit.SECONDS);
        root("CREATE TRIGGER reject_publish_batch BEFORE UPDATE ON outbox_event FOR EACH ROW "
                + "BEGIN IF NEW.status = 'PUBLISHED' AND NEW.event_id = '" + claims.getLast().eventId()
                + "' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test fault'; END IF; END");
        try {
            assertThatThrownBy(() -> store.published(claims)).isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PROCESSING' AND published_at IS NULL", Integer.class)).isEqualTo(2);
            assertThat(recordsOn(topic, 2)).hasSize(2);
        } finally {
            root("DROP TRIGGER reject_publish_batch");
        }
        expireOutbox();
        batchRelay(kafka()).relay();
        var copies = recordsOn(topic, 4);
        assertThat(copies).hasSize(4);
        assertThat(copies.stream().collect(java.util.stream.Collectors.groupingBy(r -> r.value(), java.util.stream.Collectors.counting())).values()).containsOnly(2L);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED' AND attempt_count=2", Integer.class)).isEqualTo(2);
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void stoppingPublishBatchFlushesAcknowledgedRowsAndLeavesTheRestDurable() throws Exception {
        post(); post(); post();
        var sends = new AtomicInteger();
        var relay = new AtomicReference<OutboxRelay>();
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String destination, String key, String data) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                if (sends.incrementAndGet() == 2) relay.get().stopClaiming();
                return CompletableFuture.completedFuture(null);
            }
        };
        relay.set(batchRelay(fake));
        relay.get().relay();
        assertThat(sends.get()).isEqualTo(2);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PUBLISHED'", Integer.class)).isEqualTo(2);
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PENDING' AND attempt_count=0", Integer.class)).isEqualTo(1);
    }

    private OutboxRelay batchRelay(KafkaTemplate<String, String> template) {
        return new OutboxRelay(app.getBean(OutboxStore.class), template, app.getBean(AsyncProperties.class), registry(),
                app.getBean(dev.jinhyeon.notificationlab.config.TrafficRouting.class), new dev.jinhyeon.notificationlab.config.RelayProperties(10));
    }

    @Test
    void submissionWindowBoundsOutstandingSendsAndRejectsOverlappingRuns() throws Exception {
        for (int i = 0; i < 11; i++) post();
        var submitted = new CountDownLatch(10);
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            if (futures.size() > 10) future.complete(null);
            return future;
        });
        var relay = windowRelay(fake, 10);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(relay::relay);
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
                relay.relay(); // A manual overlapping invocation must not create another window.
                assertThat(futures).hasSize(10);
                assertThat(outboxCount("PROCESSING", 1)).isEqualTo(10);
                assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
                assertThat(outboxCount("PUBLISHED", 1)).isZero();
            } finally {
                futures.forEach(f -> f.complete(null));
            }
            running.get(8, TimeUnit.SECONDS);
        }
        assertThat(futures).hasSize(11);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(11);
        assertThat(probe().attempts.get()).isZero(); // Synthetic ACKs, not actual broker delivery.
    }

    @Test
    void submissionWindowDrainsOutOfOrderAcksAfterPartialFailureWithoutRefilling() throws Exception {
        for (int i = 0; i < 4; i++) post();
        var submitted = new CountDownLatch(3);
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            return future;
        });
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(windowRelay(fake, 3)::relay);
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
                futures.get(2).complete(null);
                assertThat(outboxCount("PUBLISHED", 1)).isZero();
                futures.get(0).completeExceptionally(new IllegalStateException("synthetic broker failure"));
                futures.get(1).complete(null);
                running.get(8, TimeUnit.SECONDS);
            } finally {
                futures.forEach(f -> f.complete(null));
            }
        }
        assertThat(futures).hasSize(3);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(2);
        assertThat(outboxCount("PENDING", 1)).isEqualTo(1);
        assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
    }

    @Test
    void submissionWindowDrainsEarlierSendsWhenSendThrowsSynchronously() throws Exception {
        for (int i = 0; i < 4; i++) post();
        var submitted = new CountDownLatch(3);
        var sends = new AtomicInteger();
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            int number = sends.incrementAndGet();
            if (number == 3) {
                submitted.countDown();
                throw new IllegalStateException("synthetic send rejection");
            }
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            return future;
        });
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(windowRelay(fake, 3)::relay);
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
            } finally {
                futures.forEach(f -> f.complete(null));
            }
            running.get(8, TimeUnit.SECONDS);
        }
        assertThat(sends.get()).isEqualTo(3);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(2);
        assertThat(outboxCount("PENDING", 1)).isEqualTo(1);
        assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
    }

    @Test
    void submissionWindowUsesOneDeadlineAndLateAcksCannotPublishTimedOutLeases() throws Exception {
        for (int i = 0; i < 4; i++) post();
        var submitted = new CountDownLatch(3);
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            if (futures.size() > 3) future.complete(null);
            return future;
        });
        // Two missing ACKs share five seconds, rather than waiting five seconds for each.
        var relay = new OutboxRelay(app.getBean(OutboxStore.class), fake, app.getBean(AsyncProperties.class), registry(),
                app.getBean(dev.jinhyeon.notificationlab.config.TrafficRouting.class),
                new dev.jinhyeon.notificationlab.config.RelayProperties(10, 3), Duration.ofSeconds(5));
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(relay::relay);
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
                futures.get(2).complete(null);
                running.get(8, TimeUnit.SECONDS);
                assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(1);
                assertThat(outboxCount("PROCESSING", 1)).isEqualTo(2);
                assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
                assertThat(registry().get("outbox.publish.unconfirmed").counter().count()).isEqualTo(2);
                relay.relay();
                assertThat(futures).hasSize(3);
                assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
            } finally {
                futures.forEach(f -> f.complete(null));
            }
        }
        assertThat(outboxCount("PROCESSING", 1)).isEqualTo(2);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(1);
        assertThat(futures).allSatisfy(f -> assertThat(f.isCancelled()).isFalse());
        relay.relay(); // Completed late futures free capacity, but are never used as DB ACK callbacks.
        assertThat(futures).hasSize(4);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(2);
        assertThat(outboxCount("PROCESSING", 1)).isEqualTo(2);
    }

    @Test
    void stoppingSubmissionWindowDrainsSubmittedAcksAndLeavesUnclaimedRows() throws Exception {
        for (int i = 0; i < 5; i++) post();
        var submitted = new CountDownLatch(3);
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            return future;
        });
        var relay = windowRelay(fake, 3);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(relay::relay);
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
                relay.stopClaiming();
            } finally {
                futures.forEach(f -> f.complete(null));
            }
            running.get(8, TimeUnit.SECONDS);
        }
        relay.relay();
        assertThat(futures).hasSize(3);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(3);
        assertThat(outboxCount("PENDING", 0)).isEqualTo(2);
    }

    @Test
    void interruptedSubmissionWindowPreservesLeasesAndInterruptFlag() throws Exception {
        for (int i = 0; i < 4; i++) post();
        var submitted = new CountDownLatch(3);
        var thread = new AtomicReference<Thread>();
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            return future;
        });
        var relay = windowRelay(fake, 3);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(() -> {
                thread.set(Thread.currentThread());
                relay.relay();
                return Thread.currentThread().isInterrupted();
            });
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
                thread.get().interrupt();
                assertThat(running.get(8, TimeUnit.SECONDS)).isTrue();
                relay.relay();
                assertThat(futures).hasSize(3);
                assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
            } finally {
                futures.forEach(f -> f.complete(null));
            }
        }
        assertThat(outboxCount("PROCESSING", 1)).isEqualTo(3);
        assertThat(outboxCount("PUBLISHED", 1)).isZero();
        assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
    }

    @Test
    void submissionWindowFencesBothAckAndFailureAfterLeaseReclaim() throws Exception {
        for (int i = 0; i < 3; i++) post();
        var submitted = new CountDownLatch(3);
        var keys = new CopyOnWriteArrayList<String>();
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            keys.add(key);
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            return future;
        });
        var store = app.getBean(OutboxStore.class);
        var replacements = new java.util.ArrayList<OutboxStore.Claim>();
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(windowRelay(fake, 3)::relay);
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
                for (int i = 0; i < 2; i++) {
                    jdbc().update("UPDATE outbox_event SET available_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6)) WHERE notification_id=?", keys.get(i));
                    replacements.add(store.claim(10).orElseThrow());
                }
                futures.get(0).complete(null);
                futures.get(1).completeExceptionally(new IllegalStateException("synthetic late failure"));
                futures.get(2).complete(null);
                running.get(8, TimeUnit.SECONDS);
            } finally {
                futures.forEach(f -> f.complete(null));
            }
        }
        assertThat(outboxCount("PROCESSING", 2)).isEqualTo(2);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(1);
        for (var claim : replacements) {
            assertThat(jdbc().queryForObject("SELECT lease_token FROM outbox_event WHERE event_id=?", String.class, claim.eventId())).isEqualTo(claim.token());
        }
        assertThat(registry().get("outbox.published").counter().count()).isEqualTo(1);
    }

    @Test
    void submissionWindowRecoversRealBrokerDuplicatesAfterPublishedCommitFailure() throws Exception {
        for (int i = 0; i < 3; i++) post();
        String rejected = jdbc().queryForObject("SELECT event_id FROM outbox_event ORDER BY event_id LIMIT 1", String.class);
        root("CREATE TRIGGER reject_window_publish BEFORE UPDATE ON outbox_event FOR EACH ROW "
                + "BEGIN IF NEW.status = 'PUBLISHED' AND NEW.event_id = '" + rejected
                + "' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'synthetic commit failure'; END IF; END");
        try {
            windowRelay(kafka(), 3).relay();
            assertThat(recordsOn(topic, 3)).hasSize(3);
            assertThat(outboxCount("PROCESSING", 1)).isEqualTo(3);
            assertThat(outboxCount("PUBLISHED", 1)).isZero();
        } finally {
            root("DROP TRIGGER reject_window_publish");
        }
        expireOutbox();
        windowRelay(kafka(), 3).relay();
        var copies = recordsOn(topic, 6);
        assertThat(copies).hasSize(6);
        assertThat(copies.stream().collect(java.util.stream.Collectors.groupingBy(r -> r.value(), java.util.stream.Collectors.counting())).values()).containsOnly(2L);
        assertThat(outboxCount("PUBLISHED", 2)).isEqualTo(3);
        startWorker();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(probe().attempts.get()).isEqualTo(3);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status='PROVIDER_ACCEPTED'", Integer.class)).isEqualTo(3);
            assertThat(count("delivery_attempt")).isEqualTo(3);
        });
        awaitCommitted(6);
    }

    private int outboxCount(String state, int attempts) {
        return jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status=? AND attempt_count=?", Integer.class, state, attempts);
    }

    @Test
    void submissionWindowRetainsTheFiftyClaimRunLimitAndFlushesTheLastPartialWindow() throws Exception {
        for (int i = 0; i < 51; i++) post();
        var sends = new AtomicInteger();
        var fake = windowTemplate((destination, key, data) -> {
            sends.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        windowRelay(fake, 3).relay();
        assertThat(sends.get()).isEqualTo(50);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(50);
        assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
    }

    @Test
    void submissionWindowDrainsSubmittedAcksEvenWhenTheNextClaimFails() throws Exception {
        for (int i = 0; i < 3; i++) post();
        var delegate = app.getBean(OutboxStore.class);
        var claims = new AtomicInteger();
        var broken = new OutboxStore(null, null) {
            @Override
            public java.util.Optional<Claim> claim(int maxAttempts) {
                if (claims.incrementAndGet() == 3) throw new IllegalStateException("synthetic claim failure");
                return delegate.claim(maxAttempts);
            }

            @Override
            public int published(List<Claim> batch) { return delegate.published(batch); }
        };
        var submitted = new CountDownLatch(2);
        var futures = new CopyOnWriteArrayList<CompletableFuture<SendResult<String, String>>>();
        var fake = windowTemplate((destination, key, data) -> {
            var future = new CompletableFuture<SendResult<String, String>>();
            futures.add(future);
            submitted.countDown();
            return future;
        });
        var relay = new OutboxRelay(broken, fake, app.getBean(AsyncProperties.class), registry(),
                app.getBean(dev.jinhyeon.notificationlab.config.TrafficRouting.class),
                new dev.jinhyeon.notificationlab.config.RelayProperties(10, 3));
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(relay::relay);
            try {
                assertThat(submitted.await(8, TimeUnit.SECONDS)).isTrue();
            } finally {
                futures.forEach(f -> f.complete(null));
            }
            running.get(8, TimeUnit.SECONDS);
        }
        assertThat(claims.get()).isEqualTo(3);
        assertThat(outboxCount("PUBLISHED", 1)).isEqualTo(2);
        assertThat(outboxCount("PENDING", 0)).isEqualTo(1);
    }

    private OutboxRelay windowRelay(KafkaTemplate<String, String> template, int size) {
        return new OutboxRelay(app.getBean(OutboxStore.class), template, app.getBean(AsyncProperties.class), registry(),
                app.getBean(dev.jinhyeon.notificationlab.config.TrafficRouting.class), new dev.jinhyeon.notificationlab.config.RelayProperties(10, size));
    }

    @FunctionalInterface
    private interface WindowSend {
        CompletableFuture<SendResult<String, String>> send(String destination, String key, String data);
    }

    private KafkaTemplate<String, String> windowTemplate(WindowSend sender) {
        return new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String destination, String key, String data) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                return sender.send(destination, key, data);
            }
        };
    }

    @Test
    void acceptanceCommitsAllRowsBefore202WithoutCallingProvider() throws Exception {
        var response = post();
        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(json(response).path("status").asText()).isEqualTo("QUEUED");
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
        assertThat(probe().attempts.get()).isZero();
        assertThat(jdbc().queryForObject("SELECT payload FROM outbox_event", String.class))
                .contains("schemaVersion", "notificationId", "eventId")
                .doesNotContain("synthetic-user", "가상 데이터", "비동기 테스트");
        var get = http.send(HttpRequest.newBuilder(URI.create(base() + response.headers()
                .firstValue("Location").orElseThrow())).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(json(get)).isEqualTo(json(response));
    }

    @Test
    void outboxInsertFailureRollsBackNotificationAndReturns503() throws Exception {
        root("CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox_event FOR EACH ROW "
                + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sensitive-test-data'");
        try {
            var response = post();
            assertThat(response.statusCode()).isEqualTo(503);
            assertThat(response.body()).doesNotContain("sensitive-test-data", "synthetic-user");
            assertThat(count("notification")).isZero();
            assertThat(count("outbox_event")).isZero();
            assertThat(count("delivery_task")).isZero();
            assertThat(probe().attempts.get()).isZero();
        } finally {
            root("DROP TRIGGER reject_outbox");
        }
    }

    @Test
    void realtimeClaimLeavesCampaignCandidatesClaimableWithStaleStatistics() throws Exception {
        var lane = new TrafficProperties(TrafficProperties.Lane.CAMPAIGN, true, 2, 2);
        var tx = new TransactionTemplate(app.getBean(PlatformTransactionManager.class));
        var campaignSubmissions = new SubmissionService(app.getBean(JdbcNotificationStore.class),
                jdbc(), Clock.systemUTC(), app.getBean(DeliveryTaskStore.class), lane);
        root("ALTER TABLE outbox_event STATS_AUTO_RECALC=0");
        root("ANALYZE TABLE outbox_event");
        try {
            for (int i = 0; i < 32; i++) tx.executeWithoutResult(status -> campaignSubmissions.accept(
                    new NotificationRequest("synthetic-campaign", "campaign", "synthetic body"), null));
            jdbc().update("UPDATE outbox_event SET available_at=TIMESTAMPADD(SECOND,-5,UTC_TIMESTAMP(6))");
            String realtimeId = json(post()).path("notificationId").asText();
            var selected = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var observedIsolation = new AtomicReference<String>();
            var gated = new JdbcTemplate(app.getBean(DataSource.class)) {
                @Override
                public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
                    var rows = super.query(sql, mapper, args);
                    if (sql.contains("FOR UPDATE SKIP LOCKED") && "PENDING".equals(args[1])) {
                        observedIsolation.set(queryForObject("SELECT @@transaction_isolation", String.class));
                        selected.countDown();
                        try { assertThat(release.await(8, TimeUnit.SECONDS)).isTrue(); }
                        catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt(); throw new IllegalStateException("Claim gate interrupted");
                        }
                    }
                    return rows;
                }
            };
            var realtimeStore = transactionProxy(new OutboxStore(gated, app.getBean(TrafficProperties.class)));
            var campaignStore = transactionProxy(new OutboxStore(jdbc(), lane));
            try (var executor = Executors.newFixedThreadPool(2)) {
                var realtime = executor.submit(() -> realtimeStore.claim(10));
                try {
                    assertThat(selected.await(5, TimeUnit.SECONDS)).isTrue();
                    // Exercise the production annotation through a real Spring transaction interceptor.
                    var concurrent = executor.submit(() -> campaignStore.claim(10));
                    assertThat(concurrent.get(3, TimeUnit.SECONDS)).isPresent();
                } finally { release.countDown(); }
                assertThat(realtime.get(5, TimeUnit.SECONDS).orElseThrow().notificationId()).isEqualTo(realtimeId);
            }
            assertThat(observedIsolation.get()).isEqualTo("READ-COMMITTED");
            assertThat(jdbc().queryForObject("SELECT @@transaction_isolation", String.class)).isEqualTo("REPEATABLE-READ");
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status='PROCESSING' AND attempt_count=1 AND lease_token IS NOT NULL", Integer.class)).isEqualTo(2);
            assertThat(probe().attempts.get()).isZero();
        } finally {
            root("ALTER TABLE outbox_event STATS_AUTO_RECALC=DEFAULT");
        }
    }

    private OutboxStore transactionProxy(OutboxStore store) {
        var proxy = new ProxyFactory(store);
        proxy.setProxyTargetClass(true);
        var interceptor = new TransactionInterceptor();
        interceptor.setTransactionManager(app.getBean(PlatformTransactionManager.class));
        interceptor.setTransactionAttributeSource(new AnnotationTransactionAttributeSource());
        interceptor.afterPropertiesSet();
        proxy.addAdvice(interceptor);
        return (OutboxStore) proxy.getProxy();
    }

    @Test
    void concurrentClaimsAndExpiredLeasesFenceStalePublisher() throws Exception {
        post();
        var store = app.getBean(OutboxStore.class);
        Callable<java.util.Optional<OutboxStore.Claim>> claim = () -> store.claim(10);
        OutboxStore.Claim first;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var results = executor.invokeAll(List.of(claim, claim));
            var claims = results.stream().map(f -> {
                try { return f.get(); } catch (Exception e) { throw new RuntimeException(e); }
            }).flatMap(java.util.Optional::stream).toList();
            assertThat(claims).hasSize(1);
            first = claims.getFirst();
        }
        expireOutbox();
        var recovered = store.claim(10).orElseThrow();
        assertThat(recovered.attempt()).isEqualTo(2);
        assertThat(recovered.token()).isNotEqualTo(first.token());
        assertThat(store.published(first)).isFalse();
        assertThat(store.published(recovered)).isTrue();
    }

    @Test
    void publishedStateWaitsForFutureAndPublishDoesNotHoldDatabaseTransaction() throws Exception {
        post();
        CompletableFuture<SendResult<String, String>> ack = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String topic, String key, String data) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                calls.incrementAndGet();
                return ack;
            }
        };
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(() -> relay(fake).relay());
            await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() == 1);
            assertThat(outboxStatus()).isEqualTo("PROCESSING");
            ack.complete(null);
            running.get(5, TimeUnit.SECONDS);
            assertThat(outboxStatus()).isEqualTo("PUBLISHED");
        }
    }

    @Test
    void failedPublishBacksOffAndStopsAtAttemptLimitIncludingExpiredFinalLease() throws Exception {
        post();
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String topic, String key, String data) {
                return CompletableFuture.failedFuture(new IllegalStateException("test outage"));
            }
        };
        relay(fake).relay();
        assertThat(outboxStatus()).isEqualTo("PENDING");
        assertThat(jdbc().queryForObject("SELECT available_at > UTC_TIMESTAMP(6) FROM outbox_event", Boolean.class)).isTrue();
        assertThat(app.getBean(OutboxStore.class).claim(10)).isEmpty();
        jdbc().update("UPDATE outbox_event SET attempt_count = 9");
        expireOutbox();
        relay(fake).relay();
        assertThat(outboxStatus()).isEqualTo("FAILED");
        assertThat(jdbc().queryForObject("SELECT attempt_count FROM outbox_event", Integer.class)).isEqualTo(10);
        // Also recover a process that died during its last attempt.
        jdbc().update("UPDATE outbox_event SET status = 'PROCESSING', lease_token = ?", UUID.randomUUID().toString());
        expireOutbox();
        assertThat(app.getBean(OutboxStore.class).claim(10)).isEmpty();
        assertThat(outboxStatus()).isEqualTo("FAILED");
    }

    @Test
    void realKafkaCompletesBeforeCommittingOffsetAndReplayDoesNotCallProviderAgain() throws Exception {
        String id = json(post()).path("notificationId").asText();
        probe().behavior = request -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            assertThat(status(id)).isEqualTo("PROCESSING");
            return accepted(request);
        };
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED"));
        awaitCommitted(1);
        assertThat(probe().attempts.get()).isEqualTo(1);
        // Simulates a relay replay after ACK but before its durable PUBLISHED update.
        String payload = jdbc().queryForObject("SELECT payload FROM outbox_event", String.class);
        kafka().send(topic, id, payload).get(10, TimeUnit.SECONDS);
        awaitCommitted(2);
        assertThat(probe().attempts.get()).isEqualTo(1);
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(registry().get("outbox.unpublished").gauge().value()).isZero();
        assertThat(registry().get("notification.consumer.lag").gauge().value()).isZero();
    }

    @Test
    void completionFailureStopsConsumerWithoutSkippingOffsetAndCanRecoverExpiredLease() throws Exception {
        String id = json(post()).path("notificationId").asText();
        root("CREATE TRIGGER reject_completion BEFORE UPDATE ON notification FOR EACH ROW "
                + "BEGIN IF NEW.completed_at IS NOT NULL THEN SIGNAL SQLSTATE '45000' "
                + "SET MESSAGE_TEXT = 'sensitive-test-data'; END IF; END");
        try {
            startWorker();
            app.getBean(OutboxRelay.class).relay();
            await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
            assertThat(status(id)).isEqualTo("PROCESSING");
            assertThat(committedTotal()).isZero();
        } finally {
            root("DROP TRIGGER reject_completion");
        }
        jdbc().update("UPDATE notification SET worker_lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))");
        startWorker();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED"));
        awaitCommitted(1);
        assertThat(probe().attempts.get()).isEqualTo(2); // Ambiguous provider outcome can be attempted twice.
    }

    @Test
    void staleWorkerCannotOverwriteNewLeaseAndActiveLeaseIsNotAcknowledged() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        UUID eventId = UUID.fromString(jdbc().queryForObject("SELECT event_id FROM outbox_event", String.class));
        var store = app.getBean(WorkerStore.class);
        var first = store.claim(id, eventId, "old-worker").orElseThrow();
        assertThatThrownBy(() -> store.claim(id, eventId, "concurrent-worker")).isInstanceOf(IllegalStateException.class);
        jdbc().update("UPDATE notification SET worker_lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))");
        var second = store.claim(id, eventId, "new-worker").orElseThrow();
        var result = first.notification().completed(NotificationStatus.PROVIDER_ACCEPTED, "mock-result", null, java.time.Instant.now());
        assertThatThrownBy(() -> store.complete(result, "old-worker")).isInstanceOf(IllegalStateException.class);
        store.complete(second.notification().completed(NotificationStatus.PROVIDER_ACCEPTED, "new-result", null,
                java.time.Instant.now()), "new-worker");
        assertThat(store.claim(id, eventId, "replay")).isEmpty();
    }

    @Test
    void permanentProviderFailureIsDurableTerminalResultBeforeOffsetCommit() throws Exception {
        probe().behavior = request -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "MOCK_INVALID_TARGET");
        String id = json(post()).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("PERMANENT_FAILURE");
        assertThat(probe().attempts.get()).isEqualTo(1);
    }

    @Test
    void unsupportedEventStopsWithoutCommittingOffset() throws Exception {
        startWorker();
        kafka().send(topic, "invalid-key", "{\"schemaVersion\":99}").get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
        assertThat(committedTotal()).isZero();
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void sameKeyReusesDecodedPayloadDespiteJsonFormattingDifferences() throws Exception {
        var first = post(PAYLOAD, "request-1");
        var again = post("""
                {"body":"가상 데이터","title":"비동기 테스트","recipientId":"synthetic-user"}
                """, "request-1");
        assertThat(first.statusCode()).isEqualTo(202);
        assertThat(again.statusCode()).isEqualTo(200);
        assertThat(json(again)).isEqualTo(json(first));
        assertThat(again.headers().firstValue("Location")).isEqualTo(first.headers().firstValue("Location"));
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void sameKeyRejectsAnyChangedFieldWithoutLeakingPayloadOrKey() throws Exception {
        post(PAYLOAD, "private-key");
        for (String different : List.of(PAYLOAD.replace("synthetic-user", "Synthetic-user"),
                PAYLOAD.replace("비동기 테스트", "비동기 테스트 "), PAYLOAD.replace("가상 데이터", "다른 내용"))) {
            var conflict = post(different, "private-key");
            assertThat(conflict.statusCode()).isEqualTo(409);
            assertThat(conflict.body()).doesNotContain("private-key", "synthetic-user", "다른 내용", "가상 데이터");
        }
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
    }

    @Test
    void concurrentSameKeyCreatesExactlyOneNotificationAndOutbox() throws Exception {
        CountDownLatch ready = new CountDownLatch(8);
        Callable<HttpResponse<String>> send = () -> {
            ready.countDown();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            return post(PAYLOAD, "concurrent-key");
        };
        try (var executor = Executors.newFixedThreadPool(8)) {
            var pending = executor.invokeAll(java.util.Collections.nCopies(8, send));
            var responses = new java.util.ArrayList<HttpResponse<String>>();
            for (var future : pending) responses.add(future.get());
            assertThat(responses.stream().map(HttpResponse::statusCode).toList())
                    .containsOnly(200, 202).filteredOn(code -> code == 202).hasSize(1);
            var ids = new java.util.HashSet<String>();
            for (var response : responses) ids.add(json(response).path("notificationId").asText());
            assertThat(ids).hasSize(1);
        }
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
    }

    @Test
    void concurrentDifferentPayloadsChooseOneWinnerAndReturnConflicts() throws Exception {
        CountDownLatch ready = new CountDownLatch(8);
        var tasks = new java.util.ArrayList<Callable<HttpResponse<String>>>();
        for (int i = 0; i < 8; i++) {
            String payload = i % 2 == 0 ? PAYLOAD : PAYLOAD.replace("가상 데이터", "다른 내용");
            tasks.add(() -> {
                ready.countDown();
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                return post(payload, "mixed-key");
            });
        }
        try (var executor = Executors.newFixedThreadPool(8)) {
            var responses = executor.invokeAll(tasks);
            var codes = new java.util.ArrayList<Integer>();
            for (var response : responses) codes.add(response.get().statusCode());
            assertThat(codes).containsExactlyInAnyOrder(202, 200, 200, 200, 409, 409, 409, 409);
        }
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
    }

    @Test
    void unrelatedDuplicateFailureRollsBackKeyAndAllowsSafeRetry() throws Exception {
        root("CREATE TRIGGER reject_keyed_outbox BEFORE INSERT ON outbox_event FOR EACH ROW "
                + "SIGNAL SQLSTATE '23000' SET MYSQL_ERRNO = 1062, MESSAGE_TEXT = 'sensitive-outbox-error'");
        try {
            var failed = post(PAYLOAD, "rollback-key");
            assertThat(failed.statusCode()).isEqualTo(503);
            assertThat(failed.body()).doesNotContain("sensitive-outbox-error", "rollback-key");
            assertThat(count("notification")).isZero();
            assertThat(count("outbox_event")).isZero();
            assertThat(count("delivery_task")).isZero();
        } finally {
            root("DROP TRIGGER reject_keyed_outbox");
        }
        assertThat(post(PAYLOAD, "rollback-key").statusCode()).isEqualTo(202);
        assertThat(post(PAYLOAD, "rollback-key").statusCode()).isEqualTo(200);
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
    }

    @Test
    void replayAfterCompletionAndApplicationRestartReturnsCurrentResultWithoutResending() throws Exception {
        var first = post(PAYLOAD, "persistent-key");
        String id = json(first).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED");
        app.close();
        startApplication(false);
        var replay = post(PAYLOAD, "persistent-key");
        assertThat(replay.statusCode()).isEqualTo(200);
        assertThat(json(replay).path("notificationId").asText()).isEqualTo(id);
        assertThat(json(replay).path("status").asText()).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(probe().attempts.get()).isZero();
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
        assertThat(committedTotal()).isEqualTo(1);
    }

    @Test
    void keylessRequestsStayIndependentAndKeysAreCaseSensitiveWithLengthBoundary() throws Exception {
        assertThat(post().statusCode()).isEqualTo(202);
        assertThat(post().statusCode()).isEqualTo(202);
        assertThat(post(PAYLOAD, "Key").statusCode()).isEqualTo(202);
        assertThat(post(PAYLOAD, "key").statusCode()).isEqualTo(202);
        assertThat(post(PAYLOAD, "x".repeat(128)).statusCode()).isEqualTo(202);
        assertThat(post(PAYLOAD, "x".repeat(128)).statusCode()).isEqualTo(200);
        assertThat(count("notification")).isEqualTo(5);
        assertThat(count("outbox_event")).isEqualTo(5);
    }

    @Test
    void invalidAndRepeatedKeyHeadersDoNotCreateRows() throws Exception {
        for (String key : List.of("", "has space", "has,comma", "x".repeat(129), "quoted\"key")) {
            assertThat(post(PAYLOAD, key).statusCode()).isEqualTo(400);
        }
        assertThat(post(PAYLOAD, "same", "same").statusCode()).isEqualTo(400);
        assertThat(post(PAYLOAD, "first", "second").statusCode()).isEqualTo(400);
        assertThat(count("notification")).isZero();
        assertThat(count("outbox_event")).isZero();
        assertThat(count("delivery_task")).isZero();
    }

    @Test
    void invalidPayloadDoesNotReserveKey() throws Exception {
        assertThat(post(PAYLOAD.replace("synthetic-user", ""), "validation-key").statusCode()).isEqualTo(400);
        assertThat(post(PAYLOAD, "validation-key").statusCode()).isEqualTo(202);
        assertThat(count("notification")).isEqualTo(1);
    }

    @Test
    void replayReadsCommittedWinnerOutsideCallersOldSnapshot() {
        var tx = new TransactionTemplate(app.getBean(PlatformTransactionManager.class));
        tx.executeWithoutResult(status -> {
            assertThat(count("notification")).isZero(); // Establish an older repeatable-read snapshot.
            var submissions = app.getBean(IdempotentSubmissionService.class);
            var request = new NotificationRequest("synthetic", "title", "body");
            var first = submissions.submit(request, "transaction-key");
            var replay = submissions.submit(request, "transaction-key");
            assertThat(first.replayed()).isFalse();
            assertThat(replay.replayed()).isTrue();
            assertThat(replay.notification()).isEqualTo(first.notification());
            status.setRollbackOnly();
        });
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        assertThat(count("delivery_task")).isEqualTo(1);
    }

    @Test
    void taskIdentityIsUniqueStableAndHistorySurvivesCompletedReplay() throws Exception {
        String id = json(post(PAYLOAD, "task-key")).path("notificationId").asText();
        JsonNode initial = delivery(id);
        assertThat(initial.path("attemptCount").asInt()).isZero();
        assertThat(initial.path("maxAttempts").asInt()).isEqualTo(3);
        String taskId = initial.path("taskId").asText();
        String payload = jdbc().queryForObject("SELECT payload FROM outbox_event", String.class);
        var event = app.getBean(ObjectMapper.class).readTree(payload);
        assertThat(event.path("schemaVersion").asInt()).isEqualTo(2);
        assertThat(event.path("taskId").asText()).isEqualTo(taskId);
        assertThat(post(PAYLOAD, "task-key").statusCode()).isEqualTo(200);
        assertThat(delivery(id)).isEqualTo(initial);
        assertThat(count("delivery_task")).isEqualTo(1);
        assertThatThrownBy(() -> jdbc().update("""
                INSERT INTO delivery_task (task_id, notification_id, target_id, max_attempts, created_at)
                SELECT ?, notification_id, target_id, max_attempts, created_at FROM delivery_task
                """, UUID.randomUUID().toString())).isInstanceOf(org.springframework.dao.DuplicateKeyException.class);
        assertThatThrownBy(() -> jdbc().update("UPDATE delivery_task SET max_attempts = 0"))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        JsonNode completed = delivery(id);
        assertThat(completed.path("status").asText()).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(completed.path("attemptCount").asInt()).isEqualTo(1);
        assertThat(completed.path("attempts").get(0).path("outcome").asText()).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(completed.toString()).doesNotContain("leaseToken", "targetId", "synthetic-user", "가상 데이터");
        kafka().send(topic, id, payload).get(10, TimeUnit.SECONDS);
        awaitCommitted(2);
        assertThat(delivery(id)).isEqualTo(completed);
        assertThat(probe().attempts.get()).isEqualTo(1);
        assertThat(http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications/" + UUID.randomUUID()
                + "/delivery")).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
    }

    @Test
    void simultaneousWorkerClaimsCreateOnlyOneAttempt() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        UUID eventId = eventId();
        CountDownLatch ready = new CountDownLatch(2);
        Callable<Boolean> claim = () -> {
            ready.countDown();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            try {
                return app.getBean(WorkerStore.class).claim(id, eventId, UUID.randomUUID().toString()).orElseThrow().send();
            } catch (IllegalStateException activeLease) {
                assertThat(activeLease).hasMessage("Worker lease is active or absent");
                return false;
            }
        };
        try (var executor = Executors.newFixedThreadPool(2)) {
            var results = executor.invokeAll(List.of(claim, claim));
            assertThat(List.of(results.get(0).get(), results.get(1).get())).containsExactlyInAnyOrder(true, false);
        }
        assertThat(count("delivery_attempt")).isEqualTo(1);
        assertThat(delivery(id.toString()).path("attemptCount").asInt()).isEqualTo(1);
    }

    @Test
    void expiredRecoveryStopsAtPersistedAttemptCapBeforeCallingProvider() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        var store = app.getBean(WorkerStore.class);
        UUID eventId = eventId();
        for (int attempt = 0; attempt < 3; attempt++) {
            assertThat(store.claim(id, eventId, UUID.randomUUID().toString()).orElseThrow().send()).isTrue();
            expireWorker();
        }
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id.toString())).isEqualTo("INTERNAL_FAILURE");
        assertThat(jdbc().queryForObject("SELECT error_code FROM notification", String.class)).isEqualTo("ATTEMPTS_EXHAUSTED");
        assertThat(probe().attempts.get()).isZero();
        assertThat(registry().get("notification.worker.exhausted").counter().count()).isEqualTo(1);
        assertThat(new ObjectMapper().readTree(deadLetterPayload()).path("errorCode").asText()).isEqualTo("ATTEMPTS_EXHAUSTED");
        var history = delivery(id.toString());
        assertThat(history.path("attemptCount").asInt()).isEqualTo(3);
        assertThat(history.path("attempts")).hasSize(3);
        for (var attempt : history.path("attempts")) {
            assertThat(attempt.path("outcome").asText()).isEqualTo("UNKNOWN");
            assertThat(attempt.path("errorCode").asText()).isEqualTo("OUTCOME_UNKNOWN");
        }
        kafka().send(topic, id.toString(), jdbc().queryForObject("SELECT payload FROM outbox_event WHERE event_type = 'NOTIFICATION_REQUESTED'", String.class))
                .get(10, TimeUnit.SECONDS);
        awaitCommitted(2);
        assertThat(delivery(id.toString())).isEqualTo(history);
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void attemptInsertFailureRollsBackClaimAndCounter() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        UUID eventId = eventId();
        root("CREATE TRIGGER reject_attempt BEFORE INSERT ON delivery_attempt FOR EACH ROW "
                + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test fault'");
        try {
            assertThatThrownBy(() -> app.getBean(WorkerStore.class).claim(id, eventId, UUID.randomUUID().toString()))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThat(status(id.toString())).isEqualTo("QUEUED");
            assertThat(delivery(id.toString()).path("attemptCount").asInt()).isZero();
            assertThat(count("delivery_attempt")).isZero();
        } finally {
            root("DROP TRIGGER reject_attempt");
        }
        assertThat(app.getBean(WorkerStore.class).claim(id, eventId, UUID.randomUUID().toString()).orElseThrow().send()).isTrue();
    }

    @Test
    void historyCompletionFailureRollsBackResultAndPreservesOffsetUntilRecovery() throws Exception {
        String id = json(post()).path("notificationId").asText();
        root("CREATE TRIGGER reject_history BEFORE UPDATE ON delivery_attempt FOR EACH ROW "
                + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test fault'");
        try {
            startWorker();
            app.getBean(OutboxRelay.class).relay();
            await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
            assertThat(status(id)).isEqualTo("PROCESSING");
            assertThat(delivery(id).path("attempts").get(0).path("outcome").asText()).isEqualTo("STARTED");
            assertThat(committedTotal()).isZero();
        } finally {
            root("DROP TRIGGER reject_history");
        }
        expireWorker();
        startWorker();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED");
        var attempts = delivery(id).path("attempts");
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0).path("outcome").asText()).isEqualTo("UNKNOWN");
        assertThat(attempts.get(1).path("outcome").asText()).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(probe().attempts.get()).isEqualTo(2);
    }

    @Test
    void versionOneEventUsesPersistedTaskIdentity() throws Exception {
        String id = json(post()).path("notificationId").asText();
        jdbc().update("UPDATE outbox_event SET payload = JSON_REMOVE(JSON_SET(payload, '$.schemaVersion', 1), '$.taskId')");
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(count("delivery_attempt")).isEqualTo(1);
        assertThat(probe().attempts.get()).isEqualTo(1);
    }

    @Test
    void mismatchedTaskIdentityStopsBeforeClaimWithoutCommittingOffset() throws Exception {
        String id = json(post()).path("notificationId").asText();
        jdbc().update("UPDATE outbox_event SET payload = JSON_SET(payload, '$.taskId', ?)", UUID.randomUUID().toString());
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
        assertThat(committedTotal()).isZero();
        assertThat(status(id)).isEqualTo("QUEUED");
        assertThat(count("delivery_attempt")).isZero();
        assertThat(probe().attempts.get()).isZero();
    }

    private JsonNode delivery(String id) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create(base() + "/notifications/" + id + "/delivery"))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return json(response);
    }

    private UUID eventId() {
        return UUID.fromString(jdbc().queryForObject("SELECT event_id FROM outbox_event", String.class));
    }

    private void expireWorker() {
        jdbc().update("UPDATE notification SET worker_lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))");
    }

    @Test
    void transientFailurePersistsDueTimeAndResumesAfterApplicationRestart() throws Exception {
        probe().behavior = request -> transientFailure();
        String id = json(post(PAYLOAD, "retry-restart")).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("WAITING_RETRY");
        assertThat(jdbc().queryForObject("SELECT completed_at IS NULL FROM notification", Boolean.class)).isTrue();
        assertThat(count("outbox_event")).isEqualTo(2);
        var waiting = delivery(id);
        assertThat(waiting.path("attemptCount").asInt()).isEqualTo(1);
        assertThat(waiting.path("nextAttemptAt").isNull()).isFalse();
        assertThat(waiting.path("attempts").get(0).path("outcome").asText()).isEqualTo("RETRYABLE_FAILURE");
        assertThat(jdbc().queryForObject("""
                SELECT TIMESTAMPDIFF(MICROSECOND, created_at, available_at) FROM outbox_event
                WHERE event_type = 'NOTIFICATION_RETRY'
                """, Long.class)).isBetween(29000000L, 60000000L);
        assertThat(jdbc().queryForObject("""
                SELECT e.available_at = t.next_attempt_at FROM outbox_event e
                JOIN delivery_task t USING (notification_id) WHERE e.event_type = 'NOTIFICATION_RETRY'
                """, Boolean.class)).isTrue();
        assertThat(registry().find("notification.completed").counters()).isEmpty();
        assertThat(json(post(PAYLOAD, "retry-restart")).path("status").asText()).isEqualTo("WAITING_RETRY");
        app.close();
        startApplication(false);
        assertThat(delivery(id)).isEqualTo(waiting);
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        assertThat(probe().attempts.get()).isZero();
        assertThat(committedTotal()).isEqualTo(1);
        makeRetryDue();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(2);
        assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(delivery(id).path("attemptCount").asInt()).isEqualTo(2);
        assertThat(delivery(id).path("nextAttemptAt").isNull()).isTrue();
        assertThat(probe().attempts.get()).isEqualTo(1);
    }

    @Test
    void repeatedTransientFailuresStopAtTotalAttemptLimitAndReplayCannotReopenThem() throws Exception {
        probe().behavior = request -> transientFailure();
        String id = json(post()).path("notificationId").asText();
        String original = jdbc().queryForObject("SELECT payload FROM outbox_event", String.class);
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        for (int next = 2; next <= 3; next++) {
            makeRetryDue();
            app.getBean(OutboxRelay.class).relay();
            awaitCommitted(next);
        }
        assertThat(status(id)).isEqualTo("RETRYABLE_FAILURE");
        assertThat(count("outbox_event")).isEqualTo(4);
        assertThat(count("delivery_attempt")).isEqualTo(3);
        assertThat(probe().attempts.get()).isEqualTo(3);
        assertThat(registry().get("notification.retry.scheduled").counter().count()).isEqualTo(2);
        assertThat(registry().get("notification.completed").tag("status", "RETRYABLE_FAILURE").counter().count()).isEqualTo(1);
        assertThat(jdbc().queryForObject("SELECT completed_at IS NOT NULL FROM notification", Boolean.class)).isTrue();
        var deadLetter = new ObjectMapper().readTree(deadLetterPayload());
        assertThat(deadLetter.path("status").asText()).isEqualTo("RETRYABLE_FAILURE");
        assertThat(deadLetter.path("retryGeneration").asInt()).isEqualTo(2);
        assertThat(deadLetter.path("sourceEventId").asText()).isEqualTo(
                new ObjectMapper().readTree(retryPayload()).path("eventId").asText());
        String last = retryPayload();
        kafka().send(topic, id, original).get(10, TimeUnit.SECONDS);
        kafka().send(topic, id, last).get(10, TimeUnit.SECONDS);
        awaitCommitted(5);
        assertThat(probe().attempts.get()).isEqualTo(3);
        assertThat(count("outbox_event")).isEqualTo(4);
        assertThatThrownBy(() -> jdbc().update("""
                INSERT INTO outbox_event (event_id, notification_id, event_type, payload, retry_generation, created_at, available_at)
                SELECT ?, notification_id, event_type, payload, retry_generation, created_at, available_at
                FROM outbox_event WHERE retry_generation = 2
                """, UUID.randomUUID().toString())).isInstanceOf(org.springframework.dao.DuplicateKeyException.class);
    }

    @Test
    void obsoleteEventCannotAdvanceWaitingOrStealActiveRetry() throws Exception {
        probe().behavior = request -> transientFailure();
        String id = json(post()).path("notificationId").asText();
        String original = jdbc().queryForObject("SELECT payload FROM outbox_event", String.class);
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        makeRetryDue();
        kafka().send(topic, id, original).get(10, TimeUnit.SECONDS);
        awaitCommitted(2);
        assertThat(status(id)).isEqualTo("WAITING_RETRY");
        UUID retryEvent = UUID.fromString(jdbc().queryForObject(
                "SELECT event_id FROM outbox_event WHERE retry_generation = 1", String.class));
        var store = app.getBean(WorkerStore.class);
        var claim = store.claim(UUID.fromString(id), retryEvent, "active-retry").orElseThrow();
        kafka().send(topic, id, original).get(10, TimeUnit.SECONDS);
        awaitCommitted(3);
        assertThat(container().isRunning()).isTrue();
        assertThat(status(id)).isEqualTo("PROCESSING");
        assertThat(probe().attempts.get()).isEqualTo(1);
        assertThat(count("delivery_attempt")).isEqualTo(2);
        store.complete(claim.notification().completed(NotificationStatus.PROVIDER_ACCEPTED, "test-result", null,
                java.time.Instant.now()), "active-retry");
    }

    @Test
    void retryOutboxFailureRollsBackAttemptAndStateWithoutSkippingOriginalOffset() throws Exception {
        probe().behavior = request -> transientFailure();
        String id = json(post()).path("notificationId").asText();
        root("CREATE TRIGGER reject_retry BEFORE INSERT ON outbox_event FOR EACH ROW "
                + "BEGIN IF NEW.event_type = 'NOTIFICATION_RETRY' THEN SIGNAL SQLSTATE '45000' "
                + "SET MESSAGE_TEXT = 'test fault'; END IF; END");
        try {
            startWorker();
            app.getBean(OutboxRelay.class).relay();
            await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
            assertThat(committedTotal()).isZero();
            assertThat(status(id)).isEqualTo("PROCESSING");
            assertThat(count("outbox_event")).isEqualTo(1);
            assertThat(jdbc().queryForObject("SELECT retry_generation FROM delivery_task", Integer.class)).isZero();
            assertThat(delivery(id).path("nextAttemptAt").isNull()).isTrue();
            assertThat(delivery(id).path("attempts").get(0).path("outcome").asText()).isEqualTo("STARTED");
        } finally {
            root("DROP TRIGGER reject_retry");
        }
        probe().behavior = AsyncNotificationTest::accepted;
        expireWorker();
        startWorker();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(delivery(id).path("attempts").get(0).path("outcome").asText()).isEqualTo("UNKNOWN");
        assertThat(probe().attempts.get()).isEqualTo(2);
    }

    @Test
    void earlyRetryEventStopsWithoutCallingProviderAndRecoversOnlyWhenDue() throws Exception {
        probe().behavior = request -> transientFailure();
        String id = json(post()).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        kafka().send(topic, id, retryPayload()).get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
        assertThat(committedTotal()).isEqualTo(1);
        assertThat(count("delivery_attempt")).isEqualTo(1);
        assertThat(probe().attempts.get()).isEqualTo(1);
        makeRetryDue();
        probe().behavior = AsyncNotificationTest::accepted;
        startWorker();
        awaitCommitted(2);
        app.getBean(OutboxRelay.class).relay(); // Delayed outbox copy is now a harmless replay.
        awaitCommitted(3);
        assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(probe().attempts.get()).isEqualTo(2);
    }

    @Test
    void failedRetryPublishKeepsDurableScheduleUntilBrokerAcknowledges() throws Exception {
        probe().behavior = request -> transientFailure();
        String id = json(post()).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        makeRetryDue();
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String topic, String key, String data) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                return CompletableFuture.failedFuture(new IllegalStateException("test broker outage"));
            }
        };
        relay(fake).relay();
        assertThat(jdbc().queryForObject("SELECT status FROM outbox_event WHERE retry_generation = 1", String.class))
                .isEqualTo("PENDING");
        assertThat(status(id)).isEqualTo("WAITING_RETRY");
        assertThat(committedTotal()).isEqualTo(1); // Original offset is backed by a committed outbox, not a Kafka ACK.
        assertThat(probe().attempts.get()).isEqualTo(1);
        makeRetryDue();
        probe().behavior = AsyncNotificationTest::accepted;
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(2);
        assertThat(status(id)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(probe().attempts.get()).isEqualTo(2);
    }

    @Test
    void unexpectedProviderExceptionDoesNotCreateAutomaticRetry() throws Exception {
        probe().behavior = request -> { throw new IllegalStateException("test fault"); };
        String id = json(post()).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("INTERNAL_FAILURE");
        assertThat(count("outbox_event")).isEqualTo(2);
        assertThat(probe().attempts.get()).isEqualTo(1);
        assertThat(new ObjectMapper().readTree(deadLetterPayload()).path("status").asText()).isEqualTo("INTERNAL_FAILURE");
    }

    @Test
    void persistedOneAttemptBudgetSurvivesConfigurationChange() throws Exception {
        app.close();
        startApplication(false, "--app.delivery.max-attempts=1");
        String id = json(post()).path("notificationId").asText();
        app.close();
        startApplication(false, "--app.delivery.max-attempts=3");
        probe().behavior = request -> transientFailure();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("RETRYABLE_FAILURE");
        assertThat(delivery(id).path("maxAttempts").asInt()).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(2);
        assertThat(probe().attempts.get()).isEqualTo(1);
    }

    @Test
    void uncertainRecoveryAndTransientRetryShareOneTotalBudget() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        app.getBean(WorkerStore.class).claim(id, eventId(), "uncertain-first").orElseThrow();
        expireWorker();
        probe().behavior = request -> transientFailure();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(status(id.toString())).isEqualTo("WAITING_RETRY");
        makeRetryDue();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(2);
        assertThat(status(id.toString())).isEqualTo("RETRYABLE_FAILURE");
        assertThat(delivery(id.toString()).path("attemptCount").asInt()).isEqualTo(3);
        assertThat(delivery(id.toString()).path("attempts").get(0).path("outcome").asText()).isEqualTo("UNKNOWN");
        assertThat(count("outbox_event")).isEqualTo(3);
        assertThat(probe().attempts.get()).isEqualTo(2);
    }

    @Test
    void concurrentCompletionCreatesOnlyOneRetrySuccessor() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        var store = app.getBean(WorkerStore.class);
        var claim = store.claim(id, eventId(), "one-owner").orElseThrow();
        var result = claim.notification().completed(NotificationStatus.RETRYABLE_FAILURE, null, "MOCK_UNAVAILABLE",
                java.time.Instant.now());
        CountDownLatch ready = new CountDownLatch(2);
        Callable<Boolean> complete = () -> {
            ready.countDown();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            try {
                return store.complete(result, "one-owner");
            } catch (IllegalStateException stale) {
                assertThat(stale).hasMessage("Worker ownership was lost");
                return false;
            }
        };
        try (var executor = Executors.newFixedThreadPool(2)) {
            var results = executor.invokeAll(List.of(complete, complete));
            assertThat(List.of(results.get(0).get(), results.get(1).get())).containsExactlyInAnyOrder(true, false);
        }
        assertThat(count("outbox_event")).isEqualTo(2);
        assertThat(count("delivery_attempt")).isEqualTo(1);
        assertThat(status(id.toString())).isEqualTo("WAITING_RETRY");
    }

    private static PushResult transientFailure() {
        return new PushResult(PushResult.Outcome.RETRYABLE_FAILURE, null, "MOCK_UNAVAILABLE");
    }

    private String retryPayload() {
        return jdbc().queryForObject("SELECT payload FROM outbox_event WHERE event_type = 'NOTIFICATION_RETRY' "
                + "ORDER BY retry_generation DESC LIMIT 1", String.class);
    }

    private void makeRetryDue() {
        // Advance the persisted due time instead of sleeping for production backoff in integration tests.
        jdbc().update("UPDATE delivery_task SET next_attempt_at = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6)) "
                + "WHERE next_attempt_at IS NOT NULL");
        jdbc().update("UPDATE outbox_event SET available_at = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6)) "
                + "WHERE event_type = 'NOTIFICATION_RETRY' AND status = 'PENDING'");
    }

    @Test
    void dueRetryUsesSeparateTopicWhileFreshRequestsContinueAndLagIncludesRetry() throws Exception {
        probe().behavior = request -> transientFailure();
        String first = json(post()).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        assertThat(committedOn(topic)).isEqualTo(1);
        assertThat(committedOn(topic + ".retry")).isZero();
        probe().behavior = AsyncNotificationTest::accepted;
        String second = json(post()).path("notificationId").asText();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(2);
        assertThat(status(first)).isEqualTo("WAITING_RETRY");
        assertThat(status(second)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(topicSize(topic + ".retry")).isZero();
        makeRetryDue();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(3);
        assertThat(committedOn(topic)).isEqualTo(2);
        assertThat(committedOn(topic + ".retry")).isEqualTo(1);
        assertThat(status(first)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(probe().attempts.get()).isEqualTo(3);
        container().stop();
        kafka().send(topic + ".retry", first, retryPayload()).get(10, TimeUnit.SECONDS);
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(registry().get("notification.consumer.lag").gauge().value()).isEqualTo(1);
        startWorker();
        awaitCommitted(4);
        assertThat(probe().attempts.get()).isEqualTo(3);
    }

    @Test
    void permanentFailureReachesDlqWithStableIdentityAndNoPayloadOrAutomaticReplay() throws Exception {
        probe().behavior = request -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "MOCK_INVALID_TARGET");
        String id = json(post()).path("notificationId").asText();
        UUID source = eventId();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        app.getBean(OutboxRelay.class).relay();
        var messages = recordsOn(topic + ".dlq", 1);
        assertThat(messages).hasSize(1);
        assertThat(messages.getFirst().key()).isEqualTo(id);
        var event = new ObjectMapper().readTree(messages.getFirst().value());
        assertThat(event.path("eventType").asText()).isEqualTo("NOTIFICATION_DEAD_LETTER");
        assertThat(event.path("notificationId").asText()).isEqualTo(id);
        assertThat(event.path("taskId")).isEqualTo(delivery(id).path("taskId"));
        assertThat(event.path("sourceEventId").asText()).isEqualTo(source.toString());
        assertThat(event.path("status").asText()).isEqualTo("PERMANENT_FAILURE");
        assertThat(event.path("errorCode").asText()).isEqualTo("MOCK_INVALID_TARGET");
        assertThat(event.path("attemptCount").asInt()).isEqualTo(1);
        assertThat(event.path("maxAttempts").asInt()).isEqualTo(3);
        assertThat(event.path("retryGeneration").asInt()).isZero();
        assertThat(messages.getFirst().value()).doesNotContain("synthetic-user", "비동기 테스트", "가상 데이터", "lease", "token");
        assertThat(deadLetterStatus()).isEqualTo("PUBLISHED");
        assertThat(committedOn(topic + ".dlq")).isZero();
        String original = jdbc().queryForObject("SELECT payload FROM outbox_event WHERE event_type = 'NOTIFICATION_REQUESTED'", String.class);
        app.close();
        startApplication(false);
        startWorker();
        kafka().send(topic, id, original).get(10, TimeUnit.SECONDS);
        awaitCommitted(2);
        assertThat(probe().attempts.get()).isZero();
        assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE event_type = 'NOTIFICATION_DEAD_LETTER'", Integer.class)).isEqualTo(1);
        assertThat(new ObjectMapper().readTree(deadLetterPayload()).path("eventId")).isEqualTo(event.path("eventId"));
    }

    @Test
    void deadLetterInsertFailureRollsBackTerminalResultAndKeepsOriginalOffset() throws Exception {
        probe().behavior = request -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "MOCK_INVALID_TARGET");
        String id = json(post()).path("notificationId").asText();
        root("CREATE TRIGGER reject_dead_letter BEFORE INSERT ON outbox_event FOR EACH ROW "
                + "BEGIN IF NEW.event_type = 'NOTIFICATION_DEAD_LETTER' THEN SIGNAL SQLSTATE '45000' "
                + "SET MESSAGE_TEXT = 'test fault'; END IF; END");
        try {
            startWorker();
            app.getBean(OutboxRelay.class).relay();
            await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
            assertThat(committedTotal()).isZero();
            assertThat(status(id)).isEqualTo("PROCESSING");
            assertThat(count("outbox_event")).isEqualTo(1);
            assertThat(delivery(id).path("attempts").get(0).path("outcome").asText()).isEqualTo("STARTED");
        } finally {
            root("DROP TRIGGER reject_dead_letter");
        }
        expireWorker();
        startWorker();
        awaitCommitted(1);
        assertThat(status(id)).isEqualTo("PERMANENT_FAILURE");
        assertThat(delivery(id).path("attempts").get(0).path("outcome").asText()).isEqualTo("UNKNOWN");
        assertThat(new ObjectMapper().readTree(deadLetterPayload()).path("attemptCount").asInt()).isEqualTo(2);
        app.getBean(OutboxRelay.class).relay();
        assertThat(recordsOn(topic + ".dlq", 1)).hasSize(1);
    }

    @Test
    void deadLetterPublishRequiresAckAndSurvivesFailureAfterOriginalOffset() throws Exception {
        probe().behavior = request -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "MOCK_INVALID_TARGET");
        String id = json(post()).path("notificationId").asText();
        startWorker();
        // Publish just the initial event so the next relay call targets only the DLQ.
        var store = app.getBean(OutboxStore.class);
        var initial = store.claim(10).orElseThrow();
        kafka().send(topic, id, initial.payload()).get(10, TimeUnit.SECONDS);
        store.published(initial);
        awaitCommitted(1);
        String durable = deadLetterPayload();
        CompletableFuture<SendResult<String, String>> ack = new CompletableFuture<>();
        CountDownLatch publishing = new CountDownLatch(1);
        var fake = new KafkaTemplate<String, String>(kafka().getProducerFactory()) {
            @Override
            public CompletableFuture<SendResult<String, String>> send(String destination, String key, String data) {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                assertThat(destination).isEqualTo(topic + ".dlq");
                publishing.countDown();
                return ack;
            }
        };
        try (var executor = Executors.newSingleThreadExecutor()) {
            var running = executor.submit(() -> relay(fake).relay());
            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(deadLetterStatus()).isEqualTo("PROCESSING");
            assertThat(jdbc().queryForObject("SELECT published_at IS NULL FROM outbox_event WHERE event_type = 'NOTIFICATION_DEAD_LETTER'", Boolean.class)).isTrue();
            assertThat(committedOn(topic)).isEqualTo(1); // Backed by the durable DB handoff, not this pending ACK.
            ack.completeExceptionally(new IllegalStateException("test broker outage"));
            running.get(5, TimeUnit.SECONDS);
        }
        assertThat(deadLetterStatus()).isEqualTo("PENDING");
        app.close();
        startApplication(false);
        assertThat(deadLetterPayload()).isEqualTo(durable);
        expireOutbox();
        app.getBean(OutboxRelay.class).relay();
        assertThat(deadLetterStatus()).isEqualTo("PUBLISHED");
        assertThat(recordsOn(topic + ".dlq", 1).getFirst().value()).isEqualTo(durable);
        assertThat(probe().attempts.get()).isZero();
    }

    @Test
    void deadLetterAckThenDatabaseFailureReplaysSameEventWithoutCallingProvider() throws Exception {
        probe().behavior = request -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "MOCK_INVALID_TARGET");
        String id = json(post()).path("notificationId").asText();
        startWorker();
        var store = app.getBean(OutboxStore.class);
        var initial = store.claim(10).orElseThrow();
        kafka().send(topic, id, initial.payload()).get(10, TimeUnit.SECONDS);
        store.published(initial);
        awaitCommitted(1);
        root("CREATE TRIGGER reject_dead_letter_ack BEFORE UPDATE ON outbox_event FOR EACH ROW "
                + "BEGIN IF NEW.event_type = 'NOTIFICATION_DEAD_LETTER' AND NEW.status = 'PUBLISHED' THEN "
                + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test fault'; END IF; END");
        try {
            app.getBean(OutboxRelay.class).relay();
            assertThat(deadLetterStatus()).isEqualTo("PROCESSING");
            assertThat(recordsOn(topic + ".dlq", 1)).hasSize(1);
            assertThat(committedTotal()).isEqualTo(1);
        } finally {
            root("DROP TRIGGER reject_dead_letter_ack");
        }
        expireOutbox();
        app.getBean(OutboxRelay.class).relay();
        var copies = recordsOn(topic + ".dlq", 2);
        assertThat(copies).hasSize(2);
        assertThat(copies.getFirst().value()).isEqualTo(copies.getLast().value());
        assertThat(deadLetterStatus()).isEqualTo("PUBLISHED");
        assertThat(probe().attempts.get()).isEqualTo(1);
        assertThat(committedOn(topic + ".dlq")).isZero();
    }

    @Test
    void exhaustedLeaseCannotCommitWithoutItsDeadLetterHandoff() throws Exception {
        UUID id = UUID.fromString(json(post()).path("notificationId").asText());
        UUID source = eventId();
        var store = app.getBean(WorkerStore.class);
        for (int attempt = 0; attempt < 3; attempt++) {
            store.claim(id, source, UUID.randomUUID().toString()).orElseThrow();
            expireWorker();
        }
        root("CREATE TRIGGER reject_exhausted_dlq BEFORE INSERT ON outbox_event FOR EACH ROW "
                + "BEGIN IF NEW.event_type = 'NOTIFICATION_DEAD_LETTER' THEN SIGNAL SQLSTATE '45000' "
                + "SET MESSAGE_TEXT = 'test fault'; END IF; END");
        try {
            startWorker();
            app.getBean(OutboxRelay.class).relay();
            await().atMost(Duration.ofSeconds(20)).until(() -> !container().isRunning());
            assertThat(committedTotal()).isZero();
            assertThat(status(id.toString())).isEqualTo("PROCESSING");
            assertThat(delivery(id.toString()).path("attempts").get(2).path("outcome").asText()).isEqualTo("STARTED");
            assertThat(count("outbox_event")).isEqualTo(1);
        } finally {
            root("DROP TRIGGER reject_exhausted_dlq");
        }
        startWorker();
        awaitCommitted(1);
        assertThat(status(id.toString())).isEqualTo("INTERNAL_FAILURE");
        assertThat(delivery(id.toString()).path("attempts").get(2).path("outcome").asText()).isEqualTo("UNKNOWN");
        assertThat(probe().attempts.get()).isZero();
        assertThat(new ObjectMapper().readTree(deadLetterPayload()).path("errorCode").asText()).isEqualTo("ATTEMPTS_EXHAUSTED");
    }

    private long topicSize(String destination) throws Exception {
        var description = admin.describeTopics(List.of(destination)).allTopicNames().get(5, TimeUnit.SECONDS).get(destination);
        var latest = new java.util.HashMap<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.OffsetSpec>();
        description.partitions().forEach(p -> latest.put(new org.apache.kafka.common.TopicPartition(destination, p.partition()),
                org.apache.kafka.clients.admin.OffsetSpec.latest()));
        return admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS).values().stream().mapToLong(o -> o.offset()).sum();
    }

    private String deadLetterPayload() {
        return jdbc().queryForObject("SELECT payload FROM outbox_event WHERE event_type = 'NOTIFICATION_DEAD_LETTER'", String.class);
    }

    private String deadLetterStatus() {
        return jdbc().queryForObject("SELECT status FROM outbox_event WHERE event_type = 'NOTIFICATION_DEAD_LETTER'", String.class);
    }

    private long committedOn(String destination) throws Exception {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .entrySet().stream().filter(e -> e.getKey().topic().equals(destination)).mapToLong(e -> e.getValue().offset()).sum();
    }

    private List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recordsOn(String destination, int expected) {
        var properties = java.util.Map.<String, Object>of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "group.id", "audit-" + UUID.randomUUID(),
                "enable.auto.commit", false, "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties)) {
            // Explicit assignment avoids group startup latency and never affects the worker's offsets.
            var partitions = consumer.partitionsFor(destination).stream().map(p ->
                    new org.apache.kafka.common.TopicPartition(destination, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var messages = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(messages::add);
                assertThat(messages).hasSize(expected);
            });
            return messages;
        }
    }

    private static final String REDRIVE = "{\"reason\":\"PROVIDER_RECOVERED\",\"acknowledgePossibleDuplicate\":true}";

    @Test
    void selectedRedrivePreservesOriginalAndCompletesNewWorkAfterRestart() throws Exception {
        String original = publishedFailure("redrive-original");
        String event = deadLetterId(original);
        var oldNotification = json(post(PAYLOAD, "redrive-original"));
        var oldDelivery = delivery(original);
        var response = redrive(event, REDRIVE);
        assertThat(response.statusCode()).isEqualTo(202);
        var audit = json(response);
        String target = audit.path("targetNotificationId").asText();
        assertThat(target).isNotEqualTo(original);
        assertThat(response.headers().firstValue("Location")).contains("/notifications/" + target);
        assertThat(audit.path("rootNotificationId").asText()).isEqualTo(original);
        assertThat(audit.path("redriveNo").asInt()).isEqualTo(1);
        assertThat(audit.path("acknowledgedPossibleDuplicate").asBoolean()).isTrue();
        assertThat(delivery(target).path("taskId")).isNotEqualTo(oldDelivery.path("taskId"));
        assertThat(delivery(target).path("attemptCount").asInt()).isZero();
        assertThat(json(post(PAYLOAD, "redrive-original"))).isEqualTo(oldNotification);
        assertThat(delivery(original)).isEqualTo(oldDelivery);
        assertThat(json(deadLetterGet(event)).path("redrive")).isEqualTo(audit);
        assertThat(deadLetterGet(event).body()).doesNotContain("synthetic-user", "비동기 테스트", "가상 데이터");
        // Replaying old work cannot consume the fresh redrive's budget or invoke its provider.
        String oldEvent = jdbc().queryForObject("SELECT payload FROM outbox_event WHERE notification_id = ? "
                + "AND event_type = 'NOTIFICATION_REQUESTED'", String.class, original);
        kafka().send(topic, original, oldEvent).get(10, TimeUnit.SECONDS);
        awaitCommitted(2);
        assertThat(status(target)).isEqualTo("QUEUED");
        assertThat(probe().attempts.get()).isEqualTo(1);
        app.close();
        startApplication(false);
        var replay = redrive(event, REDRIVE);
        assertThat(replay.statusCode()).isEqualTo(200);
        assertThat(json(replay)).isEqualTo(audit);
        assertThat(count("dead_letter_redrive")).isEqualTo(1);
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(3);
        assertThat(status(target)).isEqualTo("PROVIDER_ACCEPTED");
        assertThat(status(original)).isEqualTo("PERMANENT_FAILURE");
        assertThat(probe().attempts.get()).isEqualTo(1);
        assertThat(committedOn(topic + ".dlq")).isZero();
    }

    @Test
    void concurrentIdenticalRedrivesCreateOnlyOneChildAndAudit() throws Exception {
        String original = publishedFailure(null);
        String event = deadLetterId(original);
        CountDownLatch ready = new CountDownLatch(8);
        Callable<HttpResponse<String>> send = () -> {
            ready.countDown();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            return redrive(event, REDRIVE);
        };
        try (var executor = Executors.newFixedThreadPool(8)) {
            var jobs = java.util.Collections.nCopies(8, send);
            var responses = executor.invokeAll(jobs);
            var codes = new java.util.ArrayList<Integer>();
            var targets = new java.util.HashSet<String>();
            for (var result : responses) {
                codes.add(result.get().statusCode());
                targets.add(json(result.get()).path("targetNotificationId").asText());
            }
            assertThat(codes).containsExactlyInAnyOrder(202, 200, 200, 200, 200, 200, 200, 200);
            assertThat(targets).hasSize(1);
        }
        assertThat(count("notification")).isEqualTo(2);
        assertThat(count("delivery_task")).isEqualTo(2);
        assertThat(count("outbox_event")).isEqualTo(3);
        assertThat(count("dead_letter_redrive")).isEqualTo(1);
    }

    @Test
    void concurrentDifferentRedriveReasonsConflictWithoutChangingAudit() throws Exception {
        String event = deadLetterId(publishedFailure(null));
        CountDownLatch ready = new CountDownLatch(2);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var jobs = List.<Callable<HttpResponse<String>>>of(
                    () -> { ready.countDown(); ready.await(5, TimeUnit.SECONDS); return redrive(event, REDRIVE); },
                    () -> { ready.countDown(); ready.await(5, TimeUnit.SECONDS); return redrive(event, REDRIVE.replace("PROVIDER_RECOVERED", "MANUAL_REVIEW")); });
            var results = executor.invokeAll(jobs);
            assertThat(List.of(results.get(0).get().statusCode(), results.get(1).get().statusCode()))
                    .containsExactlyInAnyOrder(202, 409);
        }
        assertThat(count("notification")).isEqualTo(2);
        assertThat(count("dead_letter_redrive")).isEqualTo(1);
    }

    @Test
    void invalidUnknownAndUnpublishedDeadLettersCannotBeRedriven() throws Exception {
        String original = publishedFailure(null);
        String event = deadLetterId(original);
        for (String invalid : List.of("{}", REDRIVE.replace("true", "false"),
                REDRIVE.replace("PROVIDER_RECOVERED", "unknown"), "{", "{\"acknowledgePossibleDuplicate\":true}")) {
            assertThat(redrive(event, invalid).statusCode()).isEqualTo(400);
        }
        assertThat(redrive("not-a-uuid", REDRIVE).statusCode()).isEqualTo(400);
        assertThat(redrive(UUID.randomUUID().toString(), REDRIVE).statusCode()).isEqualTo(404);
        String requestEvent = jdbc().queryForObject("SELECT event_id FROM outbox_event WHERE event_type = 'NOTIFICATION_REQUESTED'", String.class);
        assertThat(redrive(requestEvent, REDRIVE).statusCode()).isEqualTo(404);
        assertThat(deadLetterGet(requestEvent).statusCode()).isEqualTo(404);
        for (String state : List.of("PENDING", "FAILED")) {
            jdbc().update("UPDATE outbox_event SET status = ?, published_at = NULL WHERE event_id = ?", state, event);
            assertThat(redrive(event, REDRIVE).statusCode()).isEqualTo(409);
        }
        assertThat(count("notification")).isEqualTo(1);
        assertThat(count("dead_letter_redrive")).isZero();
    }

    @Test
    void redriveAuditAndOutboxFailuresRollBackAllNewWork() throws Exception {
        String original = publishedFailure(null);
        String event = deadLetterId(original);
        var history = delivery(original);
        for (String table : List.of("dead_letter_redrive", "outbox_event")) {
            root("CREATE TRIGGER reject_redrive BEFORE INSERT ON " + table + " FOR EACH ROW "
                    + "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test fault'");
            try {
                var response = redrive(event, REDRIVE);
                assertThat(response.statusCode()).isEqualTo(503);
                assertThat(response.body()).doesNotContain("test fault", "synthetic-user", "INSERT", "PROVIDER_RECOVERED");
                assertThat(count("notification")).isEqualTo(1);
                assertThat(count("delivery_task")).isEqualTo(1);
                assertThat(count("outbox_event")).isEqualTo(2);
                assertThat(count("dead_letter_redrive")).isZero();
                assertThat(delivery(original)).isEqualTo(history);
                assertThat(committedTotal()).isEqualTo(1);
            } finally {
                root("DROP TRIGGER reject_redrive");
            }
        }
        assertThat(redrive(event, REDRIVE).statusCode()).isEqualTo(202);
        assertThat(count("dead_letter_redrive")).isEqualTo(1);
    }

    @Test
    void manualRedriveChainStopsAtThreeAndInheritsTheOriginalAttemptBudget() throws Exception {
        app.close();
        startApplication(false, "--app.delivery.max-attempts=1");
        String original = publishedFailure(null);
        String current = original;
        String firstEvent = deadLetterId(original);
        app.close();
        startApplication(false, "--app.delivery.max-attempts=10");
        probe().behavior = request -> transientFailure();
        startWorker();
        for (int number = 1; number <= 3; number++) {
            var response = redrive(deadLetterId(current), REDRIVE);
            assertThat(response.statusCode()).isEqualTo(202);
            var audit = json(response);
            current = audit.path("targetNotificationId").asText();
            assertThat(audit.path("rootNotificationId").asText()).isEqualTo(original);
            assertThat(audit.path("redriveNo").asInt()).isEqualTo(number);
            assertThat(delivery(current).path("maxAttempts").asInt()).isEqualTo(1);
            app.getBean(OutboxRelay.class).relay();
            awaitCommitted(number + 1);
            app.getBean(OutboxRelay.class).relay();
            assertThat(status(current)).isEqualTo("RETRYABLE_FAILURE");
        }
        assertThat(redrive(deadLetterId(current), REDRIVE).statusCode()).isEqualTo(409);
        assertThat(redrive(firstEvent, REDRIVE).statusCode()).isEqualTo(200);
        assertThat(count("dead_letter_redrive")).isEqualTo(3);
        assertThat(count("notification")).isEqualTo(4);
        assertThat(count("delivery_attempt")).isEqualTo(4);
        assertThat(probe().attempts.get()).isEqualTo(3);
        assertThatThrownBy(() -> jdbc().update("UPDATE dead_letter_redrive SET redrive_no = 4 WHERE redrive_no = 3"))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
    }

    @Test
    void redriveReplayReadsCommittedAuditOutsideCallersOldSnapshot() throws Exception {
        String original = publishedFailure(null);
        String event = deadLetterId(original);
        var transaction = new TransactionTemplate(app.getBean(PlatformTransactionManager.class));
        transaction.executeWithoutResult(outer -> {
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM dead_letter_redrive", Integer.class)).isZero();
            try {
                assertThat(redrive(event, REDRIVE).statusCode()).isEqualTo(202);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
            var result = app.getBean(dev.jinhyeon.notificationlab.deadletter.DeadLetterService.class).redrive(
                    UUID.fromString(event), new dev.jinhyeon.notificationlab.deadletter.RedriveRequest(
                            dev.jinhyeon.notificationlab.deadletter.RedriveRequest.Reason.PROVIDER_RECOVERED, true));
            assertThat(result.replayed()).isTrue();
            outer.setRollbackOnly();
        });
        assertThat(count("dead_letter_redrive")).isEqualTo(1);
        assertThat(count("notification")).isEqualTo(2);
    }

    private String publishedFailure(String key) throws Exception {
        probe().behavior = request -> new PushResult(PushResult.Outcome.PERMANENT_FAILURE, null, "MOCK_INVALID_TARGET");
        String id = json(key == null ? post() : post(PAYLOAD, key)).path("notificationId").asText();
        startWorker();
        app.getBean(OutboxRelay.class).relay();
        awaitCommitted(1);
        app.getBean(OutboxRelay.class).relay();
        assertThat(jdbc().queryForObject("SELECT status FROM outbox_event WHERE event_id = ?", String.class, deadLetterId(id)))
                .isEqualTo("PUBLISHED");
        return id;
    }

    private String deadLetterId(String notificationId) {
        return jdbc().queryForObject("SELECT event_id FROM outbox_event WHERE notification_id = ? "
                + "AND event_type = 'NOTIFICATION_DEAD_LETTER'", String.class, notificationId);
    }

    private HttpResponse<String> redrive(String event, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + "/dead-letters/" + event + "/redrive"))
                .timeout(Duration.ofSeconds(20)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> deadLetterGet(String event) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base() + "/dead-letters/" + event)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @Tag("reconciliation")
    void reconcilesTenThousandHttpRequestsThroughRealKafka() throws Exception {
        long started = System.nanoTime();
        // Eight long-lived tasks: no unbounded submission queue of 10,000 requests.
        AtomicInteger acceptedCount = new AtomicInteger();
        Callable<Void> submit = () -> {
            for (int i = 0; i < 1250; i++) {
                assertThat(post().statusCode()).isEqualTo(202);
                acceptedCount.incrementAndGet();
            }
            return null;
        };
        try (var executor = Executors.newFixedThreadPool(8)) {
            var results = executor.invokeAll(java.util.Collections.nCopies(8, submit));
            for (var result : results) result.get();
        }
        await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM notification WHERE status = 'PROVIDER_ACCEPTED'", Integer.class))
                    .isEqualTo(10000);
            assertThat(jdbc().queryForObject("SELECT COUNT(*) FROM outbox_event WHERE status = 'PUBLISHED'", Integer.class))
                    .isEqualTo(10000);
        });
        awaitCommitted(10000);
        app.getBean(PipelineMetrics.class).refresh();
        assertThat(count("notification")).isEqualTo(acceptedCount.get()).isEqualTo(10000);
        assertThat(count("outbox_event")).isEqualTo(10000);
        assertThat(probe().attempts.get()).isEqualTo(10000);
        assertThat(registry().get("outbox.unpublished").gauge().value()).isZero();
        assertThat(registry().get("notification.consumer.lag").gauge().value()).isZero();
        System.out.printf("RECONCILIATION accepted=10000 providerAccepted=10000 failure=0 pending=0 published=10000 lag=0 elapsedSeconds=%.3f%n",
                (System.nanoTime() - started) / 1_000_000_000.0);
    }

    private HttpResponse<String> post() throws Exception {
        return post(PAYLOAD);
    }

    private HttpResponse<String> post(String payload, String... keys) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(base() + "/notifications"))
                .timeout(Duration.ofSeconds(20)).header("Content-Type", "application/json");
        for (String key : keys) request.header("Idempotency-Key", key);
        return http.send(request.POST(HttpRequest.BodyPublishers.ofString(payload)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode json(HttpResponse<String> response) throws Exception {
        return app.getBean(ObjectMapper.class).readTree(response.body());
    }

    private String base() { return "http://127.0.0.1:" + app.getWebServer().getPort(); }
    private JdbcTemplate jdbc() { return app.getBean(JdbcTemplate.class); }
    private Probe probe() { return app.getBean(Probe.class); }
    private MeterRegistry registry() { return app.getBean(MeterRegistry.class); }
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> kafka() { return app.getBean(KafkaTemplate.class); }
    private int count(String table) { return jdbc().queryForObject("SELECT COUNT(*) FROM " + table, Integer.class); }
    private String status(String id) { return jdbc().queryForObject("SELECT status FROM notification WHERE notification_id = ?", String.class, id); }
    private String outboxStatus() { return jdbc().queryForObject("SELECT status FROM outbox_event", String.class); }
    private void expireOutbox() { jdbc().update("UPDATE outbox_event SET available_at = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6))"); }
    private OutboxRelay relay(KafkaTemplate<String, String> template) {
        return new OutboxRelay(app.getBean(OutboxStore.class), template, app.getBean(AsyncProperties.class), registry(), app.getBean(dev.jinhyeon.notificationlab.config.TrafficRouting.class));
    }
    private org.springframework.kafka.listener.MessageListenerContainer container() {
        return app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("notification-worker");
    }
    private void startWorker() {
        container().start();
        await().atMost(Duration.ofSeconds(20)).until(() -> container().getAssignedPartitions() != null
                && container().getAssignedPartitions().size() == 6);
    }
    private long committedTotal() throws Exception {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                .entrySet().stream().filter(e -> e.getKey().topic().equals(topic) || e.getKey().topic().equals(topic + ".retry")).mapToLong(e -> e.getValue().offset()).sum();
    }
    private void awaitCommitted(long expected) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(committedTotal()).isEqualTo(expected));
    }
    private static PushResult accepted(PushRequest request) {
        return new PushResult(PushResult.Outcome.ACCEPTED, "mock-" + request.notificationId(), null);
    }
    private void root(String sql) throws Exception {
        try (var connection = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", MYSQL.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
    @TestConfiguration
    static class ProbeConfiguration {
        @Bean @Primary
        Probe providerProbe(MockPushProvider delegate) { return new Probe(delegate); }
    }
    static class Probe implements PushProvider {
        final AtomicInteger attempts = new AtomicInteger();
        volatile Function<PushRequest, PushResult> behavior;
        Probe(MockPushProvider delegate) { behavior = delegate::send; }
        @Override public PushResult send(PushRequest request) {
            attempts.incrementAndGet();
            return behavior.apply(request);
        }
    }
}
