package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;

/** Producer-only measurements: not an Outbox implementation or notification throughput test. */
@Tag("producer-batching-pilot")
@Testcontainers
@Timeout(180)
class KafkaProducerBatchingPilotTest {
    private static final int RECORDS = 1500;
    private static final int WARMUP = 150;
    private static final int PARTITIONS = 3;
    private static final List<String> METRICS = List.of("record-send-total", "record-error-total", "record-retry-total",
            "batch-size-avg", "records-per-request-avg", "compression-rate-avg", "outgoing-byte-total",
            "request-total", "record-queue-time-avg", "request-latency-avg", "record-size-avg",
            "buffer-available-bytes", "buffer-total-bytes");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2")
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                    .withMemory(1024L * 1024 * 1024).withNanoCPUs(1_000_000_000L)
                    .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    @Test
    void comparesOneFactorAtATimeAndVerifiesEveryAcknowledgedRecordAndOffset() throws Exception {
        var directory = Files.createTempDirectory(Files.createDirectories(Path.of("build/reports/producer-batching")), "run-");
        var rows = new ArrayList<Map<String, Object>>(8);
        var report = new LinkedHashMap<String, Object>();
        report.put("scope", "Kafka producer only; no Outbox, MySQL, Redis, Worker or provider calls");
        report.put("clientVersion", AppInfoParser.getVersion());
        report.put("broker", "apache/kafka:4.0.2");
        report.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        report.put("jvmMaxHeapBytes", Runtime.getRuntime().maxMemory());
        report.put("warmupRecordsPerRun", WARMUP);
        report.put("fixed", Map.of("recordsPerRun", RECORDS, "partitions", PARTITIONS, "brokerCpu", 1,
                "brokerMemoryMiB", 1024, "bufferMemoryBytes", 1048576, "batchSizeBytes", 16384,
                "maxInFlightRequestsPerConnection", 1, "acks", "all", "idempotence", true));
        var values = IntStream.range(0, RECORDS).mapToObj(KafkaProducerBatchingPilotTest::value).toList();
        var digest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < RECORDS; i++) {
            digest.update(key(i).getBytes(StandardCharsets.UTF_8)); digest.update((byte) 0);
            digest.update(values.get(i).getBytes(StandardCharsets.UTF_8)); digest.update((byte) '\n');
        }
        report.put("fixtureSha256", HexFormat.of().formatHex(digest.digest()));
        report.put("serializedValueBytesPerRun", values.stream().mapToLong(v -> v.getBytes(StandardCharsets.UTF_8).length).sum());
        var modes = Mode.values();
        var sequence = List.of(modes[0], modes[1], modes[2], modes[3], modes[3], modes[2], modes[1], modes[0]);
        report.put("sequence", sequence.stream().map(Enum::name).toList());
        report.put("rows", rows);
        var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 10000, "request.timeout.ms", 5000));
        long timestamp = System.currentTimeMillis();
        try {
            for (var mode : sequence) {
                var row = run(admin, mode, values, timestamp);
                rows.add(row);
                System.out.printf("PRODUCER_PILOT run=%d mode=%s records=%d seconds=%.3f recordsPerRequest=%.3f compression=%.3f%n",
                        rows.size(), mode, RECORDS, row.get("sendToLastAckSeconds"),
                        ((Map<?, ?>) row.get("metrics")).get("records-per-request-avg"),
                        ((Map<?, ?>) row.get("metrics")).get("compression-rate-avg"));
            }
            report.put("verifiedRecords", (RECORDS + WARMUP) * rows.size());
            report.put("measuredRecords", RECORDS * rows.size());
            report.put("result", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("result", "FAIL"); report.put("failureType", failure.getClass().getSimpleName());
            throw failure;
        } finally {
            admin.close(Duration.ofSeconds(3));
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(directory.resolve("result.json").toFile(), report);
        }
    }

    private static Map<String, Object> run(Admin admin, Mode mode, List<String> values, long timestamp) throws Exception {
        String topic = "producer-pilot-" + UUID.randomUUID();
        String group = "producer-pilot-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get(10, TimeUnit.SECONDS);
        var row = new LinkedHashMap<String, Object>();
        row.put("mode", mode.name()); row.put("submissionWindow", mode.window); row.put("lingerMillis", mode.linger);
        row.put("compression", mode.compression);
        var producer = producer(mode);
        try {
            assertThat(producer.partitionsFor(topic)).hasSize(PARTITIONS);
            send(producer, mode, topic, values, timestamp, WARMUP, true);
            var before = metrics(producer);
            // Kafka counts send attempts here, including retries; unique records are audited below.
            assertThat(before.get("record-send-total")).isGreaterThanOrEqualTo((double) WARMUP);
            assertThat(before.get("record-error-total")).isZero();
            row.put("warmupRecords", WARMUP); row.put("warmupRetries", before.get("record-retry-total"));
            row.put("warmupRecordSendAttempts", before.get("record-send-total"));
            var cpu = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long cpuStart = cpu.getProcessCpuTime(); long start = System.nanoTime();
            int retainedMax = send(producer, mode, topic, values, timestamp, RECORDS, false);
            double seconds = (System.nanoTime() - start) / 1e9;
            row.put("sendToLastAckSeconds", seconds); row.put("recordsPerSecond", RECORDS / seconds);
            row.put("clientJvmCpuPercentOneCore", (cpu.getProcessCpuTime() - cpuStart) / 1e9 / seconds * 100);
            row.put("maxRetainedFutures", retainedMax);
            var metrics = metrics(producer);
            assertThat(metrics.get("record-send-total")).isEqualTo(before.get("record-send-total") + RECORDS);
            assertThat(metrics.get("record-error-total")).isZero();
            var deltas = new LinkedHashMap<String, Double>();
            for (var name : METRICS) {
                if (name.endsWith("-total")) deltas.put(name, metrics.get(name) - before.get(name));
            }
            assertThat(deltas.get("record-send-total")).isEqualTo((double) RECORDS);
            assertThat(deltas.get("record-retry-total")).isZero();
            row.put("measurementCounterDeltas", deltas);
            row.put("metrics", metrics);
        } finally {
            producer.close(Duration.ofSeconds(3));
        }
        verify(admin, topic, group, values, timestamp);
        row.put("verifiedRecords", RECORDS + WARMUP); row.put("verifiedPartitionOffsets", List.of(550, 550, 550));
        row.put("verifiedContentAndPerPartitionOrder", true); row.put("committedEqualsEnd", true);
        return row;
    }

    private static int send(KafkaProducer<String, String> producer, Mode mode, String topic, List<String> values,
                            long timestamp, int count, boolean warmup) throws Exception {
        int retainedMax = 0;
        for (int base = 0; base < count; base += mode.window) {
            var futures = new ArrayList<Future<RecordMetadata>>(mode.window);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            for (int i = base; i < Math.min(base + mode.window, count); i++) {
                assertThat(deadline - System.nanoTime()).as("bounded submission deadline").isPositive();
                String prefix = warmup ? "warmup:" : "";
                futures.add(producer.send(new ProducerRecord<>(topic, i % PARTITIONS, timestamp, prefix + key(i), prefix + values.get(i))));
            }
            retainedMax = Math.max(retainedMax, futures.size());
            for (int i = 0; i < futures.size(); i++) {
                long remaining = deadline - System.nanoTime();
                assertThat(remaining).as("shared ACK deadline per submission window").isPositive();
                var acknowledged = futures.get(i).get(remaining, TimeUnit.NANOSECONDS);
                int sequence = base + i;
                assertThat(acknowledged.partition()).isEqualTo(sequence % PARTITIONS);
                assertThat(acknowledged.offset()).isEqualTo((warmup ? 0 : WARMUP / PARTITIONS) + sequence / PARTITIONS);
            }
        }
        return retainedMax;
    }

    private static void verify(Admin admin, String topic, String group, List<String> values, long timestamp) throws Exception {
        var properties = new HashMap<String, Object>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(), "group.id", group,
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "enable.auto.commit", false, "auto.offset.reset", "earliest", "max.poll.records", 100,
                "default.api.timeout.ms", 10000, "request.timeout.ms", 5000));
        properties.put("fetch.max.bytes", 1048576); properties.put("max.partition.fetch.bytes", 1048576);
        var consumer = new KafkaConsumer<String, String>(properties);
        var partitions = IntStream.range(0, PARTITIONS).mapToObj(p -> new TopicPartition(topic, p)).toList();
        try {
            consumer.assign(partitions); consumer.seekToBeginning(partitions);
            var seen = new boolean[RECORDS + WARMUP]; var last = new long[]{-1, -1, -1}; int count = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (count < RECORDS + WARMUP && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    int partition = record.partition(); int ordinal = Math.toIntExact(record.offset() * PARTITIONS + partition);
                    assertThat(ordinal).isBetween(0, RECORDS + WARMUP - 1);
                    assertThat(seen[ordinal]).as("no duplicate record").isFalse(); seen[ordinal] = true;
                    int sequence = ordinal < WARMUP ? ordinal : ordinal - WARMUP;
                    String prefix = ordinal < WARMUP ? "warmup:" : "";
                    assertThat(record.offset()).isEqualTo(last[partition] + 1); last[partition] = record.offset();
                    assertThat(record.key().equals(prefix + key(sequence))).as("synthetic key equality").isTrue();
                    assertThat(record.value().equals(prefix + values.get(sequence))).as("synthetic content equality").isTrue();
                    assertThat(record.timestamp()).isEqualTo(timestamp); count++;
                }
            }
            assertThat(count).isEqualTo(RECORDS + WARMUP);
            assertThat(consumer.poll(Duration.ofMillis(100)).count()).isZero();
            var ends = consumer.endOffsets(partitions, Duration.ofSeconds(5));
            var completed = new HashMap<TopicPartition, OffsetAndMetadata>();
            for (var partition : partitions) {
                assertThat(ends.get(partition)).isEqualTo((RECORDS + WARMUP) / PARTITIONS);
                assertThat(consumer.position(partition)).isEqualTo(ends.get(partition));
                completed.put(partition, new OffsetAndMetadata(ends.get(partition)));
            }
            consumer.commitSync(completed, Duration.ofSeconds(5));
            var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            assertThat(committed).hasSize(PARTITIONS);
            ends.forEach((partition, end) -> assertThat(committed.get(partition).offset()).isEqualTo(end));
        } finally {
            consumer.close(Duration.ofSeconds(3));
        }
    }

    private static KafkaProducer<String, String> producer(Mode mode) {
        var properties = new HashMap<String, Object>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "acks", "all", "enable.idempotence", true, "max.in.flight.requests.per.connection", 1,
                "buffer.memory", 1048576, "max.block.ms", 3000, "request.timeout.ms", 3000, "delivery.timeout.ms", 10000));
        properties.put("retries", 3); properties.put("batch.size", 16384);
        properties.put("linger.ms", mode.linger); properties.put("compression.type", mode.compression);
        return new KafkaProducer<>(properties);
    }

    private static Map<String, Double> metrics(KafkaProducer<String, String> producer) {
        var selected = new LinkedHashMap<String, Double>();
        producer.metrics().forEach((name, metric) -> {
            if (name.group().equals("producer-metrics") && METRICS.contains(name.name())) {
                double value = ((Number) metric.metricValue()).doubleValue();
                assertThat(Double.isFinite(value)).as("finite metric " + name.name()).isTrue();
                selected.put(name.name(), value);
            }
        });
        assertThat(selected).hasSize(METRICS.size()); return selected;
    }

    private static String key(int sequence) { return "synthetic-key-" + sequence % 120; }

    private static String value(int sequence) {
        return "{\"sequence\":" + sequence + ",\"kind\":\"SYNTHETIC\",\"nonce\":\""
                + UUID.nameUUIDFromBytes(("producer-pilot-" + sequence).getBytes(StandardCharsets.UTF_8))
                + "\",\"content\":\"" + "notification-lab-synthetic-".repeat(8) + "\"}";
    }

    private enum Mode {
        SERIAL_NONE_L0(1, 0, "none"), SERIAL_NONE_L5(1, 5, "none"),
        WINDOW10_NONE_L5(10, 5, "none"), WINDOW10_LZ4_L5(10, 5, "lz4");
        final int window;
        final int linger;
        final String compression;
        Mode(int window, int linger, String compression) { this.window = window; this.linger = linger; this.compression = compression; }
    }
}
