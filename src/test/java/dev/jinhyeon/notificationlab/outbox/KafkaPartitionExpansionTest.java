package dev.jinhyeon.notificationlab.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Broker contract tests only: disposable topics/groups, no application DB or provider calls. */
@Testcontainers
@Timeout(90)
class KafkaPartitionExpansionTest {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2")
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                    .withMemory(1024L * 1024 * 1024).withNanoCPUs(1_000_000_000L)
                    .withPortBindings(Arrays.stream(cmd.getExposedPorts()).map(port ->
                            new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", 0), port)).toList()));

    @Test
    void keyedAppendsMoveAfterExpansionButExistingRecordsAndPartitionOrderStayIntact() throws Exception {
        String topic = topic();
        try (var admin = admin(); var producer = producer()) {
            create(admin, topic);
            var appends = new ArrayList<Append>();
            var before = new HashMap<String, Integer>();
            var after = new HashMap<String, Integer>();
            for (int key = 0; key < 60; key++) {
                for (int sequence = 0; sequence < 2; sequence++) {
                    var record = send(producer, topic, null, "synthetic-key-" + key, "before-" + sequence);
                    appends.add(record); before.put(record.key(), record.partition());
                }
            }
            expand(admin, producer, topic);
            for (int key = 0; key < 60; key++) {
                for (int sequence = 0; sequence < 2; sequence++) {
                    var record = send(producer, topic, null, "synthetic-key-" + key, "after-" + sequence);
                    appends.add(record); after.put(record.key(), record.partition());
                }
            }
            long moved = before.keySet().stream().filter(key -> !before.get(key).equals(after.get(key))).count();
            assertThat(moved).isBetween(1L, 59L);
            before.forEach((key, partition) -> assertThat(after.get(key)).isIn(partition, partition + 3));
            try (var consumer = consumer(group(), "earliest")) {
                consumer.assign(partitions(topic, 6)); consumer.seekToBeginning(consumer.assignment());
                var received = drain(consumer, 240);
                // Compare actual broker metadata and the complete append order within every partition.
                assertThat(received).containsExactlyInAnyOrderElementsOf(appends);
                for (int partition = 0; partition < 6; partition++) {
                    int selected = partition;
                    assertThat(received.stream().filter(row -> row.partition() == selected).toList())
                            .isEqualTo(appends.stream().filter(row -> row.partition() == selected).toList());
                }
            }
            report("key-mapping", Map.of("keys", 60, "beforeRecords", 120, "afterRecords", 120,
                    "movedKeys", moved, "unchangedKeys", 60 - moved, "verifiedRecords", 240,
                    "existingRecordsRedistributed", false, "perPartitionAppendOrderVerified", true));
        }
    }

    @Test
    void pausedOldPartitionAllowsNewerSameKeyRecordsFirstWithoutAdvancingItsOffset() throws Exception {
        String topic = topic(); String group = group(); String key = movingKey();
        try (var admin = admin(); var producer = producer()) {
            create(admin, topic);
            var old = send(producer, topic, null, key, "1");
            expand(admin, producer, topic);
            var next = send(producer, topic, null, key, "2");
            var last = send(producer, topic, null, key, "3");
            assertThat(next.partition()).isNotEqualTo(old.partition());
            assertThat(last.partition()).isEqualTo(next.partition());
            var oldPartition = new TopicPartition(topic, old.partition());
            var newPartition = new TopicPartition(topic, next.partition());
            try (var consumer = consumer(group, "earliest")) {
                consumer.assign(List.of(oldPartition, newPartition)); consumer.seekToBeginning(consumer.assignment());
                consumer.commitSync(Map.of(oldPartition, new OffsetAndMetadata(0)), Duration.ofSeconds(10));
                // Intentional processing gate, not a claim that natural scheduling always reverses order.
                consumer.pause(Set.of(oldPartition));
                var processed = new ArrayList<>(drain(consumer, 2));
                assertThat(processed).containsExactly(next, last);
                consumer.commitSync(Map.of(newPartition, new OffsetAndMetadata(2)), Duration.ofSeconds(10));
                assertThat(committed(admin, group).get(oldPartition).offset()).isZero();
                assertThat(consumer.position(oldPartition)).isZero();
                consumer.resume(Set.of(oldPartition)); processed.addAll(drain(consumer, 1));
                assertThat(processed.stream().map(Append::value).toList()).containsExactly("2", "3", "1");
                consumer.commitSync(Map.of(oldPartition, new OffsetAndMetadata(1)), Duration.ofSeconds(10));
                assertThat(committed(admin, group).get(oldPartition).offset()).isEqualTo(1);
                report("same-key-order", Map.of("processedSequence", List.of(2, 3, 1),
                        "oldPartition", old.partition(), "newPartition", next.partition(),
                        "oldCommittedWhilePaused", 0, "oldCommittedAfterCompletion", 1,
                        "newCommitted", 2, "intentionalOldPartitionPause", true));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"earliest", "latest"})
    void restartKeepsExistingCommitsButResetPolicyControlsPreviouslyUnseenPartitions(String reset) throws Exception {
        String topic = topic(); String group = group();
        try (var admin = admin(); var producer = producer()) {
            create(admin, topic);
            for (int partition = 0; partition < 3; partition++) {
                send(producer, topic, partition, null, "warm-" + partition);
            }
            try (var initial = consumer(group, "earliest")) {
                initial.assign(partitions(topic, 3));
                var warm = drain(initial, 3); commit(initial, warm);
            }
            var oldCommits = committed(admin, group);
            assertThat(oldCommits).hasSize(3);
            oldCommits.values().forEach(value -> assertThat(value.offset()).isEqualTo(1));
            for (int partition = 0; partition < 3; partition++) {
                send(producer, topic, partition, null, "pending-" + partition);
            }
            expand(admin, producer, topic);
            for (int partition = 3; partition < 6; partition++) {
                for (int sequence = 0; sequence < 2; sequence++) {
                    send(producer, topic, partition, null, "pre-discovery-" + partition + "-" + sequence);
                }
            }
            assertThat(committed(admin, group)).isEqualTo(oldCommits);
            try (var resumed = consumer(group, reset)) {
                resumed.subscribe(List.of(topic));
                boolean earliest = reset.equals("earliest");
                var first = drain(resumed, earliest ? 9 : 3);
                assertThat(resumed.assignment()).containsExactlyInAnyOrderElementsOf(partitions(topic, 6));
                assertThat(first.stream().filter(row -> row.partition() < 3).map(Append::value).toList())
                        .containsExactlyInAnyOrder("pending-0", "pending-1", "pending-2");
                assertThat(first.stream().filter(row -> row.partition() >= 3).count()).isEqualTo(earliest ? 6 : 0);
                // Fetch position is not a completed-work offset. Auto-commit remains disabled.
                assertThat(committed(admin, group)).isEqualTo(oldCommits);
                for (int partition = 3; partition < 6; partition++) {
                    assertThat(resumed.position(new TopicPartition(topic, partition))).isEqualTo(2);
                    send(producer, topic, partition, null, "post-discovery-" + partition);
                }
                var markers = drain(resumed, 3);
                assertThat(markers.stream().map(Append::value).toList())
                        .containsExactlyInAnyOrder("post-discovery-3", "post-discovery-4", "post-discovery-5");
                var handled = new ArrayList<>(first); handled.addAll(markers); commit(resumed, handled);
                var finalCommits = committed(admin, group);
                for (int partition = 0; partition < 6; partition++) {
                    assertThat(finalCommits.get(new TopicPartition(topic, partition)).offset())
                            .isEqualTo(partition < 3 ? 2 : 3);
                }
                // latest is a deliberately unsafe negative control, never an application setting change.
                report("offset-reset-" + reset, Map.of("reset", reset, "negativeControl", !earliest,
                        "initialProcessed", 3, "resumeProcessed", handled.size(), "totalAppended", 15,
                        "preDiscoveryNewRecordsRead", earliest ? 6 : 0,
                        "preDiscoveryNewRecordsSkipped", earliest ? 0 : 6,
                        "existingOffsetsBeforeRestart", List.of(1, 1, 1),
                        "finalCommittedOffsets", List.of(2, 2, 2, 3, 3, 3), "finalLag", 0));
            }
        }
    }

    @Test
    void activeGroupReassignsSixPartitionsToTwoMembersAndRetainsExistingCommits() throws Exception {
        String topic = topic(); String group = group();
        var assignments = new AtomicInteger();
        var listener = new ConsumerRebalanceListener() {
            @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) { }
            @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { assignments.incrementAndGet(); }
        };
        try (var admin = admin(); var producer = producer();
             var first = consumer(group, "earliest"); var second = consumer(group, "earliest")) {
            create(admin, topic);
            var members = List.of(first, second);
            first.subscribe(List.of(topic), listener); second.subscribe(List.of(topic), listener);
            awaitAssignments(members, 3);
            var beforeSizes = members.stream().map(member -> member.assignment().size()).sorted().toList();
            assertThat(beforeSizes).containsExactly(1, 2);
            for (int partition = 0; partition < 3; partition++) send(producer, topic, partition, null, "warm-" + partition);
            pollAndCommit(members, 3);
            var oldCommits = committed(admin, group);
            assertThat(oldCommits).hasSize(3);
            oldCommits.values().forEach(value -> assertThat(value.offset()).isEqualTo(1));
            int beforeCallbacks = assignments.get();
            expand(admin, producer, topic); awaitAssignments(members, 6);
            var afterSizes = members.stream().map(member -> member.assignment().size()).sorted().toList();
            assertThat(afterSizes).containsExactly(3, 3);
            assertThat(assignments.get()).isGreaterThan(beforeCallbacks);
            assertThat(committed(admin, group)).isEqualTo(oldCommits);
            for (int partition = 0; partition < 6; partition++) send(producer, topic, partition, null, "after-" + partition);
            pollAndCommit(members, 6);
            var offsets = committed(admin, group);
            for (int partition = 0; partition < 6; partition++) {
                assertThat(offsets.get(new TopicPartition(topic, partition)).offset()).isEqualTo(partition < 3 ? 2 : 1);
            }
            report("live-reassignment", Map.of("members", 2, "assignmentSizesBefore", beforeSizes,
                    "assignmentSizesAfter", afterSizes, "additionalAssignmentCallbacks", assignments.get() - beforeCallbacks,
                    "processed", 9, "assignmentOverlap", 0, "existingOffsetsPreserved", true,
                    "finalCommittedOffsets", List.of(2, 2, 2, 1, 1, 1)));
        }
    }

    private static Admin admin() {
        return Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 10000, "request.timeout.ms", 5000));
    }

    private static KafkaProducer<String, String> producer() {
        var properties = new HashMap<String, Object>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "acks", "all", "enable.idempotence", true, "max.in.flight.requests.per.connection", 1,
                "buffer.memory", 1048576, "max.block.ms", 3000, "request.timeout.ms", 3000,
                "delivery.timeout.ms", 10000));
        properties.put("metadata.max.age.ms", 100);
        return new KafkaProducer<>(properties);
    }

    private static KafkaConsumer<String, String> consumer(String group, String reset) {
        var properties = new HashMap<String, Object>();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers()); properties.put("group.id", group);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", false); properties.put("auto.offset.reset", reset);
        properties.put("default.api.timeout.ms", 10000); properties.put("request.timeout.ms", 5000);
        properties.put("metadata.max.age.ms", 100); properties.put("max.poll.records", 300);
        properties.put("max.poll.interval.ms", 60000); properties.put("session.timeout.ms", 10000);
        properties.put("heartbeat.interval.ms", 1000);
        properties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        return new KafkaConsumer<>(properties);
    }

    private static void create(Admin admin, String topic) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(10, TimeUnit.SECONDS);
    }

    private static void expand(Admin admin, KafkaProducer<String, String> producer, String topic) throws Exception {
        admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(6))).all().get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(producer.partitionsFor(topic)).hasSize(6));
    }

    private static Append send(KafkaProducer<String, String> producer, String topic, Integer partition,
                               String key, String value) throws Exception {
        var metadata = producer.send(new ProducerRecord<>(topic, partition, key, value)).get(10, TimeUnit.SECONDS);
        return new Append(key, value, metadata.partition(), metadata.offset());
    }

    private static List<Append> drain(KafkaConsumer<String, String> consumer, int expected) {
        var received = new ArrayList<Append>();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (received.size() < expected && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(record -> received.add(Append.of(record)));
            assertThat(received.size()).isLessThanOrEqualTo(300);
        }
        assertThat(received).hasSize(expected);
        return received;
    }

    private static void awaitAssignments(List<KafkaConsumer<String, String>> members, int count) {
        // No records are published during this assignment gate, so poll cannot discard business work.
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            members.forEach(member -> assertThat(member.poll(Duration.ofMillis(100))).isEmpty());
            var owned = members.stream().flatMap(member -> member.assignment().stream()).toList();
            assertThat(owned).hasSize(count).doesNotHaveDuplicates();
            assertThat(members).allMatch(member -> !member.assignment().isEmpty());
        });
    }

    private static void pollAndCommit(List<KafkaConsumer<String, String>> members, int expected) {
        var received = new LinkedHashMap<KafkaConsumer<String, String>, List<Append>>();
        members.forEach(member -> received.put(member, new ArrayList<>()));
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (received.values().stream().mapToInt(List::size).sum() < expected && System.nanoTime() < deadline) {
            members.forEach(member -> member.poll(Duration.ofMillis(100)).forEach(record -> received.get(member).add(Append.of(record))));
            assertThat(received.values().stream().mapToInt(List::size).sum()).isLessThanOrEqualTo(300);
        }
        var all = received.values().stream().flatMap(List::stream).toList();
        assertThat(all).hasSize(expected);
        assertThat(all.stream().map(Append::value).toList()).doesNotHaveDuplicates();
        received.forEach(KafkaPartitionExpansionTest::commit);
    }

    private static void commit(KafkaConsumer<String, String> consumer, List<Append> handled) {
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        String topic = consumer.assignment().iterator().next().topic();
        handled.forEach(row -> offsets.merge(new TopicPartition(topic, row.partition()),
                new OffsetAndMetadata(row.offset() + 1), (left, right) -> left.offset() > right.offset() ? left : right));
        consumer.commitSync(offsets, Duration.ofSeconds(10));
    }

    private static Map<TopicPartition, OffsetAndMetadata> committed(Admin admin, String group) throws Exception {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
    }

    private static List<TopicPartition> partitions(String topic, int count) {
        return IntStream.range(0, count).mapToObj(partition -> new TopicPartition(topic, partition)).toList();
    }

    private static String movingKey() {
        return IntStream.range(0, 10000).mapToObj(index -> "synthetic-order-key-" + index)
                .filter(key -> Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % 6 >= 3)
                .findFirst().orElseThrow();
    }

    private static void report(String name, Map<String, Object> observations) throws Exception {
        var result = new LinkedHashMap<String, Object>();
        result.put("result", "PASS"); result.put("scope", "Kafka-only disposable topic contract; no provider calls");
        result.put("broker", "apache/kafka:4.0.2"); result.put("fromPartitions", 3); result.put("toPartitions", 6);
        result.put("observations", observations);
        Path directory = Files.createDirectories(Path.of("build/reports/partition-expansion"));
        Path file = Files.createTempFile(directory, name + "-", ".json");
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(file.toFile(), result);
    }

    private static String topic() { return "partition-expansion-" + UUID.randomUUID(); }
    private static String group() { return "partition-contract-" + UUID.randomUUID(); }
    private record Append(String key, String value, int partition, long offset) {
        static Append of(ConsumerRecord<String, String> record) {
            return new Append(record.key(), record.value(), record.partition(), record.offset());
        }
    }
}
