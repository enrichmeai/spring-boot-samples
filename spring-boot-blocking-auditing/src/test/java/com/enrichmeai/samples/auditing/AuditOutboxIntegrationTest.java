package com.enrichmeai.samples.auditing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.enrichmeai.samples.auditing.audit.AuditEvent;
import com.enrichmeai.samples.auditing.audit.AuditEventRepository;
import com.enrichmeai.samples.auditing.audit.AuditMessageMapper;
import com.enrichmeai.samples.auditing.audit.BatchResult;
import com.enrichmeai.samples.auditing.audit.EntityChangedEvent;
import com.enrichmeai.samples.auditing.audit.OutboxPoller;
import com.enrichmeai.samples.auditing.audit.OutboxStatus;
import com.enrichmeai.samples.auditing.cases.Case;
import com.enrichmeai.samples.auditing.cases.CaseRepository;
import com.enrichmeai.samples.auditing.cases.CaseService;
import com.enrichmeai.samples.auditing.consumer.EntityActivity;
import com.enrichmeai.samples.auditing.consumer.EntityActivityRepository;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The whole path against real Postgres and Kafka. The scheduler is off: each test drives
 * publishBatch() itself so it knows exactly when rows are published.
 */
@SpringBootTest(properties = "audit.outbox.poller-enabled=false")
class AuditOutboxIntegrationTest {

    private static final String TOPIC = "audit-events";
    private static final String CONSUMER_GROUP = "audit-projection";
    /** The same key OutboxPoller uses for its advisory lock. */
    private static final long OUTBOX_LOCK_KEY = 0x6175646974L;

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> Containers.postgres().getJdbcUrl());
        registry.add("spring.datasource.username", () -> Containers.postgres().getUsername());
        registry.add("spring.datasource.password", () -> Containers.postgres().getPassword());
        registry.add("spring.kafka.bootstrap-servers", () -> Containers.kafka().getBootstrapServers());
    }

    @Autowired CaseService caseService;
    @Autowired CaseRepository caseRepository;
    @Autowired AuditEventRepository auditRepository;
    @Autowired EntityActivityRepository activityRepository;
    @Autowired OutboxPoller poller;
    @Autowired TransactionTemplate outboxTransactionTemplate;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired ApplicationEventPublisher publisher;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired AuditMessageMapper mapper;

    private List<AuditEvent> auditOf(UUID caseId) {
        return auditRepository.findByEntityTypeAndEntityIdOrderById("case", caseId.toString());
    }

    private BatchResult publishOnce() {
        return outboxTransactionTemplate.execute(s -> poller.publishBatch());
    }

    private void drain() {
        BatchResult r;
        do {
            r = publishOnce();
            assertThat(r.lockAcquired()).as("poller lock").isTrue();
        } while (r.taken() > 0);
    }

    @Test
    @WithMockUser("alice")
    void aCommittedChangeWritesAPendingAuditRowWithActorAndSnapshots() {
        Case c = caseService.create("Lease dispute");
        caseService.close(c.getId());

        List<AuditEvent> rows = auditOf(c.getId());
        assertThat(rows).extracting(AuditEvent::getAction).containsExactly("CREATED", "CLOSED");
        assertThat(rows).allSatisfy(r -> {
            assertThat(r.getStatus()).isEqualTo(OutboxStatus.PENDING);
            assertThat(r.getActor()).isEqualTo("alice");
        });

        // Read the jsonb back as JSON, so a double-encoded string (a JSON string holding JSON) would fail.
        Map<String, Object> closed = jdbc.queryForMap(
                "SELECT before_state->>'status' AS before_status, after_state->>'status' AS after_status, "
                        + "jsonb_typeof(after_state) AS kind FROM audit_event WHERE event_id = ?",
                rows.get(1).getEventId());
        assertThat(closed).containsEntry("before_status", "OPEN")
                .containsEntry("after_status", "CLOSED")
                .containsEntry("kind", "object");
    }

    @Test
    void aRolledBackChangeLeavesNoAuditRow() {
        Case c = caseService.create("Will not close");

        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            caseService.close(c.getId());
            throw new IllegalStateException("something after the change failed");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(caseRepository.findById(c.getId())).get().extracting(Case::getStatus).isEqualTo("OPEN");
        assertThat(auditOf(c.getId())).extracting(AuditEvent::getAction).containsExactly("CREATED");
    }

    @Test
    void anEventPublishedOutsideATransactionIsNotRecorded() {
        String id = UUID.randomUUID().toString();

        publisher.publishEvent(new EntityChangedEvent("case", id, "CREATED", null, Map.of("id", id)));

        assertThat(auditRepository.findByEntityTypeAndEntityIdOrderById("case", id)).isEmpty();
    }

    @Test
    void thePollerPublishesInOrderAndTheConsumerAppliesEachEventOnce() {
        Case c = caseService.create("Boundary wall");
        caseService.rename(c.getId(), "Boundary wall (amended)");
        caseService.close(c.getId());

        drain();

        assertThat(auditOf(c.getId())).allSatisfy(r -> {
            assertThat(r.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
            assertThat(r.getPublishedAt()).isNotNull();
        });
        String key = "case:" + c.getId();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(activityRepository.findById(key)).get()
                        .satisfies(a -> {
                            assertThat(a.getEventCount()).isEqualTo(3);
                            assertThat(a.getLastAction()).isEqualTo("CLOSED");
                        }));

        // On the topic: same key for all three, in the order they happened.
        List<ConsumerRecord<String, String>> onTopic = readTopic().stream()
                .filter(r -> key.equals(r.key())).toList();
        assertThat(onTopic).extracting(r -> mapper.fromJson(r.value()).action())
                .containsExactly("CREATED", "RENAMED", "CLOSED");
    }

    @Test
    void aDuplicateDeliveryIsAppliedOnce() throws Exception {
        Case c = caseService.create("Duplicate me");
        drain();
        String key = "case:" + c.getId();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(activityRepository.findById(key)).get().extracting(EntityActivity::getEventCount).isEqualTo(1L));

        // Re-send exactly what the poller sent: same event_id, as after a crash before its commit.
        AuditEvent row = auditOf(c.getId()).get(0);
        ProducerRecord<String, String> again = new ProducerRecord<>(TOPIC, key, mapper.toJson(row));
        again.headers().add(OutboxPoller.HEADER_EVENT_ID, row.getEventId().toString().getBytes(StandardCharsets.UTF_8));
        RecordMetadata sent = kafkaTemplate.send(again).get().getRecordMetadata();

        awaitConsumerCommittedPast(new TopicPartition(TOPIC, sent.partition()), sent.offset());
        assertThat(activityRepository.findById(key)).get().extracting(EntityActivity::getEventCount).isEqualTo(1L);
    }

    @Test
    void aSecondPollerSkipsThePassWhileAnotherHoldsThePollerLock() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> holder = pool.submit(() -> outboxTransactionTemplate.executeWithoutResult(s -> {
                assertThat(auditRepository.tryPollerLock(OUTBOX_LOCK_KEY)).isTrue();
                held.countDown();
                await(release);
            }));
            await(held);

            BatchResult second = publishOnce();

            assertThat(second.lockAcquired()).isFalse();
            assertThat(second.taken()).isZero();
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
        assertThat(publishOnce().lockAcquired()).as("free again after the holder commits").isTrue();
    }

    @Test
    void thePollerPassesOverRowsAnotherTransactionHoldsInsteadOfWaiting() throws Exception {
        UUID held = caseService.create("Row held by someone else").getId();
        UUID free = caseService.create("Free row").getId();
        long heldRow = auditOf(held).get(0).getId();

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> holder = pool.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(s -> {
                jdbc.queryForObject("SELECT id FROM audit_event WHERE id = ? FOR UPDATE", Long.class, heldRow);
                locked.countDown();
                await(release);
            }));
            await(locked);

            // Without SKIP LOCKED this would wait for the holder, and the get() would time out.
            pool.submit(this::drain).get(20, TimeUnit.SECONDS);

            assertThat(auditOf(held)).extracting(AuditEvent::getStatus).containsOnly(OutboxStatus.PENDING);
            assertThat(auditOf(free)).extracting(AuditEvent::getStatus).containsOnly(OutboxStatus.PUBLISHED);
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
        drain();
        assertThat(auditOf(held)).extracting(AuditEvent::getStatus).containsOnly(OutboxStatus.PUBLISHED);
    }

    @Test
    void concurrentWritersToOneEntityAreAuditedAndPublishedInCommitOrder() throws Exception {
        UUID id = caseService.create("Contended").getId();
        CountDownLatch firstHasRow = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = pool.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(s -> {
                caseService.rename(id, "first");
                firstHasRow.countDown();
                await(releaseFirst); // still uncommitted
            }));
            await(firstHasRow);
            Future<?> second = pool.submit(() -> caseService.rename(id, "second"));

            // The second writer must queue on the row lock, before it can insert its audit row.
            Thread.sleep(1500);
            assertThat(second.isDone()).as("second writer waits for the first to commit").isFalse();

            releaseFirst.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        } finally {
            releaseFirst.countDown();
            pool.shutdownNow();
        }

        List<AuditEvent> rows = auditOf(id);
        assertThat(rows).extracting(r -> r.getAction() + ":" + mapper.fromJson(mapper.toJson(r)).after().get("title").asText())
                .containsExactly("CREATED:Contended", "RENAMED:first", "RENAMED:second");
        drain();
        List<String> titlesOnTopic = readTopic().stream().filter(r -> ("case:" + id).equals(r.key()))
                .map(r -> mapper.fromJson(r.value()).after().get("title").asText()).toList();
        assertThat(titlesOnTopic).containsExactly("Contended", "first", "second");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(20, TimeUnit.SECONDS)) {
                throw new AssertionError("latch not reached within 20 s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void aLaterEventWaitsWhileAnEarlierEventOfTheSameEntityIsWaitingOrFailed() {
        Case c = caseService.create("Held back");
        caseService.close(c.getId());
        List<AuditEvent> rows = auditOf(c.getId());
        long first = rows.get(0).getId();
        long second = rows.get(1).getId();

        // As if the first event had just failed once and is waiting for its retry.
        jdbc.update("UPDATE audit_event SET attempts = 1, next_attempt_at = ? WHERE id = ?",
                java.sql.Timestamp.from(Instant.now().plus(Duration.ofHours(1))), first);
        drain();
        assertThat(auditRepository.findById(second)).get().extracting(AuditEvent::getStatus)
                .isEqualTo(OutboxStatus.PENDING);

        // Gave up for good: the later event still waits, for a person to act.
        jdbc.update("UPDATE audit_event SET status = 'FAILED' WHERE id = ?", first);
        drain();
        assertThat(auditRepository.findById(second)).get().extracting(AuditEvent::getStatus)
                .isEqualTo(OutboxStatus.PENDING);

        // Once the first is published, the second goes.
        jdbc.update("UPDATE audit_event SET status = 'PUBLISHED', published_at = now() WHERE id = ?", first);
        drain();
        assertThat(auditRepository.findById(second)).get().extracting(AuditEvent::getStatus)
                .isEqualTo(OutboxStatus.PUBLISHED);
    }

    /** Every record currently on the topic, read with a throwaway consumer group. */
    private List<ConsumerRecord<String, String>> readTopic() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafka().getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(TOPIC));
            Map<TopicPartition, Long> end = null;
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(all::add);
                if (consumer.assignment().isEmpty()) {
                    continue;
                }
                if (end == null) {
                    end = consumer.endOffsets(consumer.assignment());
                }
                Map<TopicPartition, Long> target = end;
                if (target.entrySet().stream().allMatch(e -> consumer.position(e.getKey()) >= e.getValue())) {
                    return all;
                }
            }
        }
        throw new AssertionError("Could not read " + TOPIC + " to its end within 30 s");
    }

    /** Waits until the app's consumer group has committed past {@code offset} on {@code partition}. */
    private void awaitConsumerCommittedPast(TopicPartition partition, long offset) {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafka().getBootstrapServers());
        try (AdminClient admin = AdminClient.create(config)) {
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                OffsetAndMetadata committed = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata().get().get(partition);
                assertThat(committed).as("committed offset of %s", partition).isNotNull();
                assertThat(committed.offset()).isGreaterThan(offset);
            });
        }
    }
}
