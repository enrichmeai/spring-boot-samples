package com.enrichmeai.samples.reactiveauditing;

import static com.enrichmeai.samples.reactiveauditing.IntegrationTestSupport.TIMEOUT;
import static com.enrichmeai.samples.reactiveauditing.IntegrationTestSupport.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.enrichmeai.samples.reactiveauditing.audit.AuditEvent;
import com.enrichmeai.samples.reactiveauditing.audit.AuditEventRepository;
import com.enrichmeai.samples.reactiveauditing.audit.AuditMessageMapper;
import com.enrichmeai.samples.reactiveauditing.audit.AuditRecorder;
import com.enrichmeai.samples.reactiveauditing.audit.BatchResult;
import com.enrichmeai.samples.reactiveauditing.audit.OutboxDrainer;
import com.enrichmeai.samples.reactiveauditing.audit.OutboxPoller;
import com.enrichmeai.samples.reactiveauditing.audit.OutboxStatus;
import com.enrichmeai.samples.reactiveauditing.cases.Case;
import com.enrichmeai.samples.reactiveauditing.cases.CaseRepository;
import com.enrichmeai.samples.reactiveauditing.cases.CaseService;
import com.enrichmeai.samples.reactiveauditing.consumer.EntityActivityRepository;
import com.enrichmeai.samples.reactiveauditing.consumer.EntityActivityRepository.EntityActivity;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

/**
 * The whole path against real Postgres and Kafka. The drainer is off: each test publishes batches
 * itself so it knows exactly when rows go out. Test code may block; the application code does not.
 */
@SpringBootTest(properties = "audit.outbox.poller-enabled=false")
class ReactiveAuditOutboxIntegrationTest {

    private static final String TOPIC = "audit-events";
    private static final String CONSUMER_GROUP = "audit-projection";
    private static final String DLT = "audit-events.DLT";

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        IntegrationTestSupport.postgres(registry);
        registry.add("spring.kafka.bootstrap-servers", () -> Containers.kafka().getBootstrapServers());
    }

    @Autowired CaseService caseService;
    @Autowired CaseRepository caseRepository;
    @Autowired AuditEventRepository auditRepository;
    @Autowired AuditRecorder recorder;
    @Autowired EntityActivityRepository activityRepository;
    @Autowired OutboxPoller poller;
    @Autowired OutboxDrainer drainer;
    @Autowired TransactionalOperator tx;
    @Autowired ReactiveTransactionManager transactionManager;
    @Autowired DatabaseClient db;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired AuditMessageMapper mapper;

    private List<AuditEvent> auditOf(UUID caseId) {
        return auditRepository.findByEntity("case", caseId.toString()).collectList().block(TIMEOUT);
    }

    private Case create(String title) {
        return caseService.create(title).block(TIMEOUT);
    }

    private BatchResult publishOnce() {
        return tx.transactional(poller.publishBatch()).block(TIMEOUT);
    }

    private void drain() {
        BatchResult r;
        do {
            r = publishOnce();
            assertThat(r.lockAcquired()).as("poller lock").isTrue();
        } while (r.taken() > 0);
    }

    @Test
    void aCommittedChangeWritesAPendingAuditRowWithActorAndSnapshots() {
        Case c = as("alice", caseService.create("Lease dispute")).block(TIMEOUT);
        as("alice", caseService.close(c.id())).block(TIMEOUT);

        List<AuditEvent> rows = auditOf(c.id());
        assertThat(rows).extracting(AuditEvent::action).containsExactly("CREATED", "CLOSED");
        assertThat(rows).allSatisfy(r -> {
            assertThat(r.status()).isEqualTo(OutboxStatus.PENDING);
            assertThat(r.actor()).isEqualTo("alice");
        });
        Map<String, Object> closed = db.sql("SELECT before_state->>'status' AS b, after_state->>'status' AS a, "
                        + "jsonb_typeof(after_state) AS kind FROM audit_event WHERE event_id = :id")
                .bind("id", rows.get(1).eventId()).fetch().one().block(TIMEOUT);
        assertThat(closed).containsEntry("b", "OPEN").containsEntry("a", "CLOSED").containsEntry("kind", "object");
    }

    @Test
    void withNoUserTheActorIsSystem() {
        Case c = create("No user");
        assertThat(auditOf(c.id())).extracting(AuditEvent::actor).containsExactly("system");
    }

    @Test
    void aRolledBackChangeLeavesNoAuditRow() {
        Case c = create("Will not close");

        StepVerifier.create(tx.transactional(caseService.close(c.id())
                        .then(Mono.error(new IllegalStateException("something after the change failed")))))
                .expectError(IllegalStateException.class)
                .verify(TIMEOUT);

        assertThat(caseRepository.findById(c.id()).block(TIMEOUT).status()).isEqualTo("OPEN");
        assertThat(auditOf(c.id())).extracting(AuditEvent::action).containsExactly("CREATED");
    }

    @Test
    void recordingOutsideATransactionFailsAndWritesNothing() {
        String id = UUID.randomUUID().toString();

        StepVerifier.create(recorder.record("case", id, "CREATED", null, Map.of("id", id)))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("inside the change's transaction"))
                .verify(TIMEOUT);

        assertThat(auditRepository.findByEntity("case", id).collectList().block(TIMEOUT)).isEmpty();
    }

    @Test
    void recordingInATransactionContextWithNoActualTransactionFails() {
        String id = UUID.randomUUID().toString();
        DefaultTransactionDefinition supports = new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_SUPPORTS);
        TransactionalOperator supportsOnly = TransactionalOperator.create(transactionManager, supports);

        StepVerifier.create(supportsOnly.transactional(recorder.record("case", id, "CREATED", null, Map.of("id", id))))
                .expectError(IllegalStateException.class)
                .verify(TIMEOUT);

        assertThat(auditRepository.findByEntity("case", id).collectList().block(TIMEOUT)).isEmpty();
    }

    @Test
    void thePollerPublishesInOrderAndTheConsumerAppliesEachEventOnce() {
        Case c = create("Boundary wall");
        caseService.rename(c.id(), "Boundary wall (amended)").block(TIMEOUT);
        caseService.close(c.id()).block(TIMEOUT);

        drain();

        assertThat(auditOf(c.id())).allSatisfy(r -> {
            assertThat(r.status()).isEqualTo(OutboxStatus.PUBLISHED);
            assertThat(r.publishedAt()).isNotNull();
        });
        String key = "case:" + c.id();
        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(activityRepository.findById(key).block(TIMEOUT)).isNotNull()
                        .satisfies(a -> {
                            assertThat(a.eventCount()).isEqualTo(3);
                            assertThat(a.lastAction()).isEqualTo("CLOSED");
                        }));
        assertThat(readTopic().stream().filter(r -> key.equals(r.key())).map(r -> mapper.fromJson(r.value()).action()))
                .containsExactly("CREATED", "RENAMED", "CLOSED");
    }

    @Test
    void aDuplicateDeliveryIsAppliedOnce() throws Exception {
        Case c = create("Duplicate me");
        drain();
        String key = "case:" + c.id();
        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(activityRepository.findById(key).block(TIMEOUT)).extracting(EntityActivity::eventCount).isEqualTo(1L));

        // Re-send exactly what the poller sent: same event_id, as after a crash before its commit.
        AuditEvent row = auditOf(c.id()).get(0);
        ProducerRecord<String, String> again = new ProducerRecord<>(TOPIC, key, mapper.toJson(row));
        again.headers().add(OutboxPoller.HEADER_EVENT_ID, row.eventId().toString().getBytes(StandardCharsets.UTF_8));
        RecordMetadata sent = kafkaTemplate.send(again).get().getRecordMetadata();

        awaitConsumerCommittedPast(new TopicPartition(TOPIC, sent.partition()), sent.offset());
        assertThat(activityRepository.findById(key).block(TIMEOUT)).extracting(EntityActivity::eventCount).isEqualTo(1L);
        // Skipped by the dedup marker, not dropped by an error: one marker, nothing dead-lettered.
        assertThat(db.sql("SELECT count(*) AS n FROM processed_event WHERE event_id = :id").bind("id", row.eventId())
                .map(r -> r.get("n", Long.class)).one().block(TIMEOUT)).isEqualTo(1L);
        assertThat(readTopic(DLT)).noneMatch(r -> key.equals(r.key()));
    }

    @Test
    void aMalformedRecordIsDeadLetteredAndTheStreamCarriesOn() throws Exception {
        String badKey = "case:malformed-" + UUID.randomUUID();
        RecordMetadata sent = kafkaTemplate.send(new ProducerRecord<>(TOPIC, badKey, "{\"not\":\"an audit message\"}"))
                .get().getRecordMetadata(); // and no event_id header

        awaitConsumerCommittedPast(new TopicPartition(TOPIC, sent.partition()), sent.offset());
        assertThat(readTopic(DLT)).filteredOn(r -> badKey.equals(r.key())).singleElement()
                .satisfies(r -> assertThat(new String(r.headers().lastHeader("dlt-error").value(), StandardCharsets.UTF_8))
                        .contains("no event_id header"));

        // The consumer is not stuck behind it.
        Case c = create("After the bad record");
        drain();
        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(activityRepository.findById("case:" + c.id()).block(TIMEOUT)).isNotNull());
    }

    @Test
    void manyEventsForOneEntityAreAppliedInOrder() {
        Case c = create("Burst 0");
        for (int i = 1; i <= 50; i++) {
            caseService.rename(c.id(), "Burst " + i).block(TIMEOUT);
        }
        caseService.close(c.id()).block(TIMEOUT);

        drain(); // 52 events, sent in one go

        String key = "case:" + c.id();
        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(activityRepository.findById(key).block(TIMEOUT)).isNotNull()
                        .satisfies(a -> {
                            assertThat(a.eventCount()).isEqualTo(52);
                            // last-writer-wins column: right only if the records were applied in order
                            assertThat(a.lastAction()).isEqualTo("CLOSED");
                        }));
    }

    @Test
    void aSecondPollerSkipsThePassWhileAnotherHoldsThePollerLock() {
        CountDownLatch held = new CountDownLatch(1);
        Sinks.Empty<Void> release = Sinks.empty();
        CompletableFuture<Void> holder = tx.transactional(auditRepository.tryPollerLock(OutboxPoller.POLLER_LOCK_KEY)
                        .doOnNext(locked -> {
                            assertThat(locked).isTrue();
                            held.countDown();
                        })
                        .then(release.asMono()))
                .toFuture();
        try {
            await(held);

            BatchResult second = publishOnce();

            assertThat(second.lockAcquired()).isFalse();
            assertThat(second.taken()).isZero();
        } finally {
            release.tryEmitEmpty();
        }
        holder.orTimeout(10, TimeUnit.SECONDS).join();
        assertThat(publishOnce().lockAcquired()).as("free again after the holder commits").isTrue();
    }

    @Test
    void thePollerPassesOverRowsAnotherTransactionHoldsInsteadOfWaiting() {
        UUID held = create("Row held by someone else").id();
        UUID free = create("Free row").id();
        long heldRow = auditOf(held).get(0).id();

        CountDownLatch locked = new CountDownLatch(1);
        Sinks.Empty<Void> release = Sinks.empty();
        CompletableFuture<Void> holder = tx.transactional(db.sql("SELECT id FROM audit_event WHERE id = :id FOR UPDATE")
                        .bind("id", heldRow).fetch().one()
                        .doOnNext(row -> locked.countDown())
                        .then(release.asMono()))
                .toFuture();
        try {
            await(locked);

            // Without SKIP LOCKED this would wait for the holder, and block() would time out.
            drainer.drain().block(Duration.ofSeconds(20));

            assertThat(auditOf(held)).extracting(AuditEvent::status).containsOnly(OutboxStatus.PENDING);
            assertThat(auditOf(free)).extracting(AuditEvent::status).containsOnly(OutboxStatus.PUBLISHED);
        } finally {
            release.tryEmitEmpty();
        }
        holder.orTimeout(10, TimeUnit.SECONDS).join();
        drain();
        assertThat(auditOf(held)).extracting(AuditEvent::status).containsOnly(OutboxStatus.PUBLISHED);
    }

    @Test
    void concurrentWritersToOneEntityAreSerialisedAuditedAndPublishedInCommitOrder() throws Exception {
        UUID id = create("Contended").id();
        CountDownLatch firstHasRow = new CountDownLatch(1);
        Sinks.Empty<Void> releaseFirst = Sinks.empty();

        CompletableFuture<Void> first = tx.transactional(caseService.rename(id, "first")
                        .doOnNext(c -> firstHasRow.countDown())
                        .then(releaseFirst.asMono())) // still uncommitted
                .toFuture();
        CompletableFuture<Case> second;
        try {
            await(firstHasRow);
            second = caseService.rename(id, "second").toFuture();

            // The second writer must queue on the row lock, before it can insert its audit row.
            // Evidence, not a sleep: Postgres shows a lock request that has not been granted.
            Awaitility.await().atMost(TIMEOUT).until(() -> db.sql("SELECT count(*) AS n FROM pg_locks WHERE NOT granted")
                    .map(r -> r.get("n", Long.class)).one().block(TIMEOUT) > 0);
            assertThat(second.isDone()).as("second writer waits for the first to commit").isFalse();
        } finally {
            releaseFirst.tryEmitEmpty();
        }
        first.orTimeout(10, TimeUnit.SECONDS).join();
        second.orTimeout(10, TimeUnit.SECONDS).join();

        List<AuditEvent> rows = auditOf(id);
        assertThat(rows).extracting(r -> r.action() + ":" + title(r, "after"))
                .containsExactly("CREATED:Contended", "RENAMED:first", "RENAMED:second");
        // The second writer read the row only after the first committed, so its before-snapshot is
        // the first writer's result. Without the row lock it would have read the stale "Contended"
        // and written over the first writer's change.
        assertThat(title(rows.get(2), "before")).isEqualTo("first");
        drain();
        assertThat(readTopic().stream().filter(r -> ("case:" + id).equals(r.key()))
                .map(r -> mapper.fromJson(r.value()).after().get("title").asText()))
                .containsExactly("Contended", "first", "second");
    }

    @Test
    void aLaterEventWaitsWhileAnEarlierEventOfTheSameEntityIsWaitingOrFailed() {
        Case c = create("Held back");
        caseService.close(c.id()).block(TIMEOUT);
        List<AuditEvent> rows = auditOf(c.id());
        long first = rows.get(0).id();
        long second = rows.get(1).id();

        // As if the first event had just failed once and is waiting for its retry.
        sql("UPDATE audit_event SET attempts = 1, next_attempt_at = :t WHERE id = :id",
                Map.of("t", Instant.now().plus(Duration.ofHours(1)), "id", first));
        drain();
        assertThat(auditRepository.findById(second).block(TIMEOUT).status()).isEqualTo(OutboxStatus.PENDING);

        // Gave up for good: the later event still waits, for a person to act.
        sql("UPDATE audit_event SET status = 'FAILED' WHERE id = :id", Map.of("id", first));
        drain();
        assertThat(auditRepository.findById(second).block(TIMEOUT).status()).isEqualTo(OutboxStatus.PENDING);

        // Once the first is published, the second goes.
        sql("UPDATE audit_event SET status = 'PUBLISHED', published_at = now() WHERE id = :id", Map.of("id", first));
        drain();
        assertThat(auditRepository.findById(second).block(TIMEOUT).status()).isEqualTo(OutboxStatus.PUBLISHED);
    }

    private String title(AuditEvent row, String side) {
        var message = mapper.fromJson(mapper.toJson(row));
        return ("before".equals(side) ? message.before() : message.after()).get("title").asText();
    }

    private void sql(String statement, Map<String, Object> params) {
        DatabaseClient.GenericExecuteSpec spec = db.sql(statement);
        for (Map.Entry<String, Object> p : params.entrySet()) {
            spec = spec.bind(p.getKey(), p.getValue());
        }
        spec.then().block(TIMEOUT);
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

    private List<ConsumerRecord<String, String>> readTopic() {
        return readTopic(TOPIC);
    }

    /** Every record currently on {@code topic}, read with a throwaway consumer group. */
    private List<ConsumerRecord<String, String>> readTopic(String topic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafka().getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
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
        throw new AssertionError("Could not read " + topic + " to its end within 30 s");
    }

    /** Waits until the app's consumer group has committed past {@code offset} on {@code partition}. */
    private void awaitConsumerCommittedPast(TopicPartition partition, long offset) {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafka().getBootstrapServers());
        try (AdminClient admin = AdminClient.create(config)) {
            Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
                OffsetAndMetadata committed = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata().get().get(partition);
                assertThat(committed).as("committed offset of %s", partition).isNotNull();
                assertThat(committed.offset()).isGreaterThan(offset);
            });
        }
    }
}
