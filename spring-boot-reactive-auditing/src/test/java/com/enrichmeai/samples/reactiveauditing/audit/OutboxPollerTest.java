package com.enrichmeai.samples.reactiveauditing.audit;

import static com.enrichmeai.samples.reactiveauditing.audit.AuditEventTest.NOW;
import static com.enrichmeai.samples.reactiveauditing.audit.AuditEventTest.pending;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/** The poller's decisions, without a database or a broker. */
class OutboxPollerTest {

    private AuditEventRepository repository;
    private KafkaTemplate<String, String> kafka;
    private OutboxPoller poller;
    private final List<String> sentKeys = new ArrayList<>();
    private final Map<Long, AuditEvent> saved = new LinkedHashMap<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(AuditEventRepository.class);
        kafka = mock(KafkaTemplate.class);
        OutboxProperties props = new OutboxProperties("audit-events", false, Duration.ofSeconds(1), 20,
                Duration.ofSeconds(2), 3, Duration.ofMinutes(5));
        AuditMessageMapper mapper = new AuditMessageMapper(new ObjectMapper().registerModule(new JavaTimeModule()));
        poller = new OutboxPoller(repository, kafka, props, mapper, Clock.fixed(NOW, ZoneOffset.UTC));
        when(repository.tryPollerLock(anyLong())).thenReturn(Mono.just(true));
        when(repository.save(any())).thenAnswer(inv -> {
            AuditEvent e = inv.getArgument(0);
            saved.put(e.id(), e);
            return Mono.empty();
        });
    }

    private void batch(AuditEvent... events) {
        when(repository.lockNextBatch(NOW, 20)).thenReturn(Flux.just(events));
    }

    /** Sends for entity {@code badEntityId} fail with {@code error}; others succeed. Records every key sent. */
    private void sendFailsFor(String badEntityId, Throwable error) {
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            ProducerRecord<String, String> record = inv.getArgument(0);
            sentKeys.add(record.key());
            return record.key().equals("case:" + badEntityId)
                    ? CompletableFuture.failedFuture(error)
                    : CompletableFuture.completedFuture(mock(SendResult.class));
        });
    }

    @Test
    void doesNothingWhenAnotherInstanceHoldsThePollerLock() {
        when(repository.tryPollerLock(anyLong())).thenReturn(Mono.just(false));

        StepVerifier.create(poller.publishBatch()).expectNext(new BatchResult(false, 0, 0, false)).verifyComplete();

        verify(repository, never()).lockNextBatch(any(), anyInt());
        verify(kafka, never()).send(any(ProducerRecord.class));
    }

    @Test
    void marksSentEventsPublished() {
        batch(pending(1, "1", "CREATED"), pending(2, "2", "CREATED"));
        sendFailsFor("none", new RuntimeException());

        StepVerifier.create(poller.publishBatch()).expectNext(new BatchResult(true, 2, 2, false)).verifyComplete();

        assertThat(saved.values()).extracting(AuditEvent::status).containsOnly(OutboxStatus.PUBLISHED);
    }

    @Test
    void aFailedEventHoldsBackLaterEventsOfTheSameEntityOnly() {
        batch(pending(1, "bad", "CREATED"), pending(2, "good", "CREATED"), pending(3, "bad", "CLOSED"));
        sendFailsFor("bad", new RecordTooLargeException("broker said no"));

        StepVerifier.create(poller.publishBatch()).expectNext(new BatchResult(true, 3, 1, false)).verifyComplete();

        assertThat(sentKeys).containsExactly("case:bad", "case:good"); // event 3 was never sent
        assertThat(saved.get(1L).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(saved.get(1L).attempts()).isEqualTo(1);
        assertThat(saved.get(1L).lastError()).contains("broker said no");
        assertThat(saved.get(2L).status()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(saved).doesNotContainKey(3L);
    }

    @Test
    void anUnreachableBrokerStopsTheBatchAndDefersInsteadOfFailing() {
        AuditEvent down = pending(1, "down", "CREATED").deferred(new RuntimeException(), NOW, Duration.ofMinutes(5));
        down = new AuditEvent(down.id(), down.eventId(), down.entityType(), down.entityId(), down.action(), down.actor(),
                null, "{}", NOW, OutboxStatus.PENDING, 9, NOW, null, null); // already 9 attempts; max is 3
        batch(down, pending(2, "good", "CREATED"));
        sendFailsFor("down", new TimeoutException("Topic audit-events not present in metadata after 3000 ms."));

        StepVerifier.create(poller.publishBatch()).expectNext(new BatchResult(true, 2, 0, true)).verifyComplete();

        assertThat(sentKeys).containsExactly("case:down"); // stopped at the first broker failure
        assertThat(saved.get(1L).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(saved.get(1L).attempts()).isEqualTo(10);
        assertThat(saved).doesNotContainKey(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void ourOwnSendTimeoutCountsAsBrokerUnavailable() {
        batch(pending(1, "slow", "CREATED"));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>()); // never completes

        StepVerifier.create(poller.publishBatch()).expectNext(new BatchResult(true, 1, 0, true)).verifyComplete();

        assertThat(saved.get(1L).status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(saved.get(1L).lastError()).contains("TimeoutException");
    }

    @Test
    void aRecordSpecificFailureCountsTowardsFailed() {
        AuditEvent twice = new AuditEvent(1, java.util.UUID.randomUUID(), "case", "bad", "CREATED", "alice", null, "{}",
                NOW, OutboxStatus.PENDING, 2, NOW, null, null);
        batch(twice);
        sendFailsFor("bad", new RecordTooLargeException("too big"));

        StepVerifier.create(poller.publishBatch()).expectNextCount(1).verifyComplete();

        assertThat(saved.get(1L).status()).isEqualTo(OutboxStatus.FAILED); // third attempt of 3
    }
}
