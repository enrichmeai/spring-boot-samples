package com.enrichmeai.samples.auditing.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.transaction.support.TransactionTemplate;

/** The poller's decisions, without a database or a broker. */
class OutboxPollerTest {

    private static final Instant NOW = Instant.parse("2026-11-01T10:00:00Z");

    private AuditEventRepository repository;
    private KafkaTemplate<String, String> kafka;
    private OutboxPoller poller;
    private final List<String> sentKeys = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(AuditEventRepository.class);
        kafka = mock(KafkaTemplate.class);
        OutboxProperties props = new OutboxProperties("audit-events", false, Duration.ofSeconds(1), 20,
                Duration.ofSeconds(1), 3, Duration.ofMinutes(5));
        AuditMessageMapper mapper = new AuditMessageMapper(new ObjectMapper().registerModule(new JavaTimeModule()));
        poller = new OutboxPoller(repository, kafka, mock(TransactionTemplate.class), props, mapper,
                Clock.fixed(NOW, ZoneOffset.UTC));
        when(repository.tryPollerLock(anyLong())).thenReturn(true);
    }

    private static AuditEvent event(String entityId, String action) {
        return AuditEvent.pending("case", entityId, action, "alice", null, "{}", NOW);
    }

    /** Sends for entity "bad" fail with {@code error}; everything else succeeds. Records every key sent. */
    private void sendFailsFor(String badEntityId, Throwable error) {
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            ProducerRecord<String, String> record = inv.getArgument(0);
            sentKeys.add(record.key());
            if (record.key().equals("case:" + badEntityId)) {
                return CompletableFuture.failedFuture(error);
            }
            return CompletableFuture.completedFuture(mock(SendResult.class));
        });
    }

    /** A record-specific, non-retriable failure (e.g. the record is too large). */
    private void brokerFailsFor(String badEntityId) {
        sendFailsFor(badEntityId, new RecordTooLargeException("broker said no"));
    }

    @Test
    void doesNothingWhenAnotherInstanceHoldsThePollerLock() {
        when(repository.tryPollerLock(anyLong())).thenReturn(false);

        BatchResult result = poller.publishBatch();

        assertThat(result.lockAcquired()).isFalse();
        assertThat(result.taken()).isZero();

        verify(repository, never()).lockNextBatch(any(), anyInt());
        verify(kafka, never()).send(any(ProducerRecord.class));
    }

    @Test
    void marksSentEventsPublished() {
        AuditEvent a = event("1", "CREATED");
        AuditEvent b = event("2", "CREATED");
        when(repository.lockNextBatch(NOW, 20)).thenReturn(List.of(a, b));
        brokerFailsFor("none");

        BatchResult result = poller.publishBatch();

        assertThat(result).isEqualTo(new BatchResult(true, 2, 2, false));

        assertThat(a.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(b.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
    }

    @Test
    void aFailedEventHoldsBackLaterEventsOfTheSameEntityOnly() {
        AuditEvent bad1 = event("bad", "CREATED");
        AuditEvent other = event("good", "CREATED");
        AuditEvent bad2 = event("bad", "CLOSED");
        when(repository.lockNextBatch(NOW, 20)).thenReturn(List.of(bad1, other, bad2));
        brokerFailsFor("bad");

        poller.publishBatch();

        assertThat(sentKeys).containsExactly("case:bad", "case:good"); // bad2 was never sent
        assertThat(bad1.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(bad1.getAttempts()).isEqualTo(1);
        assertThat(bad1.getNextAttemptAt()).isAfter(NOW);
        assertThat(bad1.getLastError()).contains("broker said no");
        assertThat(other.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(bad2.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(bad2.getAttempts()).isZero();
        verify(kafka, times(2)).send(any(ProducerRecord.class));
    }

    @Test
    void aRecordSpecificFailureCountsTowardsFailed() {
        AuditEvent bad = event("bad", "CREATED");
        when(repository.lockNextBatch(NOW, 20)).thenReturn(List.of(bad));
        brokerFailsFor("bad");

        for (int i = 0; i < 3; i++) {
            poller.publishBatch();
        }

        assertThat(bad.getStatus()).isEqualTo(OutboxStatus.FAILED); // max-attempts is 3 here
    }

    @Test
    void anUnreachableBrokerStopsTheBatchAndNeverMakesAnEventFailed() {
        AuditEvent first = event("down", "CREATED");
        AuditEvent other = event("good", "CREATED");
        when(repository.lockNextBatch(NOW, 20)).thenReturn(List.of(first, other));
        sendFailsFor("down", new TimeoutException("Topic audit-events not present in metadata after 3000 ms."));

        BatchResult result = null;
        for (int i = 0; i < 10; i++) { // far past max-attempts (3)
            result = poller.publishBatch();
        }

        assertThat(result.brokerUnavailable()).isTrue();
        assertThat(result.published()).isZero();
        assertThat(sentKeys).containsOnly("case:down"); // the batch stopped every time
        assertThat(first.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(first.getAttempts()).isEqualTo(10);
        assertThat(first.getNextAttemptAt()).isAfter(NOW);
        assertThat(other.getAttempts()).isZero();
    }

    @Test
    void ourOwnSendTimeoutCountsAsBrokerUnavailable() {
        assertThat(OutboxPoller.isBrokerUnavailable(
                new java.util.concurrent.ExecutionException(new TimeoutException("expired")))).isTrue();
        assertThat(OutboxPoller.isBrokerUnavailable(new java.util.concurrent.TimeoutException())).isTrue();
        assertThat(OutboxPoller.isBrokerUnavailable(
                new java.util.concurrent.ExecutionException(new RecordTooLargeException("big")))).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void anInterruptLeavesTheEventUntouched() {
        AuditEvent e = event("1", "CREATED");
        when(repository.lockNextBatch(NOW, 20)).thenReturn(List.of(e));
        CompletableFuture<SendResult<String, String>> interrupted = new CompletableFuture<>() {
            @Override
            public SendResult<String, String> get(long timeout, java.util.concurrent.TimeUnit unit)
                    throws InterruptedException {
                throw new InterruptedException("shutting down");
            }
        };
        when(kafka.send(any(ProducerRecord.class))).thenReturn(interrupted);

        try {
            poller.publishBatch();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted(); // clear it for the next test
        }
        assertThat(e.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(e.getAttempts()).isZero();
        assertThat(e.getLastError()).isNull();
    }
}
