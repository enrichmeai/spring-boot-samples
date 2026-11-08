package com.enrichmeai.samples.reactiveauditing.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.enrichmeai.samples.reactiveauditing.audit.AuditMessage;
import com.enrichmeai.samples.reactiveauditing.audit.AuditMessageMapper;
import com.enrichmeai.samples.reactiveauditing.audit.OutboxPoller;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/** The consumer never lets its Mono fail: it retries, then dead-letters. */
class AuditEventConsumerTest {

    private static final String DLT = "audit-events.DLT";

    private final ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule());
    private ProcessedEventRepository processed;
    private EntityActivityRepository activity;
    private KafkaTemplate<String, String> kafka;
    private AuditEventConsumer consumer;
    private final List<ProducerRecord<String, String>> deadLettered = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        processed = mock(ProcessedEventRepository.class);
        activity = mock(EntityActivityRepository.class);
        kafka = mock(KafkaTemplate.class);
        TransactionalOperator tx = mock(TransactionalOperator.class);
        when(tx.transactional(any(Mono.class))).thenAnswer(inv -> inv.getArgument(0));
        when(activity.recordEvent(anyString(), anyString())).thenReturn(Mono.empty());
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            deadLettered.add(inv.getArgument(0));
            return CompletableFuture.completedFuture(mock(SendResult.class));
        });
        consumer = new AuditEventConsumer(processed, activity, new AuditMessageMapper(json), tx, kafka, DLT,
                3, Duration.ofMillis(1));
    }

    private ConsumerRecord<String, String> record(boolean withHeader, String value) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("audit-events", 0, 7L, "case:1", value);
        if (withHeader) {
            r.headers().add(OutboxPoller.HEADER_EVENT_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        }
        return r;
    }

    private String message() throws Exception {
        return json.writeValueAsString(new AuditMessage(UUID.randomUUID(), "case", "1", "CREATED", "alice",
                Instant.parse("2026-11-08T10:00:00Z"), null, null));
    }

    @Test
    void aTransientFailureIsRetriedAndThenApplied() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        when(processed.markProcessed(any())).thenReturn(Mono.defer(() -> calls.incrementAndGet() <= 2
                ? Mono.error(new IllegalStateException("connection reset"))
                : Mono.just(1L)));

        StepVerifier.create(consumer.on(record(true, message()))).verifyComplete();

        assertThat(calls).hasValue(3);
        verify(activity, times(1)).recordEvent("case:1", "CREATED");
        assertThat(deadLettered).isEmpty();
    }

    @Test
    void aRecordThatKeepsFailingIsDeadLetteredNotDropped() throws Exception {
        when(processed.markProcessed(any())).thenReturn(Mono.error(new IllegalStateException("database down")));

        StepVerifier.create(consumer.on(record(true, message()))).verifyComplete();

        assertThat(deadLettered).singleElement().satisfies(d -> {
            assertThat(d.topic()).isEqualTo(DLT);
            assertThat(d.key()).isEqualTo("case:1");
            assertThat(new String(d.headers().lastHeader(AuditEventConsumer.HEADER_ERROR).value(), StandardCharsets.UTF_8))
                    .contains("database down");
            assertThat(new String(d.headers().lastHeader(AuditEventConsumer.HEADER_SOURCE).value(), StandardCharsets.UTF_8))
                    .isEqualTo("audit-events-0@7");
        });
        verify(processed, times(4)).markProcessed(any()); // 1 try + 3 retries
    }

    @Test
    void aMalformedRecordGoesStraightToTheDeadLetterTopic() throws Exception {
        StepVerifier.create(consumer.on(record(false, message()))).verifyComplete();

        verify(processed, never()).markProcessed(any());
        assertThat(deadLettered).singleElement().satisfies(d ->
                assertThat(new String(d.headers().lastHeader(AuditEventConsumer.HEADER_ERROR).value(), StandardCharsets.UTF_8))
                        .contains("no event_id header"));
    }

    @Test
    void anUnparseableValueGoesStraightToTheDeadLetterTopic() {
        StepVerifier.create(consumer.on(record(true, "not json"))).verifyComplete();

        verify(processed, never()).markProcessed(any());
        assertThat(deadLettered).hasSize(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void ifEvenTheDeadLetterSendFailsTheMonoFails() {
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        StepVerifier.create(consumer.on(record(false, "x"))).expectError(IllegalStateException.class).verify();
    }
}
