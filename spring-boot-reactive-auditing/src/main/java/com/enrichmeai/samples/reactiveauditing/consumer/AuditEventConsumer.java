package com.enrichmeai.samples.reactiveauditing.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import com.enrichmeai.samples.reactiveauditing.audit.AuditMessage;
import com.enrichmeai.samples.reactiveauditing.audit.AuditMessageMapper;
import com.enrichmeai.samples.reactiveauditing.audit.OutboxPoller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * Reads the audit stream. Delivery is at-least-once, so it records each event_id and skips one it
 * has seen; the marker and the side effects commit in one R2DBC transaction.
 *
 * <p>The method returns a Mono. spring-kafka runs it asynchronously and acknowledges the record
 * when the Mono completes, so the consumer thread never waits on the database.
 *
 * <p><b>The Mono must not fail.</b> In spring-kafka 3.3.16 a failed async listener is acknowledged
 * and only logged (MessagingMessageListenerAdapter#asyncFailure), so a failure here would drop the
 * record for good. Instead: a transient failure is retried with backoff; a malformed record, or one
 * that still fails after the retries, is published to the dead-letter topic, and only then
 * acknowledged.
 */
@Component
class AuditEventConsumer {

    static final String HEADER_ERROR = "dlt-error";
    static final String HEADER_SOURCE = "dlt-source";

    private static final Logger log = LoggerFactory.getLogger(AuditEventConsumer.class);

    private final ProcessedEventRepository processed;
    private final EntityActivityRepository activity;
    private final AuditMessageMapper mapper;
    private final TransactionalOperator tx;
    private final KafkaTemplate<String, String> kafka;
    private final String deadLetterTopic;
    private final Retry retry;

    AuditEventConsumer(ProcessedEventRepository processed, EntityActivityRepository activity, AuditMessageMapper mapper,
                       TransactionalOperator transactionalOperator, KafkaTemplate<String, String> kafka,
                       @Value("${audit.consumer.dead-letter-topic}") String deadLetterTopic,
                       @Value("${audit.consumer.retries:3}") int retries,
                       @Value("${audit.consumer.retry-backoff:200ms}") Duration retryBackoff) {
        this.processed = processed;
        this.activity = activity;
        this.mapper = mapper;
        this.tx = transactionalOperator;
        this.kafka = kafka;
        this.deadLetterTopic = deadLetterTopic;
        this.retry = Retry.backoff(retries, retryBackoff)
                .filter(error -> !(error instanceof MalformedRecordException));
    }

    @KafkaListener(topics = "${audit.outbox.topic}", groupId = "audit-projection")
    public Mono<Void> on(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> tx.transactional(apply(record))) // each retry is a fresh transaction
                .retryWhen(retry)
                .onErrorResume(error -> deadLetter(record, error));
    }

    private Mono<Void> apply(ConsumerRecord<String, String> record) {
        UUID eventId = eventId(record);
        AuditMessage message = parse(record);
        return processed.markProcessed(eventId).flatMap(inserted -> {
            if (inserted == 0) {
                log.debug("Skipped duplicate audit event {}", eventId);
                return Mono.empty();
            }
            return activity.recordEvent(message.entityType() + ":" + message.entityId(), message.action());
        });
    }

    /** If this fails too, the Mono fails and spring-kafka logs it: the one case nothing can keep. */
    private Mono<Void> deadLetter(ConsumerRecord<String, String> record, Throwable error) {
        Throwable cause = Exceptions.unwrapRetryExhausted(error);
        log.error("Audit record {}-{}@{} sent to {}: {}", record.topic(), record.partition(), record.offset(),
                deadLetterTopic, cause.toString());
        ProducerRecord<String, String> dead = new ProducerRecord<>(deadLetterTopic, null, record.key(), record.value(),
                record.headers());
        dead.headers().add(HEADER_ERROR, cause.toString().getBytes(StandardCharsets.UTF_8));
        dead.headers().add(HEADER_SOURCE,
                (record.topic() + "-" + record.partition() + "@" + record.offset()).getBytes(StandardCharsets.UTF_8));
        // send() can block for metadata: keep it off the consumer and event-loop threads.
        return Mono.fromFuture(() -> kafka.send(dead)).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private UUID eventId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(OutboxPoller.HEADER_EVENT_ID);
        if (header == null) {
            throw new MalformedRecordException("no event_id header");
        }
        try {
            return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            throw new MalformedRecordException("event_id header is not a UUID");
        }
    }

    private AuditMessage parse(ConsumerRecord<String, String> record) {
        try {
            return mapper.fromJson(record.value());
        } catch (IllegalArgumentException e) {
            throw new MalformedRecordException("value is not an audit message: " + e.getMessage());
        }
    }

    /** Retrying cannot fix it: straight to the dead-letter topic. */
    static final class MalformedRecordException extends RuntimeException {
        MalformedRecordException(String message) {
            super(message);
        }
    }

    private static final class Exceptions {
        static Throwable unwrapRetryExhausted(Throwable t) {
            return reactor.core.Exceptions.isRetryExhausted(t) && t.getCause() != null ? t.getCause() : t;
        }
    }
}
