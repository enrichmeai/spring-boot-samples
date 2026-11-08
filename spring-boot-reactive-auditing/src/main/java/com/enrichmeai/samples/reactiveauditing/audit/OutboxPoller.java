package com.enrichmeai.samples.reactiveauditing.audit;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Copies PENDING audit rows to Kafka. One transaction per batch (the caller supplies it): take the
 * poller lock, lock the rows, send each one in order, write its new state, commit. The batch stops
 * at the first sign Kafka is unreachable, so an outage costs one send timeout per poll, not one per
 * row.
 *
 * <p>Delivery is at-least-once. If a send succeeds and the commit then fails, or the batch is
 * cancelled, the row is still PENDING and is sent again, so consumers must skip a repeated
 * {@code event_id}.
 */
@Component
public class OutboxPoller {

    public static final String HEADER_EVENT_ID = "event_id";
    public static final String HEADER_ACTION = "action";
    /** Any constant shared by every instance of this app; it names the advisory lock. */
    public static final long POLLER_LOCK_KEY = 0x6175646974L; // "audit"

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final AuditEventRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxProperties props;
    private final AuditMessageMapper mapper;
    private final Clock clock;

    public OutboxPoller(AuditEventRepository repository, KafkaTemplate<String, String> kafka,
                        OutboxProperties props, AuditMessageMapper mapper, Clock clock) {
        this.repository = repository;
        this.kafka = kafka;
        this.props = props;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** Publishes one batch. Must be subscribed inside a transaction. */
    public Mono<BatchResult> publishBatch() {
        return repository.tryPollerLock(POLLER_LOCK_KEY).flatMap(locked -> {
            if (!locked) {
                return Mono.just(BatchResult.lockHeldElsewhere());
            }
            Instant now = clock.instant();
            return repository.lockNextBatch(now, props.batchSize()).collectList()
                    .flatMap(batch -> sendInOrder(batch, now));
        });
    }

    private Mono<BatchResult> sendInOrder(List<AuditEvent> batch, Instant now) {
        Set<String> blockedKeys = ConcurrentHashMap.newKeySet();
        AtomicBoolean brokerUnavailable = new AtomicBoolean();
        AtomicInteger published = new AtomicInteger();

        // concatMap: one record at a time, in id order.
        return Flux.fromIterable(batch)
                .concatMap(event -> {
                    // A failed event holds back the later events of its entity in this batch; the
                    // query holds them back in later batches until it is retried.
                    if (brokerUnavailable.get() || blockedKeys.contains(event.partitionKey())) {
                        return Mono.empty();
                    }
                    return send(event)
                            .then(Mono.fromCallable(() -> event.published(now)))
                            .doOnNext(e -> published.incrementAndGet())
                            .onErrorResume(error -> Mono.just(afterFailure(event, error, now, blockedKeys, brokerUnavailable)))
                            .flatMap(repository::save); // outside onErrorResume: a DB failure fails the batch
                })
                .then(Mono.fromCallable(() ->
                        new BatchResult(true, batch.size(), published.get(), brokerUnavailable.get())));
    }

    /**
     * KafkaTemplate.send() can block the calling thread for up to max.block.ms while it fetches
     * metadata, so it is called on boundedElastic, never on an event-loop thread.
     */
    private Mono<Void> send(AuditEvent event) {
        return Mono.fromFuture(() -> kafka.send(toRecord(event)))
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(props.sendTimeout())
                .then();
    }

    private AuditEvent afterFailure(AuditEvent event, Throwable error, Instant now,
                                    Set<String> blockedKeys, AtomicBoolean brokerUnavailable) {
        if (isBrokerUnavailable(error)) {
            // Every later send would wait out the same timeout. Stop, and do not count the outage
            // against this event's attempt budget.
            brokerUnavailable.set(true);
            AuditEvent deferred = event.deferred(error, now, props.maxBackoff());
            log.warn("Kafka unreachable; audit outbox paused until {}: {}", deferred.nextAttemptAt(), deferred.lastError());
            return deferred;
        }
        blockedKeys.add(event.partitionKey());
        AuditEvent failed = event.failedAttempt(error, now, props.maxAttempts(), props.maxBackoff());
        if (failed.status() == OutboxStatus.FAILED) {
            log.error("Audit event {} ({}) gave up after {} attempts: {}", failed.eventId(), failed.partitionKey(),
                    failed.attempts(), failed.lastError());
        } else {
            log.warn("Audit event {} ({}) publish failed, attempt {}, next try at {}: {}", failed.eventId(),
                    failed.partitionKey(), failed.attempts(), failed.nextAttemptAt(), failed.lastError());
        }
        return failed;
    }

    /**
     * Retriable Kafka errors (timeouts, no metadata, no leader) and our own wait running out mean
     * the broker could not take the record. Anything else (too large, not serialisable) is this
     * record's problem and counts towards FAILED.
     */
    static boolean isBrokerUnavailable(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof RetriableException || t instanceof TimeoutException) {
                return true;
            }
        }
        return false;
    }

    private ProducerRecord<String, String> toRecord(AuditEvent e) {
        ProducerRecord<String, String> record = new ProducerRecord<>(props.topic(), e.partitionKey(), mapper.toJson(e));
        record.headers().add(HEADER_EVENT_ID, e.eventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(HEADER_ACTION, e.action().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
