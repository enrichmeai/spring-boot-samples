package com.enrichmeai.samples.auditing.audit;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Copies PENDING audit rows to Kafka. One transaction per batch: take the poller lock, lock the
 * rows, send each one, mark it, commit. The batch stops at the first sign Kafka is unreachable, so
 * an outage costs one send timeout per poll, not one per row.
 *
 * <p>Delivery is at-least-once. If a send succeeds and the commit then fails, the row is still
 * PENDING and is sent again, so consumers must skip a repeated {@code event_id}.
 */
@Component
public class OutboxPoller {

    public static final String HEADER_EVENT_ID = "event_id";
    public static final String HEADER_ACTION = "action";
    /** Any constant shared by every instance of this app; it names the advisory lock. */
    static final long POLLER_LOCK_KEY = 0x6175646974L; // "audit"

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final AuditEventRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final OutboxProperties props;
    private final AuditMessageMapper mapper;
    private final Clock clock;

    public OutboxPoller(AuditEventRepository repository, KafkaTemplate<String, String> kafka,
                        TransactionTemplate outboxTransactionTemplate, OutboxProperties props,
                        AuditMessageMapper mapper, Clock clock) {
        this.repository = repository;
        this.kafka = kafka;
        this.tx = outboxTransactionTemplate;
        this.props = props;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** Runs only when scheduling is enabled (audit.outbox.poller-enabled, see OutboxConfig). */
    @Scheduled(fixedDelayString = "${audit.outbox.poll-interval}")
    public void poll() {
        BatchResult result;
        do {
            result = tx.execute(status -> publishBatch());
        } while (result != null
                && result.lockAcquired()
                && !result.brokerUnavailable()
                && result.taken() == props.batchSize()
                && !Thread.currentThread().isInterrupted());
    }

    /** Publishes one batch. Must run inside a transaction. */
    public BatchResult publishBatch() {
        if (!repository.tryPollerLock(POLLER_LOCK_KEY)) {
            return BatchResult.lockHeldElsewhere();
        }
        Instant now = clock.instant();
        List<AuditEvent> batch = repository.lockNextBatch(now, props.batchSize());
        Set<String> blockedKeys = new HashSet<>();
        int published = 0;
        boolean brokerUnavailable = false;

        for (AuditEvent event : batch) {
            // A failed event holds back the later events of its entity in this batch; the query
            // holds them back in later batches until it is retried.
            if (blockedKeys.contains(event.partitionKey())) {
                continue;
            }
            try {
                kafka.send(toRecord(event)).get(props.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
                event.markPublished(now);
                published++;
            } catch (InterruptedException ie) {
                // Shutting down: not the event's fault, so it is left exactly as it was.
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                if (isBrokerUnavailable(ex)) {
                    // Every later send would wait out the same timeout. Stop, and do not count
                    // the outage against this event's attempt budget.
                    event.markDeferred(ex, now, props.maxBackoff());
                    brokerUnavailable = true;
                    log.warn("Kafka unreachable; audit outbox paused until {}: {}",
                            event.getNextAttemptAt(), event.getLastError());
                    break;
                }
                blockedKeys.add(event.partitionKey());
                event.markAttemptFailed(ex, now, props.maxAttempts(), props.maxBackoff());
                if (event.getStatus() == OutboxStatus.FAILED) {
                    log.error("Audit event {} ({}) gave up after {} attempts: {}", event.getEventId(),
                            event.partitionKey(), event.getAttempts(), event.getLastError());
                } else {
                    log.warn("Audit event {} ({}) publish failed, attempt {}, next try at {}: {}",
                            event.getEventId(), event.partitionKey(), event.getAttempts(),
                            event.getNextAttemptAt(), event.getLastError());
                }
            }
        }
        // The rows are managed entities: their new status is flushed when the transaction commits.
        return new BatchResult(true, batch.size(), published, brokerUnavailable);
    }

    /**
     * Retriable Kafka errors (timeouts, no metadata, no leader) and our own wait running out mean
     * the broker could not take the record. Anything else (too large, not serialisable) is this
     * record's problem and counts towards FAILED.
     */
    static boolean isBrokerUnavailable(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof RetriableException || t instanceof java.util.concurrent.TimeoutException) {
                return true;
            }
        }
        return false;
    }

    private ProducerRecord<String, String> toRecord(AuditEvent e) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(props.topic(), e.partitionKey(), mapper.toJson(e));
        record.headers().add(HEADER_EVENT_ID, e.getEventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(HEADER_ACTION, e.getAction().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
