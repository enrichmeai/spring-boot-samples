package com.enrichmeai.samples.reactiveauditing.audit;

import java.time.Instant;
import java.util.UUID;

import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Readable;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * SQL for {@code audit_event}, through {@link DatabaseClient}: it joins the reactive transaction
 * in the subscriber's context, and it handles jsonb, FOR UPDATE and advisory locks directly.
 */
@Repository
public class AuditEventRepository {

    private final DatabaseClient db;

    public AuditEventRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Void> insertPending(UUID eventId, String entityType, String entityId, String action, String actor,
                                    String beforeJson, String afterJson, Instant now) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO audit_event (event_id, entity_type, entity_id, action, actor,
                                                 before_state, after_state, occurred_at, status, next_attempt_at)
                        VALUES (:eventId, :entityType, :entityId, :action, :actor,
                                :before, :after, :now, 'PENDING', :now)
                        """)
                .bind("eventId", eventId)
                .bind("entityType", entityType)
                .bind("entityId", entityId)
                .bind("action", action)
                .bind("actor", actor)
                .bind("now", now);
        spec = beforeJson == null ? spec.bindNull("before", Json.class) : spec.bind("before", Json.of(beforeJson));
        spec = afterJson == null ? spec.bindNull("after", Json.class) : spec.bind("after", Json.of(afterJson));
        return spec.then();
    }

    /**
     * Only one poller works at a time, across all instances: a transaction-scoped Postgres
     * advisory lock, released on commit or rollback. False when another instance holds it.
     */
    public Mono<Boolean> tryPollerLock(long key) {
        return db.sql("SELECT pg_try_advisory_xact_lock(:key) AS locked")
                .bind("key", key)
                .map(row -> row.get("locked", Boolean.class))
                .one();
    }

    /**
     * The next due rows, oldest first. A row is skipped while an earlier row of the same entity
     * is FAILED or waiting for a retry, so a later event never overtakes an earlier one.
     * Must run inside a transaction: FOR UPDATE holds the rows until it commits.
     */
    public Flux<AuditEvent> lockNextBatch(Instant now, int batchSize) {
        return db.sql("""
                        SELECT a.* FROM audit_event a
                         WHERE a.status = 'PENDING'
                           AND a.next_attempt_at <= :now
                           AND NOT EXISTS (
                                 SELECT 1 FROM audit_event b
                                  WHERE b.entity_type = a.entity_type
                                    AND b.entity_id = a.entity_id
                                    AND b.id < a.id
                                    AND (b.status = 'FAILED'
                                         OR (b.status = 'PENDING' AND b.next_attempt_at > :now)))
                         ORDER BY a.id
                         LIMIT :batchSize
                         FOR UPDATE SKIP LOCKED
                        """)
                .bind("now", now)
                .bind("batchSize", batchSize)
                .map(AuditEventRepository::toEvent)
                .all();
    }

    public Mono<Void> save(AuditEvent e) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        UPDATE audit_event
                           SET status = :status, attempts = :attempts, next_attempt_at = :nextAttemptAt,
                               last_error = :lastError, published_at = :publishedAt
                         WHERE id = :id
                        """)
                .bind("id", e.id())
                .bind("status", e.status().name())
                .bind("attempts", e.attempts())
                .bind("nextAttemptAt", e.nextAttemptAt());
        spec = e.lastError() == null ? spec.bindNull("lastError", String.class) : spec.bind("lastError", e.lastError());
        spec = e.publishedAt() == null ? spec.bindNull("publishedAt", Instant.class) : spec.bind("publishedAt", e.publishedAt());
        return spec.then();
    }

    public Flux<AuditEvent> findByEntity(String entityType, String entityId) {
        return db.sql("SELECT * FROM audit_event WHERE entity_type = :t AND entity_id = :i ORDER BY id")
                .bind("t", entityType)
                .bind("i", entityId)
                .map(AuditEventRepository::toEvent)
                .all();
    }

    public Mono<AuditEvent> findById(long id) {
        return db.sql("SELECT * FROM audit_event WHERE id = :id").bind("id", id)
                .map(AuditEventRepository::toEvent).one();
    }

    static AuditEvent toEvent(Readable row) {
        Json before = row.get("before_state", Json.class);
        Json after = row.get("after_state", Json.class);
        return new AuditEvent(
                row.get("id", Long.class),
                row.get("event_id", UUID.class),
                row.get("entity_type", String.class),
                row.get("entity_id", String.class),
                row.get("action", String.class),
                row.get("actor", String.class),
                before == null ? null : before.asString(),
                after == null ? null : after.asString(),
                row.get("occurred_at", Instant.class),
                OutboxStatus.valueOf(row.get("status", String.class)),
                row.get("attempts", Integer.class),
                row.get("next_attempt_at", Instant.class),
                row.get("last_error", String.class),
                row.get("published_at", Instant.class));
    }
}
