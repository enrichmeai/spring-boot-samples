package com.enrichmeai.samples.auditing.audit;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** One row of {@code audit_event}: the audit record and its outbox state. */
@Entity
@Table(name = "audit_event")
public class AuditEvent {

    static final int MAX_ERROR_LENGTH = 2000;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true, updatable = false)
    private UUID eventId;

    @Column(name = "entity_type", nullable = false, updatable = false)
    private String entityType;

    @Column(name = "entity_id", nullable = false, updatable = false)
    private String entityId;

    @Column(nullable = false, updatable = false)
    private String action;

    @Column(nullable = false, updatable = false)
    private String actor;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "before_state", columnDefinition = "jsonb", updatable = false)
    private String beforeState;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "after_state", columnDefinition = "jsonb", updatable = false)
    private String afterState;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected AuditEvent() {
        // for JPA
    }

    public static AuditEvent pending(String entityType, String entityId, String action, String actor,
                                     String beforeJson, String afterJson, Instant now) {
        AuditEvent e = new AuditEvent();
        e.eventId = UUID.randomUUID();
        e.entityType = entityType;
        e.entityId = entityId;
        e.action = action;
        e.actor = actor;
        e.beforeState = beforeJson;
        e.afterState = afterJson;
        e.occurredAt = now;
        e.status = OutboxStatus.PENDING;
        e.nextAttemptAt = now;
        return e;
    }

    /** All events of one entity share a key, so Kafka puts them on one partition, in order. */
    public String partitionKey() {
        return entityType + ":" + entityId;
    }

    public void markPublished(Instant now) {
        status = OutboxStatus.PUBLISHED;
        publishedAt = now;
        lastError = null;
    }

    public void markAttemptFailed(Throwable error, Instant now, int maxAttempts, Duration maxBackoff) {
        attempts++;
        lastError = truncate(describe(error));
        if (attempts >= maxAttempts) {
            status = OutboxStatus.FAILED;
            return;
        }
        long backoffSeconds = Math.min(1L << Math.min(attempts, 30), maxBackoff.toSeconds());
        nextAttemptAt = now.plusSeconds(backoffSeconds);
    }

    /**
     * The broker could not be reached. Retried with backoff, but never counted towards FAILED: an
     * outage is not this event's fault, and FAILED would block its entity until a person acts.
     */
    public void markDeferred(Throwable error, Instant now, Duration maxBackoff) {
        attempts++;
        lastError = truncate(describe(error));
        long backoffSeconds = Math.min(1L << Math.min(attempts, 30), maxBackoff.toSeconds());
        nextAttemptAt = now.plusSeconds(backoffSeconds);
    }

    static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getName() + ": " + root.getMessage();
    }

    static String truncate(String s) {
        return s == null || s.length() <= MAX_ERROR_LENGTH ? s : s.substring(0, MAX_ERROR_LENGTH);
    }

    public Long getId() { return id; }
    public UUID getEventId() { return eventId; }
    public String getEntityType() { return entityType; }
    public String getEntityId() { return entityId; }
    public String getAction() { return action; }
    public String getActor() { return actor; }
    public String getBeforeState() { return beforeState; }
    public String getAfterState() { return afterState; }
    public Instant getOccurredAt() { return occurredAt; }
    public OutboxStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public String getLastError() { return lastError; }
    public Instant getPublishedAt() { return publishedAt; }
}
