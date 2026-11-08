package com.enrichmeai.samples.reactiveauditing.audit;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * One row of {@code audit_event}, read by the poller. Immutable: each state change is computed
 * here and written by {@link AuditEventRepository}.
 */
public record AuditEvent(
        long id,
        UUID eventId,
        String entityType,
        String entityId,
        String action,
        String actor,
        String beforeState,
        String afterState,
        Instant occurredAt,
        OutboxStatus status,
        int attempts,
        Instant nextAttemptAt,
        String lastError,
        Instant publishedAt) {

    static final int MAX_ERROR_LENGTH = 2000;

    /** All events of one entity share a key, so Kafka puts them on one partition, in order. */
    public String partitionKey() {
        return entityType + ":" + entityId;
    }

    public AuditEvent published(Instant now) {
        return with(OutboxStatus.PUBLISHED, attempts, nextAttemptAt, null, now);
    }

    /** A failure that is this record's own (too large, not serialisable): counts towards FAILED. */
    public AuditEvent failedAttempt(Throwable error, Instant now, int maxAttempts, Duration maxBackoff) {
        int n = attempts + 1;
        OutboxStatus next = n >= maxAttempts ? OutboxStatus.FAILED : OutboxStatus.PENDING;
        return with(next, n, now.plusSeconds(backoffSeconds(n, maxBackoff)), truncate(describe(error)), null);
    }

    /**
     * The broker could not be reached. Retried with backoff, but never counted towards FAILED: an
     * outage is not this event's fault, and FAILED would block its entity until a person acts.
     */
    public AuditEvent deferred(Throwable error, Instant now, Duration maxBackoff) {
        int n = attempts + 1;
        return with(OutboxStatus.PENDING, n, now.plusSeconds(backoffSeconds(n, maxBackoff)),
                truncate(describe(error)), null);
    }

    static long backoffSeconds(int attempts, Duration maxBackoff) {
        return Math.min(1L << Math.min(attempts, 30), maxBackoff.toSeconds());
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

    private AuditEvent with(OutboxStatus newStatus, int newAttempts, Instant newNextAttemptAt,
                            String newLastError, Instant newPublishedAt) {
        return new AuditEvent(id, eventId, entityType, entityId, action, actor, beforeState, afterState,
                occurredAt, newStatus, newAttempts, newNextAttemptAt, newLastError, newPublishedAt);
    }
}
