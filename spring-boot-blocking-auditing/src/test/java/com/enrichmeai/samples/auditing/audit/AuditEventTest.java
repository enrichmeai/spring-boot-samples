package com.enrichmeai.samples.auditing.audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

class AuditEventTest {

    private static final Instant NOW = Instant.parse("2026-11-01T10:00:00Z");

    private static AuditEvent pending() {
        return AuditEvent.pending("case", "42", "CLOSED", "alice", null, "{}", NOW);
    }

    @Test
    void newEventIsPendingAndDueNow() {
        AuditEvent e = pending();
        assertThat(e.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(e.getNextAttemptAt()).isEqualTo(NOW);
        assertThat(e.getEventId()).isNotNull();
        assertThat(e.partitionKey()).isEqualTo("case:42");
    }

    @Test
    void backoffDoublesAndIsCappedAtMaxBackoff() {
        AuditEvent e = pending();
        e.markAttemptFailed(new RuntimeException("x"), NOW, 20, Duration.ofSeconds(10));
        assertThat(e.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(2));
        e.markAttemptFailed(new RuntimeException("x"), NOW, 20, Duration.ofSeconds(10));
        assertThat(e.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(4));
        e.markAttemptFailed(new RuntimeException("x"), NOW, 20, Duration.ofSeconds(10));
        assertThat(e.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(8));
        e.markAttemptFailed(new RuntimeException("x"), NOW, 20, Duration.ofSeconds(10));
        assertThat(e.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(10));
        assertThat(e.getAttempts()).isEqualTo(4);
        assertThat(e.getStatus()).isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void becomesFailedAtMaxAttempts() {
        AuditEvent e = pending();
        e.markAttemptFailed(new RuntimeException("x"), NOW, 2, Duration.ofMinutes(5));
        assertThat(e.getStatus()).isEqualTo(OutboxStatus.PENDING);
        e.markAttemptFailed(new RuntimeException("x"), NOW, 2, Duration.ofMinutes(5));
        assertThat(e.getStatus()).isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    void lastErrorNamesTheRootCauseAndIsTruncated() {
        AuditEvent e = pending();
        String huge = "y".repeat(10_000);
        e.markAttemptFailed(new IllegalStateException("outer", new java.io.IOException(huge)),
                NOW, 5, Duration.ofMinutes(5));
        assertThat(e.getLastError()).startsWith("java.io.IOException: yyy");
        assertThat(e.getLastError()).hasSize(AuditEvent.MAX_ERROR_LENGTH);
    }

    @Test
    void publishingClearsTheLastError() {
        AuditEvent e = pending();
        e.markAttemptFailed(new RuntimeException("x"), NOW, 5, Duration.ofMinutes(5));
        e.markPublished(NOW);
        assertThat(e.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(e.getLastError()).isNull();
        assertThat(e.getPublishedAt()).isEqualTo(NOW);
    }
}
