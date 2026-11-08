package com.enrichmeai.samples.reactiveauditing.audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class AuditEventTest {

    static final Instant NOW = Instant.parse("2026-11-08T10:00:00Z");

    static AuditEvent pending(long id, String entityId, String action) {
        return new AuditEvent(id, UUID.randomUUID(), "case", entityId, action, "alice", null, "{}", NOW,
                OutboxStatus.PENDING, 0, NOW, null, null);
    }

    @Test
    void partitionKeyIsTheEntity() {
        assertThat(pending(1, "42", "CLOSED").partitionKey()).isEqualTo("case:42");
    }

    @Test
    void backoffDoublesAndIsCapped() {
        AuditEvent e = pending(1, "1", "CREATED");
        Duration cap = Duration.ofSeconds(10);
        e = e.failedAttempt(new RuntimeException("x"), NOW, 20, cap);
        assertThat(e.nextAttemptAt()).isEqualTo(NOW.plusSeconds(2));
        e = e.failedAttempt(new RuntimeException("x"), NOW, 20, cap);
        assertThat(e.nextAttemptAt()).isEqualTo(NOW.plusSeconds(4));
        e = e.failedAttempt(new RuntimeException("x"), NOW, 20, cap);
        e = e.failedAttempt(new RuntimeException("x"), NOW, 20, cap);
        assertThat(e.nextAttemptAt()).isEqualTo(NOW.plusSeconds(10));
        assertThat(e.attempts()).isEqualTo(4);
    }

    @Test
    void recordFailuresBecomeFailedAtMaxAttempts() {
        AuditEvent e = pending(1, "1", "CREATED").failedAttempt(new RuntimeException("x"), NOW, 2, Duration.ofMinutes(5));
        assertThat(e.status()).isEqualTo(OutboxStatus.PENDING);
        e = e.failedAttempt(new RuntimeException("x"), NOW, 2, Duration.ofMinutes(5));
        assertThat(e.status()).isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    void deferralsNeverBecomeFailed() {
        AuditEvent e = pending(1, "1", "CREATED");
        for (int i = 0; i < 50; i++) {
            e = e.deferred(new RuntimeException("down"), NOW, Duration.ofMinutes(5));
        }
        assertThat(e.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(e.attempts()).isEqualTo(50);
        assertThat(e.nextAttemptAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    @Test
    void lastErrorNamesTheRootCauseAndIsTruncated() {
        AuditEvent e = pending(1, "1", "CREATED").failedAttempt(
                new IllegalStateException("outer", new java.io.IOException("y".repeat(10_000))), NOW, 5, Duration.ofMinutes(5));
        assertThat(e.lastError()).startsWith("java.io.IOException: yyy").hasSize(AuditEvent.MAX_ERROR_LENGTH);
    }

    @Test
    void publishingClearsTheLastError() {
        AuditEvent e = pending(1, "1", "CREATED").failedAttempt(new RuntimeException("x"), NOW, 5, Duration.ofMinutes(5))
                .published(NOW);
        assertThat(e.status()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(e.lastError()).isNull();
        assertThat(e.publishedAt()).isEqualTo(NOW);
    }
}
