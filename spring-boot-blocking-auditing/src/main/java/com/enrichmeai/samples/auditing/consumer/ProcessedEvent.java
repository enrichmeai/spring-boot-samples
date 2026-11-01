package com.enrichmeai.samples.auditing.consumer;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** The consumer's idempotency marker: one row per event_id already handled. */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
        // for JPA
    }

    public UUID getEventId() { return eventId; }
    public Instant getProcessedAt() { return processedAt; }
}
