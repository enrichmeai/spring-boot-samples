package com.enrichmeai.samples.auditing.audit;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/** The Kafka message value: the contract consumers read. */
public record AuditMessage(UUID eventId, String entityType, String entityId, String action, String actor,
                           Instant occurredAt, JsonNode before, JsonNode after) {
}
