package com.enrichmeai.samples.auditing.audit;

/**
 * Published by a service, inside its transaction, for every change worth auditing.
 * {@code before} and {@code after} are plain snapshots (maps or records), never JPA entities.
 */
public record EntityChangedEvent(String entityType, String entityId, String action,
                                 Object before, Object after) {
}
