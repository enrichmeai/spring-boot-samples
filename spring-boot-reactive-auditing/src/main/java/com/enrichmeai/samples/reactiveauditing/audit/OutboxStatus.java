package com.enrichmeai.samples.reactiveauditing.audit;

public enum OutboxStatus {
    /** Written, not yet on Kafka. */
    PENDING,
    /** On Kafka. The row stays: it is the audit record. */
    PUBLISHED,
    /** Gave up after max-attempts. Blocks later events for the same entity until someone acts. */
    FAILED
}
