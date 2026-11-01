package com.enrichmeai.samples.auditing.audit;

/**
 * What one call to {@link OutboxPoller#publishBatch()} did. "Another instance holds the lock" and
 * "nothing was due" are different answers, so they are different fields.
 *
 * @param lockAcquired      false when another instance was polling, so this one looked at nothing
 * @param taken             rows locked and attempted
 * @param published         rows sent and marked PUBLISHED
 * @param brokerUnavailable the batch stopped early because Kafka could not be reached
 */
public record BatchResult(boolean lockAcquired, int taken, int published, boolean brokerUnavailable) {

    static BatchResult lockHeldElsewhere() {
        return new BatchResult(false, 0, 0, false);
    }
}
