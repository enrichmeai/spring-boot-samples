package com.enrichmeai.samples.reactiveauditing.audit;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("audit.outbox")
public record OutboxProperties(
        String topic,
        boolean pollerEnabled,
        Duration pollInterval,
        int batchSize,
        Duration sendTimeout,
        int maxAttempts,
        Duration maxBackoff) {

    public OutboxProperties {
        if (batchSize < 1) {
            throw new IllegalArgumentException("audit.outbox.batch-size must be at least 1");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("audit.outbox.max-attempts must be at least 1");
        }
        if (sendTimeout == null || sendTimeout.isZero() || sendTimeout.isNegative()) {
            throw new IllegalArgumentException("audit.outbox.send-timeout must be positive");
        }
    }
}
