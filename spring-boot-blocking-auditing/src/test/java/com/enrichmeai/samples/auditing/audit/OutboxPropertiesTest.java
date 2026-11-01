package com.enrichmeai.samples.auditing.audit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class OutboxPropertiesTest {

    private static OutboxProperties props(int batch, Duration send, int maxAttempts) {
        return new OutboxProperties("t", true, Duration.ofSeconds(1), batch, send, maxAttempts, Duration.ofMinutes(5));
    }

    @Test
    void refusesAnEmptyBatch() {
        assertThatThrownBy(() -> props(0, Duration.ofSeconds(9), 10)).hasMessageContaining("batch-size");
    }

    @Test
    void refusesZeroAttempts() {
        assertThatThrownBy(() -> props(20, Duration.ofSeconds(9), 0)).hasMessageContaining("max-attempts");
    }

    @Test
    void refusesANonPositiveSendTimeout() {
        assertThatThrownBy(() -> props(20, Duration.ZERO, 10)).hasMessageContaining("send-timeout");
    }

    @Test
    void acceptsTheShippedDefaults() {
        assertThatCode(() -> props(20, Duration.ofSeconds(9), 10)).doesNotThrowAnyException();
    }
}
