package com.enrichmeai.samples.auditing.consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class ConsumerConfig {

    /**
     * Retry a failing record twice, one second apart, then log it and move on. In production, add
     * a DeadLetterPublishingRecoverer so a poison record is kept rather than only logged.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, 2));
    }
}
