package com.enrichmeai.samples.auditing.audit;

import java.time.Clock;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
class OutboxConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    NewTopic auditTopic(OutboxProperties props) {
        // Replicas: 3 in production.
        return TopicBuilder.name(props.topic()).partitions(3).replicas(1).build();
    }

    @Bean
    TransactionTemplate outboxTransactionTemplate(PlatformTransactionManager transactionManager) {
        // No transaction timeout: with JPA, Spring applies it only as a timeout on queries, so it
        // would not bound the time spent waiting on Kafka. The batch bounds that itself: it stops
        // at the first sign the broker is unreachable.
        return new TransactionTemplate(transactionManager);
    }

    /** Switched off in tests that drive publishBatch() themselves. */
    @Configuration
    @EnableScheduling
    @ConditionalOnProperty(name = "audit.outbox.poller-enabled", havingValue = "true", matchIfMissing = true)
    static class SchedulingConfig {
    }
}
