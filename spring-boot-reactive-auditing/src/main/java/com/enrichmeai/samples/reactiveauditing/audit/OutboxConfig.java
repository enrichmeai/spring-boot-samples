package com.enrichmeai.samples.reactiveauditing.audit;

import java.time.Clock;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

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
    NewTopic auditDeadLetterTopic(@Value("${audit.consumer.dead-letter-topic}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }
}
