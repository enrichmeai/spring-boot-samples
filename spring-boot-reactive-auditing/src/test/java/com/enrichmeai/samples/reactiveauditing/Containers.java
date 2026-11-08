package com.enrichmeai.samples.reactiveauditing;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** One Postgres and one Kafka for the whole test run, started on first use. */
public final class Containers {

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private Containers() {
    }

    public static synchronized PostgreSQLContainer<?> postgres() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
        return POSTGRES;
    }

    public static synchronized KafkaContainer kafka() {
        if (!KAFKA.isRunning()) {
            KAFKA.start();
        }
        return KAFKA;
    }
}
