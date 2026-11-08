package com.enrichmeai.samples.reactiveauditing.consumer;

import java.util.UUID;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

import reactor.core.publisher.Mono;

@Repository
public class ProcessedEventRepository {

    private final DatabaseClient db;

    public ProcessedEventRepository(DatabaseClient db) {
        this.db = db;
    }

    /** @return 1 the first time this event_id is seen, 0 for a duplicate */
    public Mono<Long> markProcessed(UUID eventId) {
        return db.sql("INSERT INTO processed_event (event_id) VALUES (:id) ON CONFLICT DO NOTHING")
                .bind("id", eventId)
                .fetch().rowsUpdated();
    }
}
