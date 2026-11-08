package com.enrichmeai.samples.reactiveauditing.consumer;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

import reactor.core.publisher.Mono;

/** An example read model fed by the audit stream: how many events each entity has had. */
@Repository
public class EntityActivityRepository {

    public record EntityActivity(String entityKey, long eventCount, String lastAction) {}

    private final DatabaseClient db;

    public EntityActivityRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Void> recordEvent(String entityKey, String action) {
        return db.sql("""
                        INSERT INTO entity_activity (entity_key, event_count, last_action) VALUES (:key, 1, :action)
                        ON CONFLICT (entity_key)
                        DO UPDATE SET event_count = entity_activity.event_count + 1, last_action = EXCLUDED.last_action
                        """)
                .bind("key", entityKey).bind("action", action)
                .then();
    }

    public Mono<EntityActivity> findById(String entityKey) {
        return db.sql("SELECT * FROM entity_activity WHERE entity_key = :key").bind("key", entityKey)
                .map(row -> new EntityActivity(row.get("entity_key", String.class),
                        row.get("event_count", Long.class), row.get("last_action", String.class)))
                .one();
    }
}
