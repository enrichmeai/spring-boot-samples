package com.enrichmeai.samples.auditing.consumer;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface EntityActivityRepository extends JpaRepository<EntityActivity, String> {

    @Modifying
    @Query(value = """
            INSERT INTO entity_activity (entity_key, event_count, last_action) VALUES (:key, 1, :action)
            ON CONFLICT (entity_key)
            DO UPDATE SET event_count = entity_activity.event_count + 1, last_action = EXCLUDED.last_action
            """, nativeQuery = true)
    int recordEvent(@Param("key") String entityKey, @Param("action") String action);
}
