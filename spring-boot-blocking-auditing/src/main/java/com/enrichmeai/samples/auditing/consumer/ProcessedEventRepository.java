package com.enrichmeai.samples.auditing.consumer;

import java.util.UUID;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

public interface ProcessedEventRepository extends Repository<ProcessedEvent, UUID> {

    /** @return 1 the first time this event_id is seen, 0 for a duplicate */
    @Modifying
    @Query(value = "INSERT INTO processed_event (event_id) VALUES (:id) ON CONFLICT DO NOTHING",
           nativeQuery = true)
    int markProcessed(@Param("id") UUID eventId);

    long count();
}
