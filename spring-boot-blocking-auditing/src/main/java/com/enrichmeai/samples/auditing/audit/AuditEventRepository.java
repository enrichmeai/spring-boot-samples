package com.enrichmeai.samples.auditing.audit;

import java.time.Instant;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AuditEventRepository extends JpaRepository<AuditEvent, Long> {

    /**
     * Only one poller works at a time, across all instances: a transaction-scoped Postgres
     * advisory lock, released on commit or rollback. Returns false when another instance holds it.
     * One poller is what makes per-entity order strict; with two, each could send a different
     * event of the same entity.
     */
    @Query(value = "SELECT pg_try_advisory_xact_lock(:key)", nativeQuery = true)
    boolean tryPollerLock(@Param("key") long key);

    /**
     * The next due rows, oldest first. A row is skipped while an earlier row of the same entity
     * is FAILED or waiting for a retry, so a later event never overtakes an earlier one.
     * Call inside a transaction: FOR UPDATE holds the rows until it commits.
     */
    @Query(value = """
            SELECT a.* FROM audit_event a
             WHERE a.status = 'PENDING'
               AND a.next_attempt_at <= :now
               AND NOT EXISTS (
                     SELECT 1 FROM audit_event b
                      WHERE b.entity_type = a.entity_type
                        AND b.entity_id = a.entity_id
                        AND b.id < a.id
                        AND (b.status = 'FAILED'
                             OR (b.status = 'PENDING' AND b.next_attempt_at > :now)))
             ORDER BY a.id
             LIMIT :batchSize
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<AuditEvent> lockNextBatch(@Param("now") Instant now, @Param("batchSize") int batchSize);

    long countByStatus(OutboxStatus status);

    List<AuditEvent> findByEntityTypeAndEntityIdOrderById(String entityType, String entityId);
}
