package com.enrichmeai.samples.auditing.cases;

import java.util.Optional;
import java.util.UUID;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CaseRepository extends JpaRepository<Case, UUID> {

    /**
     * SELECT ... FOR UPDATE. Two writers to one case queue here, BEFORE either inserts its audit
     * row, so audit ids for one entity follow commit order. Without it, the first writer's audit
     * row could get the lower id but commit second, and be published second.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from Case c where c.id = :id")
    Optional<Case> findByIdForUpdate(@Param("id") UUID id);
}
