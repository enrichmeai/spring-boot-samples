package com.enrichmeai.samples.auditing.audit;

import java.time.Clock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/** Writes the audit row inside the transaction that made the change. */
@Component
class AuditRecorder {

    private final AuditEventRepository repository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    AuditRecorder(AuditEventRepository repository, ObjectMapper objectMapper, Clock clock) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /*
     * BEFORE_COMMIT runs while the caller's transaction is still open, so this INSERT commits or
     * rolls back with the business change. fallbackExecution = false (the default, stated here on
     * purpose): an event published with no transaction is not recorded, so a missing
     * @Transactional shows up as a missing audit row, which the tests catch.
     */
    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT, fallbackExecution = false)
    void on(EntityChangedEvent event) {
        repository.save(AuditEvent.pending(
                event.entityType(),
                event.entityId(),
                event.action(),
                currentActor(),
                toJson(event.before()),
                toJson(event.after()),
                clock.instant()));
    }

    private static String currentActor() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated() || auth instanceof AnonymousAuthenticationToken) {
            return "system";
        }
        return auth.getName();
    }

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            // Rolls the business transaction back too: no change without its audit record.
            throw new IllegalStateException("Cannot serialise audit state", e);
        }
    }
}
