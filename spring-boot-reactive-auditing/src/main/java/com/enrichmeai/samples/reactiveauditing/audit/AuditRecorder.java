package com.enrichmeai.samples.reactiveauditing.audit;

import java.time.Clock;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;

import reactor.core.publisher.Mono;

/**
 * Writes the audit row as a step of the service's own reactive chain, so it runs in the same
 * transaction as the change.
 *
 * <p>Why not {@code @TransactionalEventListener}, as in the blocking sample: its listener is invoked
 * as a callback with no way to return reactive work into the transaction being committed. An
 * explicit step in the chain is plain, and a forgotten {@code @Transactional} fails loudly here
 * instead of writing a row in a transaction of its own.
 */
@Component
public class AuditRecorder {

    private final AuditEventRepository repository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public AuditRecorder(AuditEventRepository repository, ObjectMapper objectMapper, Clock clock) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public Mono<Void> record(String entityType, String entityId, String action, Object before, Object after) {
        return TransactionSynchronizationManager.forCurrentTransaction()
                .onErrorMap(ex -> new IllegalStateException(
                        "Audit for " + entityType + ":" + entityId + " must run inside the change's transaction", ex))
                .flatMap(tx -> tx.isActualTransactionActive()
                        ? Mono.empty()
                        : Mono.error(new IllegalStateException("Audit for " + entityType + ":" + entityId
                                + " must run inside the change's transaction")))
                .then(currentActor())
                .flatMap(actor -> repository.insertPending(UUID.randomUUID(), entityType, entityId, action,
                        actor, toJson(before), toJson(after), clock.instant()));
    }

    private static Mono<String> currentActor() {
        return ReactiveSecurityContextHolder.getContext()
                .map(SecurityContext::getAuthentication)
                .filter(AuditRecorder::isRealUser)
                .map(Authentication::getName)
                .defaultIfEmpty("system");
    }

    private static boolean isRealUser(Authentication auth) {
        return auth != null && auth.isAuthenticated() && !(auth instanceof AnonymousAuthenticationToken);
    }

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            // Fails the chain, so the business transaction rolls back too.
            throw new IllegalStateException("Cannot serialise audit state", e);
        }
    }
}
