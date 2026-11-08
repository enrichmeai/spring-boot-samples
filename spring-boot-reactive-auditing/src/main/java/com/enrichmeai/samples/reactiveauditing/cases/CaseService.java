package com.enrichmeai.samples.reactiveauditing.cases;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.UnaryOperator;

import com.enrichmeai.samples.reactiveauditing.audit.AuditRecorder;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import reactor.core.publisher.Mono;

/**
 * An ordinary reactive business service. Each change records its audit row as the last step of
 * the same chain, so both are in one transaction.
 */
@Service
public class CaseService {

    static final String ENTITY_TYPE = "case";

    private final CaseRepository cases;
    private final AuditRecorder audit;

    public CaseService(CaseRepository cases, AuditRecorder audit) {
        this.cases = cases;
        this.audit = audit;
    }

    @Transactional
    public Mono<Case> create(String title) {
        return cases.insert(new Case(UUID.randomUUID(), title, "OPEN"))
                .flatMap(c -> audit.record(ENTITY_TYPE, c.id().toString(), "CREATED", null, c.snapshot()).thenReturn(c));
    }

    @Transactional
    public Mono<Case> rename(UUID id, String title) {
        return change(id, "RENAMED", c -> c.renamed(title));
    }

    @Transactional
    public Mono<Case> close(UUID id) {
        return change(id, "CLOSED", Case::closed);
    }

    /** Lock the row, change it, audit it. Every update path goes through here. */
    private Mono<Case> change(UUID id, String action, UnaryOperator<Case> how) {
        return cases.findByIdForUpdate(id)
                .switchIfEmpty(Mono.error(() -> new NoSuchElementException("No case " + id)))
                .flatMap(before -> cases.update(how.apply(before))
                        .flatMap(after -> audit.record(ENTITY_TYPE, id.toString(), action, before.snapshot(), after.snapshot())
                                .thenReturn(after)));
    }
}
