package com.enrichmeai.samples.auditing.cases;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import com.enrichmeai.samples.auditing.audit.EntityChangedEvent;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** An ordinary business service. Each change publishes one EntityChangedEvent in its transaction. */
@Service
public class CaseService {

    static final String ENTITY_TYPE = "case";

    private final CaseRepository cases;
    private final ApplicationEventPublisher events;

    public CaseService(CaseRepository cases, ApplicationEventPublisher events) {
        this.cases = cases;
        this.events = events;
    }

    @Transactional
    public Case create(String title) {
        Case c = cases.save(new Case(UUID.randomUUID(), title, "OPEN"));
        events.publishEvent(new EntityChangedEvent(ENTITY_TYPE, c.getId().toString(), "CREATED", null, snapshot(c)));
        return c;
    }

    @Transactional
    public Case rename(UUID id, String title) {
        Case c = find(id);
        Map<String, Object> before = snapshot(c); // taken before the change
        c.rename(title);
        events.publishEvent(new EntityChangedEvent(ENTITY_TYPE, id.toString(), "RENAMED", before, snapshot(c)));
        return c;
    }

    @Transactional
    public Case close(UUID id) {
        Case c = find(id);
        Map<String, Object> before = snapshot(c);
        c.close();
        events.publishEvent(new EntityChangedEvent(ENTITY_TYPE, id.toString(), "CLOSED", before, snapshot(c)));
        return c;
    }

    /** Locks the row: see CaseRepository#findByIdForUpdate. Every update path must load through here. */
    private Case find(UUID id) {
        return cases.findByIdForUpdate(id).orElseThrow(() -> new NoSuchElementException("No case " + id));
    }

    /** A plain map, never the entity: no lazy loading, no Jackson recursion. */
    private static Map<String, Object> snapshot(Case c) {
        return Map.of("id", c.getId(), "title", c.getTitle(), "status", c.getStatus());
    }
}
