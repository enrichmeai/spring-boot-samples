package com.enrichmeai.samples.reactiveauditing.cases;

import java.util.UUID;

import io.r2dbc.spi.Readable;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

import reactor.core.publisher.Mono;

@Repository
public class CaseRepository {

    private final DatabaseClient db;

    public CaseRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Case> insert(Case c) {
        return db.sql("INSERT INTO cases (id, title, status) VALUES (:id, :title, :status)")
                .bind("id", c.id()).bind("title", c.title()).bind("status", c.status())
                .then().thenReturn(c);
    }

    public Mono<Case> update(Case c) {
        return db.sql("UPDATE cases SET title = :title, status = :status WHERE id = :id")
                .bind("id", c.id()).bind("title", c.title()).bind("status", c.status())
                .then().thenReturn(c);
    }

    public Mono<Case> findById(UUID id) {
        return db.sql("SELECT * FROM cases WHERE id = :id").bind("id", id).map(CaseRepository::toCase).one();
    }

    /**
     * SELECT ... FOR UPDATE. Two writers to one case queue here, BEFORE either inserts its audit
     * row, so audit ids for one entity follow commit order.
     */
    public Mono<Case> findByIdForUpdate(UUID id) {
        return db.sql("SELECT * FROM cases WHERE id = :id FOR UPDATE").bind("id", id).map(CaseRepository::toCase).one();
    }

    private static Case toCase(Readable row) {
        return new Case(row.get("id", UUID.class), row.get("title", String.class), row.get("status", String.class));
    }
}
