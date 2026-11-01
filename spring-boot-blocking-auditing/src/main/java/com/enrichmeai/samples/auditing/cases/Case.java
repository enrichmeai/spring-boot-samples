package com.enrichmeai.samples.auditing.cases;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "cases")
public class Case {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false)
    private String status;

    protected Case() {
        // for JPA
    }

    public Case(UUID id, String title, String status) {
        this.id = id;
        this.title = title;
        this.status = status;
    }

    public UUID getId() { return id; }
    public String getTitle() { return title; }
    public String getStatus() { return status; }

    void rename(String newTitle) { this.title = newTitle; }
    void close() { this.status = "CLOSED"; }
}
