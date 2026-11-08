package com.enrichmeai.samples.reactiveauditing.cases;

import java.util.Map;
import java.util.UUID;

/** A row of {@code cases}. */
public record Case(UUID id, String title, String status) {

    Case renamed(String newTitle) {
        return new Case(id, newTitle, status);
    }

    Case closed() {
        return new Case(id, title, "CLOSED");
    }

    /** A plain map for the audit snapshot. */
    Map<String, Object> snapshot() {
        return Map.of("id", id, "title", title, "status", status);
    }
}
