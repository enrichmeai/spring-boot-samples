package com.enrichmeai.samples.auditing.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** An example read model fed by the audit stream: how many events each entity has had. */
@Entity
@Table(name = "entity_activity")
public class EntityActivity {

    @Id
    @Column(name = "entity_key")
    private String entityKey;

    @Column(name = "event_count", nullable = false)
    private long eventCount;

    @Column(name = "last_action", nullable = false)
    private String lastAction;

    protected EntityActivity() {
        // for JPA
    }

    public String getEntityKey() { return entityKey; }
    public long getEventCount() { return eventCount; }
    public String getLastAction() { return lastAction; }
}
