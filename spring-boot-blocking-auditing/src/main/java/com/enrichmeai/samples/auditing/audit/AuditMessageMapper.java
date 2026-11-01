package com.enrichmeai.samples.auditing.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

/** Turns an audit row into the Kafka message value. */
@Component
public class AuditMessageMapper {

    private final ObjectMapper objectMapper;

    public AuditMessageMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String toJson(AuditEvent e) {
        try {
            return objectMapper.writeValueAsString(new AuditMessage(
                    e.getEventId(), e.getEntityType(), e.getEntityId(), e.getAction(), e.getActor(),
                    e.getOccurredAt(), tree(e.getBeforeState()), tree(e.getAfterState())));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Cannot build the message for audit event " + e.getEventId(), ex);
        }
    }

    public AuditMessage fromJson(String json) {
        try {
            return objectMapper.readValue(json, AuditMessage.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Not an audit message", ex);
        }
    }

    private JsonNode tree(String json) throws JsonProcessingException {
        return json == null ? null : objectMapper.readTree(json);
    }
}
