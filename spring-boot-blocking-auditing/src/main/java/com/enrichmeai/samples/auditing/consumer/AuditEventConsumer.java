package com.enrichmeai.samples.auditing.consumer;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.enrichmeai.samples.auditing.audit.AuditMessage;
import com.enrichmeai.samples.auditing.audit.AuditMessageMapper;
import com.enrichmeai.samples.auditing.audit.OutboxPoller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reads the audit stream. Delivery is at-least-once, so it records each event_id and skips one it
 * has seen. The marker and the side effects commit in one database transaction.
 */
@Component
class AuditEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditEventConsumer.class);

    private final ProcessedEventRepository processed;
    private final EntityActivityRepository activity;
    private final AuditMessageMapper mapper;

    AuditEventConsumer(ProcessedEventRepository processed, EntityActivityRepository activity,
                       AuditMessageMapper mapper) {
        this.processed = processed;
        this.activity = activity;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "${audit.outbox.topic}", groupId = "audit-projection")
    @Transactional
    public void on(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(OutboxPoller.HEADER_EVENT_ID);
        if (header == null) {
            throw new IllegalArgumentException("Record at offset " + record.offset() + " has no event_id header");
        }
        UUID eventId = UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));

        if (processed.markProcessed(eventId) == 0) {
            log.debug("Skipped duplicate audit event {}", eventId);
            return;
        }
        AuditMessage message = mapper.fromJson(record.value());
        activity.recordEvent(message.entityType() + ":" + message.entityId(), message.action());
    }
}
