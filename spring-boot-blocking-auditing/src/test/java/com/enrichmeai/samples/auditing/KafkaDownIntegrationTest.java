package com.enrichmeai.samples.auditing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import com.enrichmeai.samples.auditing.audit.AuditEvent;
import com.enrichmeai.samples.auditing.audit.AuditEventRepository;
import com.enrichmeai.samples.auditing.audit.BatchResult;
import com.enrichmeai.samples.auditing.audit.OutboxPoller;
import com.enrichmeai.samples.auditing.audit.OutboxStatus;
import com.enrichmeai.samples.auditing.cases.Case;
import com.enrichmeai.samples.auditing.cases.CaseService;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;

/** Kafka is unreachable: writes still succeed and their audit rows wait, with the failure recorded. */
@SpringBootTest(properties = {
        "audit.outbox.poller-enabled=false",
        "spring.kafka.bootstrap-servers=localhost:1",   // nothing listens here
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class KafkaDownIntegrationTest {

    @DynamicPropertySource
    static void postgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> Containers.postgres().getJdbcUrl());
        registry.add("spring.datasource.username", () -> Containers.postgres().getUsername());
        registry.add("spring.datasource.password", () -> Containers.postgres().getPassword());
    }

    @Autowired CaseService caseService;
    @Autowired AuditEventRepository auditRepository;
    @Autowired OutboxPoller poller;
    @Autowired TransactionTemplate outboxTransactionTemplate;

    @Test
    void theWriteSucceedsAndTheAuditRowWaitsForARetry() {
        Case c = caseService.create("Broker is down"); // must not throw

        Instant before = Instant.now();
        BatchResult result = outboxTransactionTemplate.execute(s -> poller.publishBatch());

        assertThat(result.brokerUnavailable()).isTrue();

        List<AuditEvent> rows = auditRepository.findByEntityTypeAndEntityIdOrderById("case", c.getId().toString());
        assertThat(rows).singleElement().satisfies(r -> {
            assertThat(r.getStatus()).isEqualTo(OutboxStatus.PENDING);
            assertThat(r.getAttempts()).isEqualTo(1);
            assertThat(r.getNextAttemptAt()).isAfter(before);
            assertThat(r.getLastError()).isNotBlank();
            assertThat(r.getPublishedAt()).isNull();
        });
    }
}
