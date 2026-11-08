package com.enrichmeai.samples.reactiveauditing;

import static com.enrichmeai.samples.reactiveauditing.IntegrationTestSupport.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import com.enrichmeai.samples.reactiveauditing.audit.AuditEvent;
import com.enrichmeai.samples.reactiveauditing.audit.AuditEventRepository;
import com.enrichmeai.samples.reactiveauditing.audit.BatchResult;
import com.enrichmeai.samples.reactiveauditing.audit.OutboxPoller;
import com.enrichmeai.samples.reactiveauditing.audit.OutboxStatus;
import com.enrichmeai.samples.reactiveauditing.cases.Case;
import com.enrichmeai.samples.reactiveauditing.cases.CaseService;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;

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
        IntegrationTestSupport.postgres(registry);
    }

    @Autowired CaseService caseService;
    @Autowired AuditEventRepository auditRepository;
    @Autowired OutboxPoller poller;
    @Autowired TransactionalOperator tx;

    @Test
    void theWriteSucceedsAndTheAuditRowWaitsForARetry() {
        Case c = caseService.create("Broker is down").block(TIMEOUT); // must not fail

        Instant before = Instant.now();
        BatchResult result = tx.transactional(poller.publishBatch()).block(TIMEOUT);

        assertThat(result.brokerUnavailable()).isTrue();
        List<AuditEvent> rows = auditRepository.findByEntity("case", c.id().toString()).collectList().block(TIMEOUT);
        assertThat(rows).singleElement().satisfies(r -> {
            assertThat(r.status()).isEqualTo(OutboxStatus.PENDING);
            assertThat(r.attempts()).isEqualTo(1);
            assertThat(r.nextAttemptAt()).isAfter(before);
            assertThat(r.lastError()).isNotBlank();
            assertThat(r.publishedAt()).isNull();
        });
    }
}
