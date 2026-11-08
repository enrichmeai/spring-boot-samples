package com.enrichmeai.samples.reactiveauditing.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * Runs the poller: drain every due batch, wait poll-interval, repeat. Each batch is its own
 * transaction. Started with the application context and stopped (cancelled) with it; a cancelled
 * batch rolls back, so its rows are simply sent again later.
 */
@Component
public class OutboxDrainer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxDrainer.class);

    private final OutboxPoller poller;
    private final TransactionalOperator tx;
    private final OutboxProperties props;
    private volatile Disposable running;

    public OutboxDrainer(OutboxPoller poller, TransactionalOperator transactionalOperator, OutboxProperties props) {
        this.poller = poller;
        this.tx = transactionalOperator;
        this.props = props;
    }

    /** Publishes batches until one comes back short, finds Kafka unreachable, or finds another poller. */
    public Mono<Void> drain() {
        Mono<BatchResult> oneBatch = Mono.defer(() -> tx.transactional(poller.publishBatch()));
        return oneBatch
                .expand(result -> shouldContinue(result) ? oneBatch : Mono.empty())
                .then();
    }

    private boolean shouldContinue(BatchResult r) {
        return r.lockAcquired() && !r.brokerUnavailable() && r.taken() == props.batchSize();
    }

    @Override
    public void start() {
        if (!props.pollerEnabled()) {
            return;
        }
        running = Mono.defer(this::drain)
                .onErrorResume(ex -> {
                    log.error("Audit outbox poll failed; retrying after {}", props.pollInterval(), ex);
                    return Mono.empty();
                })
                .then(Mono.delay(props.pollInterval()))
                .repeat()
                .subscribe();
    }

    @Override
    public void stop() {
        Disposable d = running;
        if (d != null) {
            d.dispose();
        }
        running = null;
    }

    @Override
    public boolean isRunning() {
        return running != null && !running.isDisposed();
    }
}
