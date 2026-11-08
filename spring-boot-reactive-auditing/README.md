# spring-boot-reactive-auditing

The reactive version of [`spring-boot-blocking-auditing`](../spring-boot-blocking-auditing/): audit
backend updates in a Spring WebFlux app on R2DBC and Postgres, and publish each change to Kafka,
without losing events and without auditing changes that rolled back.

It uses the same pattern, a **transactional outbox with a polling publisher**, the same schema and
the same Kafka message. What changes is how each step is wired when nothing may block a thread. The
reasons are in [docs/reactive-design.md](docs/reactive-design.md). Why an outbox at all, rather than a
dual write, an `AFTER_COMMIT` listener, Spring Modulith or Debezium, is in the blocking sample's
[architecture options](../spring-boot-blocking-auditing/docs/architecture-options.md).

"Every update" means every update that goes through a service which calls `AuditRecorder.record`.
A bulk or native SQL update, another application writing the same tables, or a service that forgets
the call is not audited. If you need that, add a database trigger or change data capture.

```
request, ONE reactive transaction (@Transactional on a Mono)   asynchronous relay
------------------------------------------------------------   -------------------------------------
CaseService                                                    OutboxDrainer (SmartLifecycle)
  1. SELECT ... FOR UPDATE, then UPDATE the row                  drain, wait poll-interval, repeat
  2. AuditRecorder.record(...) as the next step of the chain   OutboxPoller, one transaction per batch
       refuses to run outside a transaction                      advisory lock (one poller)
       INSERT audit_event (PENDING)                              SELECT ... FOR UPDATE SKIP LOCKED
commit: both rows or neither                                     send on boundedElastic, key = entity
                                                                 mark PUBLISHED, or back off
                                                               AuditEventConsumer (one record per poll)
                                                                 returns a Mono that never fails:
                                                                 retries, else dead-letter topic;
                                                                 skips a repeated event_id
```

## Guarantees, and how each is enforced

| Guarantee | How | Test |
|---|---|---|
| An audit row exists if and only if the change committed | The audit `INSERT` is a step of the service's reactive chain, inside its `@Transactional` | `aRolledBackChangeLeavesNoAuditRow`, `aCommittedChangeWritesAPendingAuditRowWithActorAndSnapshots` |
| An audit outside a transaction fails loudly | `AuditRecorder` errors when there is no reactive transaction context, or a context with no actual transaction (propagation `SUPPORTS`) | `recordingOutsideATransactionFailsAndWritesNothing`, `recordingInATransactionContextWithNoActualTransactionFails` |
| The actor is the authenticated user | Read from `ReactiveSecurityContextHolder`; `system` when there is none | `aCommittedChange…`, `withNoUserTheActorIsSystem` |
| A Kafka outage loses nothing and fails no request, however long it lasts | Rows stay `PENDING`. A retriable Kafka error or the send timeout stops the batch and backs off without counting towards `FAILED` | `KafkaDownIntegrationTest`, `anUnreachableBrokerStopsTheBatchAndDefersInsteadOfFailing`, `ourOwnSendTimeoutCountsAsBrokerUnavailable` |
| A record the broker rejects (too large, for example) stops retrying | Non-retriable errors count towards `max-attempts`, then the row is `FAILED` | `aRecordSpecificFailureCountsTowardsFailed` |
| Concurrent writers to one entity are serialised, and audited in commit order | The service reads the row `FOR UPDATE`, so a second writer waits, then sees the first writer's result as its "before" | `concurrentWritersToOneEntityAreSerialisedAuditedAndPublishedInCommitOrder` |
| Events of one entity are published in order | One poller at a time; `concatMap` sends one record at a time; the Kafka key is the entity; a row waits while an earlier row of its entity is `FAILED` or waiting for a retry | `aLaterEventWaitsWhileAnEarlierEventOfTheSameEntityIsWaitingOrFailed`, `aFailedEventHoldsBackLaterEventsOfTheSameEntityOnly`, `thePollerPublishesInOrderAndTheConsumerAppliesEachEventOnce` |
| Only one instance polls at a time | `pg_try_advisory_xact_lock`; a poller that does not get it reports `lockAcquired = false`, not "nothing was due" | `aSecondPollerSkipsThePassWhileAnotherHoldsThePollerLock` |
| The poller never waits on a row another transaction holds | `FOR UPDATE SKIP LOCKED` | `thePollerPassesOverRowsAnotherTransactionHoldsInsteadOfWaiting` |
| A duplicate delivery is applied once | At-least-once delivery; the consumer inserts `event_id` into `processed_event` with `ON CONFLICT DO NOTHING` in the same R2DBC transaction as its side effects | `aDuplicateDeliveryIsAppliedOnce` (also checks one marker row and nothing dead-lettered) |
| The consumer never drops a record silently | spring-kafka 3.3.16 acknowledges a failed async listener and only logs it, so the listener's `Mono` never fails: transient errors are retried with backoff (`audit.consumer.retries`), and a malformed record, or one that still fails, goes to `audit-events.DLT` with the error in a header | `aTransientFailureIsRetriedAndThenApplied`, `aRecordThatKeepsFailingIsDeadLetteredNotDropped`, `aMalformedRecordGoesStraightToTheDeadLetterTopic`, `aMalformedRecordIsDeadLetteredAndTheStreamCarriesOn` |
| The consumer applies one entity's events in order | `max-poll-records: 1`: with an async listener the container pauses polling until the poll's records are acknowledged, so records are applied one at a time | `manyEventsForOneEntityAreAppliedInOrder` (a race-based check: with the setting removed it failed 3 of 3 runs at 52 events; at 22 events it failed only 1 of 2) |

Each guard above was checked to make its test fail when removed: the row lock, the advisory lock,
`SKIP LOCKED`, the outage handling, the per-entity `NOT EXISTS` predicate, the in-batch blocked-key
check, the consumer's dedup, the recorder's transaction check, the consumer's retry, its
dead-lettering, and one record per poll.

Not covered: cancellation of a batch mid-send, which rolls the batch back so its rows are sent again
(allowed by at-least-once, not tested). If even the dead-letter send fails, the `Mono` fails and
spring-kafka acknowledges and logs the record: the one case where a record is not kept.

## Run the tests

Needs JDK 21 and Docker (Testcontainers starts `postgres:16-alpine` and `apache/kafka:3.8.0`).

```bash
./mvnw verify
```

## Run the app

```bash
docker compose up -d
```

```bash
./mvnw spring-boot:run
```

```bash
curl -u demo:demo -H 'Content-Type: application/json' -d '{"title":"Lease dispute"}' localhost:8080/cases
```

Then `POST /cases/{id}/close`. `audit_event` holds one row per change, `PENDING` for about a second,
then `PUBLISHED`. `entity_activity` is the consumer's read model.

## Configuration

`audit.outbox.*` is the same as in the blocking sample: `topic`, `poller-enabled`, `poll-interval`,
`batch-size`, `send-timeout` (keep it above the producer's `max.block.ms` + `delivery.timeout.ms`),
`max-attempts` (record-specific failures only) and `max-backoff`.

What differs:

- `spring.r2dbc.*` for the application and `spring.flyway.*` (JDBC) for migrations at startup.
- `spring.kafka.listener.ack-mode: manual`. The listener returns a `Mono`, and spring-kafka commits
  the offset when it completes (it forces `MANUAL` for such listeners anyway).
- `spring.kafka.consumer.max-poll-records: 1`, for in-order consumption (see above). It caps the
  consumer at one record per database round trip. For more throughput, add partitions (and
  listener concurrency), or serialise per partition inside the listener instead.
- `audit.consumer.dead-letter-topic` (`audit-events.DLT`), `retries` (3) and `retry-backoff` (200ms).

## Operating it

As for the blocking sample: alert on the oldest `PENDING` row and on any `FAILED` row. A `FAILED` row
blocks later events of its entity until someone resets it to `PENDING` with `attempts = 0` and
`next_attempt_at = now()`. Archive `audit_event` by retention, and prune `processed_event`.
