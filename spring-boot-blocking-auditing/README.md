# spring-boot-blocking-auditing

How to audit backend updates in a standard, blocking Spring Boot app (Spring MVC, Spring Data JPA,
Postgres) **and** publish each change to Kafka, without losing events and without auditing changes
that rolled back.

"Every update" here means every update that goes through a service which publishes an
`EntityChangedEvent`. A bulk JPQL or native update, another application writing the same tables, or a
service that forgets to publish is not audited. If you need that, add a database trigger or change
data capture.

The pattern is the **transactional outbox with a polling publisher**:

```
request thread, ONE database transaction           asynchronous relay
-----------------------------------------          --------------------------------------------
CaseService (@Transactional)                        OutboxPoller (@Scheduled, every 1 s)
  1. load the row FOR UPDATE, change it               takes a Postgres advisory lock (one poller)
  2. publishEvent(EntityChangedEvent)                 SELECT ... FOR UPDATE SKIP LOCKED
AuditRecorder (BEFORE_COMMIT)                         sends each row to Kafka, key = entity
  3. INSERT audit_event (PENDING)                     marks it PUBLISHED, or backs off on failure
commit: both rows or neither                        AuditEventConsumer
                                                      skips a repeated event_id, then applies it
```

`audit_event` is both the audit record and the outbox. Rows stay after they are published.

Why this design over the alternatives (dual write, an `AFTER_COMMIT` listener, Spring Modulith,
Debezium): [docs/architecture-options.md](docs/architecture-options.md).

## Guarantees, and how each is enforced

| Guarantee | How | Test |
|---|---|---|
| An audit row exists if and only if the change committed | The recorder is a `BEFORE_COMMIT` `@TransactionalEventListener`, so its `INSERT` is in the caller's transaction | `aRolledBackChangeLeavesNoAuditRow`, `aCommittedChangeWritesAPendingAuditRowWithActorAndSnapshots` |
| A missing `@Transactional` does not half-work | `fallbackExecution = false` (Spring's default, stated explicitly): an event published with no transaction is not recorded | `anEventPublishedOutsideATransactionIsNotRecorded` |
| A Kafka outage loses nothing and fails no request, however long it lasts | Rows stay `PENDING`. A retriable Kafka error or our own send timeout stops the batch and backs off without counting towards `FAILED` | `KafkaDownIntegrationTest`, `anUnreachableBrokerStopsTheBatchAndNeverMakesAnEventFailed` |
| A record the broker rejects (too large, for example) stops retrying | Non-retriable errors count towards `max-attempts`, then the row is `FAILED` | `aRecordSpecificFailureCountsTowardsFailed` |
| Concurrent writers to one entity are audited in commit order | Services load the row with `SELECT ... FOR UPDATE` (`CaseRepository.findByIdForUpdate`), so the second writer waits before it inserts its audit row | `concurrentWritersToOneEntityAreAuditedAndPublishedInCommitOrder` |
| Events of one entity are published in order | One poller at a time; the Kafka key is the entity; a row waits while an earlier row of its entity is `FAILED` or waiting for a retry | `aLaterEventWaitsWhileAnEarlierEventOfTheSameEntityIsWaitingOrFailed`, `aFailedEventHoldsBackLaterEventsOfTheSameEntityOnly`, `thePollerPublishesInOrderAndTheConsumerAppliesEachEventOnce` |
| Only one instance polls at a time | `pg_try_advisory_xact_lock`. A poller that does not get it reports `lockAcquired = false`, which is different from "nothing was due" | `aSecondPollerSkipsThePassWhileAnotherHoldsThePollerLock` |
| The poller never waits on a row another transaction holds | `FOR UPDATE SKIP LOCKED` | `thePollerPassesOverRowsAnotherTransactionHoldsInsteadOfWaiting` |
| A duplicate delivery is applied once | Delivery is at-least-once; the consumer inserts `event_id` into `processed_event` with `ON CONFLICT DO NOTHING` in the same transaction as its side effects | `aDuplicateDeliveryIsAppliedOnce` |
| A shutdown does not use up a retry | An interrupted send leaves the row untouched | `anInterruptLeavesTheEventUntouched` |

Each test above for the row lock, the advisory lock, `SKIP LOCKED`, the outage handling, the
per-entity `NOT EXISTS` predicate, the poller's in-batch blocked-key check and the consumer's dedup
check was checked to fail when the logic it guards is removed.

What these do not cover: the consumer retries a record it cannot process twice and then logs and
skips it, so a poison record is missing from the read model (the audit row and the Kafka record
still exist). Add a dead-letter topic for that.

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

Then close it with `POST /cases/{id}/close` and look at `audit_event`: one row per change, `PENDING`
for about a second, then `PUBLISHED`. `entity_activity` is the consumer's read model.

## Configuration (`audit.outbox.*`)

| Property | Default | Meaning |
|---|---|---|
| `topic` | `audit-events` | Kafka topic |
| `poller-enabled` | `true` | Turns the `@Scheduled` poller on (tests switch it off and call `publishBatch()` themselves) |
| `poll-interval` | `1s` | Delay between polls |
| `batch-size` | `20` | Rows per transaction |
| `send-timeout` | `9s` | How long the poller waits for one send. Keep it above the producer's `max.block.ms` + `delivery.timeout.ms` (3 s + 5 s), so the producer gives up first |
| `max-attempts` | `10` | After this many record-specific failures a row becomes `FAILED`. Broker outages do not count |
| `max-backoff` | `5m` | Cap on the exponential retry delay (2 s, 4 s, 8 s, …) |

## Operating it

- **Alert on** the age of the oldest `PENDING` row, and on any `FAILED` row.
- **A `FAILED` row blocks** later events of the same entity on purpose, to keep their order. Fix the
  cause, then set the row back to `PENDING` with `attempts = 0` and `next_attempt_at = now()`.
- **Long transactions.** The batch holds its row locks and a connection while it sends. A healthy
  batch takes milliseconds per record; an outage costs one send timeout per poll, because the batch
  stops at the first broker failure. If your database sets `idle_in_transaction_session_timeout`,
  keep it above that. If a transaction dies after its sends, those rows are simply sent again.
- **No transaction timeout.** With JPA, Spring applies a transaction timeout only as a timeout on
  queries, so it would not bound the time spent waiting on Kafka. The batch bounds that itself.
- **Throughput.** One poller sends one record at a time. That is simple and keeps order. If it becomes
  the limit, send a batch asynchronously and wait on all the futures, or move to change data capture
  (Debezium).
- **Retention.** `PUBLISHED` rows are the audit trail. Archive or partition `audit_event` by your
  retention policy, and prune `processed_event` past the topic's retention.

## Not covered here

- Hibernate Envers for full row-version history: it can sit alongside this.
- A dead-letter topic for records the consumer cannot process (it retries twice, then logs and skips).
- Updates that bypass the services (bulk or native SQL, other applications).
- Masking personal data in the `before`/`after` snapshots.
