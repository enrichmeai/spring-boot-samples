# Reactive design: what changes from the blocking sample

**Decision:** keep the same transactional outbox, schema, poller algorithm and Kafka message as
[`spring-boot-blocking-auditing`](../../spring-boot-blocking-auditing/), and change only the wiring
that a non-blocking stack forces. The options and the reasons for an outbox are in the blocking
sample's [architecture options](../../spring-boot-blocking-auditing/docs/architecture-options.md). This
document covers the reactive choices.

## Decisions

| Concern | Blocking sample | Reactive sample | Why |
|---|---|---|---|
| Web | Spring MVC | WebFlux | The point of the sample |
| Database access | JPA / Hibernate | R2DBC through `DatabaseClient` | jsonb, `FOR UPDATE`, `SKIP LOCKED` and advisory locks are plain SQL; no ORM layer to translate them |
| Migrations | Flyway over JDBC | Flyway over JDBC, at startup only | Flyway has no R2DBC driver. The JDBC driver is used for migrations and nothing else |
| Writing the audit row | `BEFORE_COMMIT` `@TransactionalEventListener` | An explicit `AuditRecorder.record(...)` step in the service's chain | See "Why not a transactional event listener" |
| Missing transaction | Event silently not recorded (`fallbackExecution = false`) | `record` fails with `IllegalStateException` | A reactive chain can check its transaction context before it writes |
| Who the actor is | `SecurityContextHolder` (thread-bound) | `ReactiveSecurityContextHolder` (Reactor context) | Reactive code changes threads; the security context travels in the subscriber context |
| Row lock for concurrent writers | Needed so the audit insert follows commit order (Hibernate flushes the `UPDATE` after the audit insert) | Needed so the second writer reads the first writer's result | R2DBC runs the `UPDATE` at once, so order is already right. Without the lock the second writer computes its change from a stale read and overwrites the first, and its "before" snapshot is wrong. The test checks that snapshot |
| Kafka send | `KafkaTemplate.send(...).get(timeout)` | `Mono.fromFuture(() -> kafka.send(...))` on `boundedElastic`, with `.timeout(...)` | See "Sending to Kafka" |
| Order within a batch | A `for` loop | `concatMap` | One record at a time, in id order |
| Running the poller | `@Scheduled(fixedDelay)` | `OutboxDrainer`, a `SmartLifecycle` running drain, delay, repeat | Composes with the reactive transaction and stops (cancels) with the context |
| Batch transaction | `TransactionTemplate` | `TransactionalOperator` | The reactive equivalent |
| Consumer | `@KafkaListener` + `@Transactional`, blocking JPA calls | `@KafkaListener` returning `Mono<Void>`, `TransactionalOperator` | spring-kafka runs a `Mono` listener asynchronously and commits its offset when it completes, so the consumer thread never waits on the database |
| Consumer failures | `DefaultErrorHandler` retries twice, then logs and skips | The `Mono` retries, then publishes to `audit-events.DLT`, and only then completes | A failed async listener is acknowledged and only logged; the container's error handler never sees it |
| Consumer order | One record at a time by construction | `max-poll-records: 1` | An async listener receives every record of a poll at once, so they would run concurrently |

## Why not a transactional event listener

In the blocking sample, a service publishes an event and a `BEFORE_COMMIT` listener inserts the
audit row in the same transaction. Spring 6.1 added `TransactionalEventPublisher`, which carries the
reactive `TransactionContext` as the event source so such listeners fire for reactive transactions
(spring-tx 6.2.19, `org.springframework.transaction.reactive.TransactionalEventPublisher`). But the
listener is still a callback that returns nothing to the commit. To write the audit row inside the
transaction it would have to block on the R2DBC insert, or start work the commit does not wait for.

An explicit step in the chain has neither problem. It also turns a forgotten `@Transactional` into an
error instead of a missing row.

## Sending to Kafka

- **Not `ReactiveKafkaProducerTemplate`.** It is still in spring-kafka 3.3.16, but it is built on
  reactor-kafka, a project that is no longer actively developed. Depending on it would put an unmaintained
  library in the path of every audit event.
- **`KafkaTemplate` is asynchronous apart from one wait.** `send()` returns a `CompletableFuture`. But
  it can block the calling thread for up to `max.block.ms` while it fetches topic metadata (kafka-clients
  3.9.2 `ProducerConfig`, `MAX_BLOCK_MS_DOC`). So the send is subscribed on `boundedElastic`, never on a
  Netty event-loop thread, and bounded by `.timeout(send-timeout)`.
- **Timeouts are outages.** `.timeout(...)` raises `java.util.concurrent.TimeoutException`, which the
  poller treats like a retriable Kafka error. It stops the batch and backs off without counting
  towards `FAILED`.

## The consumer

spring-kafka 3.3.16 detects a listener method that returns `Mono` (`MessagingMessageListenerAdapter`)
and forces the container's ack mode to `MANUAL` (`KafkaMessageListenerContainer`). It then commits the
offset out of order, when each `Mono` completes. The sample sets `ack-mode: manual` explicitly so the
configuration says what happens.

The dedup marker and the read-model update run in one R2DBC transaction, so a redelivery after a crash
finds the marker and skips.

**Two traps an async listener adds, both found by the review and read in the 3.3.16 source:**

1. **A failed `Mono` is not redelivered.** `MessagingMessageListenerAdapter#asyncFailure` passes the
   error to the listener's error handler. With none configured, the error is rethrown, then caught,
   the record is acknowledged, and the error is logged. The container's `DefaultErrorHandler` retry does
   not apply. So the listener's `Mono` is built never to fail. A transient error is retried with
   backoff in a fresh transaction each time. A malformed record (no `event_id`, unparseable value), or
   one that still fails after the retries, is published to `audit-events.DLT` with the error and its
   source offset in headers, and only then acknowledged. If even that send fails, the `Mono` fails and
   the record is logged: the one case nothing keeps.
2. **Records of one poll run concurrently.** The container hands every record of a poll to the
   listener without waiting for the previous `Mono`. It pauses the next poll until all of them are
   acknowledged (`KafkaMessageListenerContainer#doPauseConsumerIfNecessary`). With
   `max-poll-records: 1` that pause makes processing strictly sequential, so one entity's events are
   applied in order. The cost is one database round trip per record per consumer; partitions and
   listener concurrency scale it out.

## What did not change

- The schema (`V1__audit_outbox.sql`, identical) and the Kafka message (`AuditMessage`, identical).
- The poller's rules: the advisory lock, `SKIP LOCKED`, the per-entity `NOT EXISTS` predicate,
  blocked keys within a batch, exponential backoff, outages not counting towards `FAILED`, and
  `BatchResult` telling "lock held elsewhere" apart from "nothing due".
- At-least-once delivery with consumer deduplication.

## How this was verified

- `./mvnw verify`: 35 tests, 0 failures, against Postgres 16 and Kafka 3.8 in Testcontainers.
- Eleven guards were each shown to make their test fail when removed. Two did not fail at first: the
  row lock and the recorder's transaction-active check. Their tests were strengthened until they did,
  and the "Row lock" row above records what that showed. The ordering check for `max-poll-records: 1`
  is race-based: with the setting removed it failed 3 of 3 runs at 52 events, but only 1 of 2 at 22.
- An independent review found the two consumer traps above (one major, one minor) and two weak
  tests. The duplicate test now also checks the marker row and the dead-letter topic. The
  concurrent-writer test now waits for Postgres to show an ungranted lock instead of sleeping.
- No `block()`, `join()` or `get()` in `src/main` (grepped).
- The app was run against `compose.yaml` and exercised with `curl`: two changes, both `PUBLISHED`,
  actor recorded, consumer count 2, 401 without credentials, and 404 for an unknown id.
- Library behaviour was read in the source jars this build resolves: spring-tx 6.2.19, spring-kafka
  3.3.16 and kafka-clients 3.9.2.
- Not verified: the claim that reactor-kafka is no longer actively developed. It comes from memory; check
  its repository before relying on it.
