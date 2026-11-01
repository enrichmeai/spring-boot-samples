-- The business table this sample audits.
CREATE TABLE cases (
    id     UUID         PRIMARY KEY,
    title  TEXT         NOT NULL,
    status VARCHAR(30)  NOT NULL
);

-- The audit record AND the outbox. Rows are kept after publishing: this table is the record.
CREATE TABLE audit_event (
    id              BIGSERIAL    PRIMARY KEY,          -- total order
    event_id        UUID         NOT NULL UNIQUE,      -- consumer dedup key
    entity_type     VARCHAR(100) NOT NULL,
    entity_id       VARCHAR(100) NOT NULL,
    action          VARCHAR(50)  NOT NULL,
    actor           VARCHAR(255) NOT NULL,
    before_state    JSONB,
    after_state     JSONB,
    occurred_at     TIMESTAMPTZ  NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING',  -- PENDING | PUBLISHED | FAILED
    attempts        INT          NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ  NOT NULL,
    last_error      TEXT,
    published_at    TIMESTAMPTZ
);

-- What the poller scans.
CREATE INDEX audit_event_pending ON audit_event (next_attempt_at, id) WHERE status = 'PENDING';
-- What the per-entity ordering check (NOT EXISTS an earlier blocked row) probes.
CREATE INDEX audit_event_entity_open ON audit_event (entity_type, entity_id, id) WHERE status <> 'PUBLISHED';

-- Consumer side: idempotency marker.
CREATE TABLE processed_event (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Consumer side: an example read model. A counter makes a duplicate delivery visible
-- (it would count twice), which is what the idempotency test relies on.
CREATE TABLE entity_activity (
    entity_key  VARCHAR(220) PRIMARY KEY,
    event_count BIGINT       NOT NULL,
    last_action VARCHAR(50)  NOT NULL
);
