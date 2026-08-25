-- Reference DDL for MySQL 8+ and MariaDB 10.6+ (the versions whose FOR UPDATE
-- SKIP LOCKED is real — accent gates on the same floors).
--
-- Type mapping from the PostgreSQL reference, and why:
--   UUID        -> CHAR(36)     no native type; identities are UUIDv7, whose canonical
--                               string form sorts identically to its byte order, so
--                               time-ordered index locality survives
--   TIMESTAMPTZ -> DATETIME(6)  NOT TIMESTAMP: MySQL's TIMESTAMP ends at 2038-01-19,
--                               and deadlines are caller-chosen instants that may
--                               legitimately exceed it. DATETIME carries no zone; the
--                               provider reads and writes every value through the same
--                               JDBC conversion, and the database itself never does
--                               time arithmetic on these columns.
--   BYTEA       -> LONGBLOB     payloads are opaque bytes of unbounded size
--
-- Naming convention (same as the PostgreSQL reference):
--   submitted_at  when the computation was submitted (a domain fact)
--   completed_at  when the computation reached its terminal outcome
--   created_at    when THIS row was written (bookkeeping only)

CREATE TABLE IF NOT EXISTS continuum_computation (
    id CHAR(36) NOT NULL PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    deadline_at DATETIME(6) NOT NULL,
    dispatch_payload LONGBLOB,
    attempt_count INT NOT NULL,
    submitted_at DATETIME(6) NOT NULL,
    last_updated_at DATETIME(6) NOT NULL
);
CREATE INDEX idx_continuum_computation_kind_deadline
    ON continuum_computation (kind, deadline_at);

CREATE TABLE IF NOT EXISTS continuum_continuation (
    id CHAR(36) NOT NULL PRIMARY KEY,
    computation_id CHAR(36) NOT NULL REFERENCES continuum_computation (id),
    payload LONGBLOB NOT NULL,
    created_at DATETIME(6) NOT NULL
);
CREATE INDEX idx_continuum_continuation_computation
    ON continuum_continuation (computation_id);

CREATE TABLE IF NOT EXISTS continuum_result (
    computation_id CHAR(36) NOT NULL PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload LONGBLOB,
    expiry_kind VARCHAR(20),
    message TEXT,
    deadline_at DATETIME(6) NOT NULL,
    attempt_count INT NOT NULL,
    submitted_at DATETIME(6) NOT NULL,
    completed_at DATETIME(6) NOT NULL
);
CREATE INDEX idx_continuum_result_kind_completed
    ON continuum_result (kind, completed_at);

CREATE TABLE IF NOT EXISTS continuum_outbox (
    id CHAR(36) NOT NULL PRIMARY KEY,
    computation_id CHAR(36) NOT NULL,
    continuation_id CHAR(36) NOT NULL,
    kind VARCHAR(200) NOT NULL,
    continuation_payload LONGBLOB NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload LONGBLOB,
    expiry_kind VARCHAR(20),
    message TEXT,
    available_at DATETIME(6) NOT NULL,
    claimed_by VARCHAR(200),
    claimed_until DATETIME(6),
    attempt_count INT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    submitted_at DATETIME(6) NOT NULL,
    completed_at DATETIME(6) NOT NULL
);
CREATE INDEX idx_continuum_outbox_kind_available
    ON continuum_outbox (kind, available_at);
