-- Reference DDL for Oracle 23ai+ (IF NOT EXISTS needs 23c).
--
-- Type mapping from the PostgreSQL reference, and why:
--   UUID        -> VARCHAR2(36)                identities are UUIDv7; canonical text sorts as bytes
--   TIMESTAMPTZ -> TIMESTAMP(6) WITH TIME ZONE  native
--   BYTEA       -> BLOB                          opaque bytes of unbounded size
--   TEXT        -> CLOB                          diagnostic prose of unbounded size
--   VARCHAR     -> VARCHAR2                      Oracle's variable-length string type
--
-- Naming convention (same as the PostgreSQL reference):
--   submitted_at  when the computation was submitted (a domain fact)
--   completed_at  when the computation reached its terminal outcome
--   created_at    when THIS row was written (bookkeeping only)

CREATE TABLE IF NOT EXISTS continuum_computation (
    id VARCHAR2(36) NOT NULL PRIMARY KEY,
    kind VARCHAR2(200) NOT NULL,
    deadline_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    dispatch_payload BLOB,
    attempt_count INT NOT NULL,
    submitted_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_updated_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_computation_kind_deadline
    ON continuum_computation (kind, deadline_at);

CREATE TABLE IF NOT EXISTS continuum_continuation (
    id VARCHAR2(36) NOT NULL PRIMARY KEY,
    computation_id VARCHAR2(36) NOT NULL REFERENCES continuum_computation (id),
    payload BLOB NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_continuation_computation
    ON continuum_continuation (computation_id);

CREATE TABLE IF NOT EXISTS continuum_result (
    computation_id VARCHAR2(36) NOT NULL PRIMARY KEY,
    kind VARCHAR2(200) NOT NULL,
    outcome_type VARCHAR2(20) NOT NULL,
    outcome_payload BLOB,
    expiry_kind VARCHAR2(20),
    message CLOB,
    deadline_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    attempt_count INT NOT NULL,
    submitted_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_result_kind_completed
    ON continuum_result (kind, completed_at);

CREATE TABLE IF NOT EXISTS continuum_outbox (
    id VARCHAR2(36) NOT NULL PRIMARY KEY,
    computation_id VARCHAR2(36) NOT NULL,
    continuation_id VARCHAR2(36) NOT NULL,
    kind VARCHAR2(200) NOT NULL,
    continuation_payload BLOB NOT NULL,
    outcome_type VARCHAR2(20) NOT NULL,
    outcome_payload BLOB,
    expiry_kind VARCHAR2(20),
    message CLOB,
    available_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    claimed_by VARCHAR2(200),
    claimed_until TIMESTAMP(6) WITH TIME ZONE,
    attempt_count INT NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    submitted_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_outbox_kind_available
    ON continuum_outbox (kind, available_at);
