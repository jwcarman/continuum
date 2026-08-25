-- Reference DDL for SQL Server 2012+ (OFFSET/FETCH needs 2012).
--
-- Plain CREATE rather than IF NOT EXISTS: SQL Server's conditional form is a
-- separate IF statement wrapping each CREATE, which reads worse than saying
-- "run this once". Type mapping from the PostgreSQL reference, and why:
--   UUID        -> CHAR(36)         identities are UUIDv7; canonical text sorts as bytes
--   TIMESTAMPTZ -> DATETIME2(6)     no zone; every value is read and written through the
--                                   same JDBC conversion, and the database itself never does
--                                   time arithmetic on these columns
--   BYTEA       -> VARBINARY(MAX)   opaque bytes of unbounded size
--   TEXT        -> VARCHAR(MAX)     diagnostic prose of unbounded size
--
-- Naming convention (same as the PostgreSQL reference):
--   submitted_at  when the computation was submitted (a domain fact)
--   completed_at  when the computation reached its terminal outcome
--   created_at    when THIS row was written (bookkeeping only)

CREATE TABLE continuum_computation (
    id CHAR(36) NOT NULL PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    deadline_at DATETIME2(6) NOT NULL,
    dispatch_payload VARBINARY(MAX),
    attempt_count INT NOT NULL,
    submitted_at DATETIME2(6) NOT NULL,
    last_updated_at DATETIME2(6) NOT NULL
);
CREATE INDEX idx_continuum_computation_kind_deadline
    ON continuum_computation (kind, deadline_at);

CREATE TABLE continuum_continuation (
    id CHAR(36) NOT NULL PRIMARY KEY,
    computation_id CHAR(36) NOT NULL REFERENCES continuum_computation (id),
    payload VARBINARY(MAX) NOT NULL,
    created_at DATETIME2(6) NOT NULL
);
CREATE INDEX idx_continuum_continuation_computation
    ON continuum_continuation (computation_id);

CREATE TABLE continuum_result (
    computation_id CHAR(36) NOT NULL PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload VARBINARY(MAX),
    expiry_kind VARCHAR(20),
    message VARCHAR(MAX),
    deadline_at DATETIME2(6) NOT NULL,
    attempt_count INT NOT NULL,
    submitted_at DATETIME2(6) NOT NULL,
    completed_at DATETIME2(6) NOT NULL
);
CREATE INDEX idx_continuum_result_kind_completed
    ON continuum_result (kind, completed_at);

CREATE TABLE continuum_outbox (
    id CHAR(36) NOT NULL PRIMARY KEY,
    computation_id CHAR(36) NOT NULL,
    continuation_id CHAR(36) NOT NULL,
    kind VARCHAR(200) NOT NULL,
    continuation_payload VARBINARY(MAX) NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload VARBINARY(MAX),
    expiry_kind VARCHAR(20),
    message VARCHAR(MAX),
    available_at DATETIME2(6) NOT NULL,
    claimed_by VARCHAR(200),
    claimed_until DATETIME2(6),
    attempt_count INT NOT NULL,
    created_at DATETIME2(6) NOT NULL,
    submitted_at DATETIME2(6) NOT NULL,
    completed_at DATETIME2(6) NOT NULL
);
CREATE INDEX idx_continuum_outbox_kind_available
    ON continuum_outbox (kind, available_at);
