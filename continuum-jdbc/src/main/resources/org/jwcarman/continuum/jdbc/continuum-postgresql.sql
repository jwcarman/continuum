-- TIMESTAMP(6) WITH TIME ZONE is spelled out rather than the TIMESTAMPTZ alias
-- deliberately: identical on PostgreSQL, and it lets this same file serve H2
-- (test/embedded), which rejects the alias.
--
-- Naming convention:
--   submitted_at  when the computation was submitted (a domain fact, carried onto
--                 every row describing that computation)
--   completed_at  when the computation reached its terminal outcome
--   created_at    when THIS row was written (bookkeeping only, never a domain fact)

CREATE TABLE IF NOT EXISTS continuum_computation (
    id UUID PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    deadline_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    dispatch_payload BYTEA,
    attempt_count INT NOT NULL,
    submitted_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    last_updated_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_computation_kind_deadline
    ON continuum_computation (kind, deadline_at);

CREATE TABLE IF NOT EXISTS continuum_continuation (
    id UUID PRIMARY KEY,
    computation_id UUID NOT NULL REFERENCES continuum_computation (id),
    payload BYTEA NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_continuation_computation
    ON continuum_continuation (computation_id);

CREATE TABLE IF NOT EXISTS continuum_result (
    computation_id UUID PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload BYTEA,
    expiry_kind VARCHAR(20),
    message TEXT,
    deadline_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    attempt_count INT NOT NULL,
    submitted_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_result_kind_completed
    ON continuum_result (kind, completed_at);

CREATE TABLE IF NOT EXISTS continuum_outbox (
    id UUID PRIMARY KEY,
    computation_id UUID NOT NULL,
    continuation_id UUID NOT NULL,
    kind VARCHAR(200) NOT NULL,
    continuation_payload BYTEA NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload BYTEA,
    expiry_kind VARCHAR(20),
    message TEXT,
    available_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    claimed_by VARCHAR(200),
    claimed_until TIMESTAMP(6) WITH TIME ZONE,
    attempt_count INT NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    submitted_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_outbox_kind_available
    ON continuum_outbox (kind, available_at);
