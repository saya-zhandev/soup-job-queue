CREATE TYPE job_status AS ENUM (
    'queued',
    'running',
    'succeeded',
    'failed',
    'cancelled'
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL UNIQUE,
    status job_status NOT NULL DEFAULT 'queued',
    payload JSONB NOT NULL,
    worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    side_effect_count INTEGER NOT NULL DEFAULT 0 CHECK (side_effect_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_queued_polling_idx
    ON jobs (created_at, id)
    WHERE status = 'queued';

CREATE INDEX jobs_expired_leases_idx
    ON jobs (lease_expires_at, id)
    WHERE status = 'running' AND lease_expires_at IS NOT NULL;
