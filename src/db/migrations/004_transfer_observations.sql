CREATE TABLE transfer_executions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL,
    worker_id text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX transfer_executions_job_idx ON transfer_executions (job_id);

-- This audit deliberately permits duplicates so a measurement can detect them.
CREATE TABLE transfer_effects (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL,
    step_key text NOT NULL CHECK (step_key IN ('debit', 'credit', 'receipt')),
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX transfer_effects_job_idx ON transfer_effects (job_id);
