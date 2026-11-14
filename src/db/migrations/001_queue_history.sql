CREATE TABLE jobs_ledger (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    job_id uuid NOT NULL,
    queue_name text NOT NULL,
    "from" text CHECK ("from" IN ('ready', 'delayed', 'in_flight', 'succeeded', 'dead')),
    "to" text NOT NULL CHECK ("to" IN ('ready', 'delayed', 'in_flight', 'succeeded', 'dead', 'purged')),
    at timestamptz NOT NULL,
    worker_id text,
    attempt integer NOT NULL CHECK (attempt >= 0),
    detail jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX jobs_ledger_job_history_idx ON jobs_ledger (job_id, at, id);

CREATE TABLE dead_letter (
    job_id uuid NOT NULL,
    replay_count integer NOT NULL CHECK (replay_count >= 0),
    queue_name text NOT NULL,
    job_type text NOT NULL,
    payload jsonb NOT NULL,
    last_error text NOT NULL,
    stack text,
    attempts integer NOT NULL CHECK (attempts > 0),
    dead_at timestamptz NOT NULL,
    replayed_at timestamptz,
    PRIMARY KEY (job_id, replay_count)
);

CREATE INDEX dead_letter_queue_time_idx ON dead_letter (queue_name, dead_at DESC);

