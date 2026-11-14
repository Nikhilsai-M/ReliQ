CREATE TABLE idempotency_keys (
    job_id uuid NOT NULL,
    step_key text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, step_key)
);

CREATE TABLE accounts (
    id uuid PRIMARY KEY,
    balance_cents bigint NOT NULL CHECK (balance_cents >= 0)
);

CREATE TABLE transfers (
    job_id uuid PRIMARY KEY,
    debit_account_id uuid NOT NULL REFERENCES accounts (id),
    credit_account_id uuid NOT NULL REFERENCES accounts (id),
    amount_cents bigint NOT NULL CHECK (amount_cents > 0),
    CHECK (debit_account_id <> credit_account_id)
);

CREATE TABLE receipts (
    job_id uuid PRIMARY KEY REFERENCES transfers (job_id),
    created_at timestamptz NOT NULL DEFAULT now()
);

