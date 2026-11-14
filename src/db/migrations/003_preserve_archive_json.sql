-- Preserve JSON strings containing escapes that jsonb cannot represent.
ALTER TABLE dead_letter ALTER COLUMN payload TYPE json USING payload::json;
ALTER TABLE jobs_ledger ALTER COLUMN detail TYPE json USING detail::json;
