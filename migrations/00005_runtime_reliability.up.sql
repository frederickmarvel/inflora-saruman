-- Runtime idempotency lifecycle state and timestamps for stale-claim recovery.
-- Add the columns with defaults first so existing rows remain writable while
-- their lifecycle values are reconstructed from the legacy response fields.
ALTER TABLE idempotency
  ADD COLUMN state VARCHAR(16) DEFAULT 'IN_PROGRESS',
  ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();

UPDATE idempotency
SET state = CASE
      WHEN processed_at IS NOT NULL
        OR response_status IS NOT NULL
        OR response_body IS NOT NULL THEN 'COMPLETED'
      ELSE 'IN_PROGRESS'
    END,
    updated_at = COALESCE(processed_at, created_at, NOW());

ALTER TABLE idempotency
  ALTER COLUMN state SET DEFAULT 'IN_PROGRESS',
  ALTER COLUMN state SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET NOT NULL,
  ADD CONSTRAINT chk_idempotency_state CHECK (state IN ('IN_PROGRESS', 'COMPLETED'));

-- Recovery scans use updated_at to find stale claims without indexing completed
-- records.
CREATE INDEX idx_idempotency_in_progress
  ON idempotency (updated_at)
  WHERE state = 'IN_PROGRESS';