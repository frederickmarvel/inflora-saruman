DROP INDEX IF EXISTS idx_idempotency_in_progress;
ALTER TABLE idempotency DROP CONSTRAINT IF EXISTS chk_idempotency_state;
ALTER TABLE idempotency
  DROP COLUMN IF EXISTS updated_at,
  DROP COLUMN IF EXISTS state;