-- 00014_payout_withdrawals.down.sql
-- Enum values cannot be dropped in place (the reason down migrations are
-- gated to dev/test): both types are rebuilt without the Phase C values.
-- Rows holding the dropped values cannot exist in a clean dev/test rollback.
--
-- Ordering constraints discovered by the pre-00014 integration test:
--  * CREATE OR REPLACE VIEW cannot drop columns (42P16) and a view rule pins
--    the base column type (cannot alter type of a column used by a view), so
--    v_streamer_balances is DROPPED first and recreated as the LAST statement.
--  * ALTER COLUMN TYPE rewrites dependent index predicates with the OLD
--    enum-typed literals (operator text = payout_status), so the payout
--    partial index must be dropped before the type rebuild.

-- No NEEDS_REVIEW row may survive a rollback (mid-flight payouts only).
DELETE FROM payouts WHERE status = 'NEEDS_REVIEW';
-- DISBURSED accounts exist only after a payout completion posting.
DELETE FROM ledger_accounts WHERE account_type = 'DISBURSED';

DROP VIEW IF EXISTS v_streamer_balances;

-- Payout-status enum rebuild: drop dependent indexes first (see header note).
DROP INDEX IF EXISTS idx_payouts_status_requested;
DROP INDEX IF EXISTS idx_payouts_streamer_status_time;
ALTER TABLE payouts ALTER COLUMN status TYPE text;
ALTER TABLE payouts ALTER COLUMN status DROP DEFAULT;
DROP TYPE payout_status;
CREATE TYPE payout_status AS ENUM ('REQUESTED', 'BATCHED', 'PROCESSING', 'SETTLED', 'FAILED');
ALTER TABLE payouts ALTER COLUMN status TYPE payout_status USING status::payout_status;
ALTER TABLE payouts ALTER COLUMN status SET DEFAULT 'REQUESTED';
CREATE INDEX idx_payouts_status_requested ON payouts (status) WHERE status IN ('REQUESTED', 'BATCHED');
CREATE INDEX idx_payouts_streamer_status_time ON payouts (streamer_id, status, requested_at DESC);

ALTER TABLE reconciliation_drift DROP COLUMN IF EXISTS kind;

DROP INDEX IF EXISTS uq_gw_withdrawal_provider_payout;
ALTER TABLE gateway_withdrawals
  DROP COLUMN IF EXISTS provider_name,
  DROP COLUMN IF EXISTS provider_payout_id,
  DROP COLUMN IF EXISTS bank_account_ref,
  DROP COLUMN IF EXISTS status_checked_at,
  DROP COLUMN IF EXISTS last_known_provider_status;

ALTER TABLE payout_batches
  DROP COLUMN IF EXISTS submitted_by,
  DROP COLUMN IF EXISTS approved_by,
  DROP COLUMN IF EXISTS paused,
  DROP COLUMN IF EXISTS resolution_note;

ALTER TABLE payouts DROP CONSTRAINT IF EXISTS fk_payouts_batch;
ALTER TABLE payouts
  DROP COLUMN IF EXISTS attempts,
  DROP COLUMN IF EXISTS lease_expires_at,
  DROP COLUMN IF EXISTS last_event_id;

-- Ledger-enum rebuild without DISBURSED.
DELETE FROM ledger_entries WHERE entry_type = 'STREAMER_DISBURSED_CREDIT';
ALTER TABLE ledger_accounts ALTER COLUMN account_type TYPE text;
ALTER TABLE reconciliation_drift ALTER COLUMN account_type TYPE text;
ALTER TABLE ledger_entries ALTER COLUMN entry_type TYPE text;
DROP TYPE ledger_account_type;
DROP TYPE ledger_entry_type;
CREATE TYPE ledger_account_type AS ENUM (
  'DONOR_CASH',
  'STREAMER_PENDING',
  'STREAMER_AVAILABLE',
  'STREAMER_PAID',
  'PLATFORM_REVENUE',
  'MDR_REVENUE'
);
CREATE TYPE ledger_entry_type AS ENUM (
  'DONATION_DEPOSIT',
  'STREAMER_PENDING_CREDIT',
  'STREAMER_AVAILABLE_CREDIT',
  'PLATFORM_FEE',
  'MDR_FEE',
  'STREAMER_PAID_DEBIT',
  'REFUND_DEBIT',
  'REFUND_CREDIT',
  'HOLD_DEBIT',
  'HOLD_CREDIT'
);
ALTER TABLE ledger_accounts ALTER COLUMN account_type TYPE ledger_account_type USING account_type::ledger_account_type;
ALTER TABLE reconciliation_drift ALTER COLUMN account_type TYPE ledger_account_type USING account_type::ledger_account_type;
ALTER TABLE ledger_entries ALTER COLUMN entry_type TYPE ledger_entry_type USING entry_type::ledger_entry_type;

-- Recreate the balance view exactly as 00001 built it (no disbursed column).
CREATE VIEW v_streamer_balances AS
SELECT
  s.id AS streamer_id,
  s.display_name,
  s.is_active,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_PENDING'    THEN la.balance_idr ELSE 0 END), 0) AS pending_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_AVAILABLE'  THEN la.balance_idr ELSE 0 END), 0) AS available_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_PAID'       THEN la.balance_idr ELSE 0 END), 0) AS paid_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'PLATFORM_REVENUE'   THEN la.balance_idr ELSE 0 END), 0) AS platform_idr,
  COALESCE((
    SELECT SUM(amount_idr)
    FROM fund_holds fh
    WHERE fh.streamer_id = s.id AND fh.status = 'ACTIVE' AND fh.target_type = 'STREAMER'
  ), 0) AS held_idr
FROM streamers s
LEFT JOIN ledger_accounts la ON la.streamer_id = s.id
GROUP BY s.id, s.display_name, s.is_active;