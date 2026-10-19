-- 00014_payout_withdrawals.up.sql
-- CONTRACT_DRIFT D-047..D-058 + D-004/D-005/D-006 (implementation):
-- Phase C payouts. Grows the frozen shape without renumbering anything:
-- two enum values, payout claim/lease columns for the provider-lease sweep,
-- batch custody columns, gateway-withdrawal provider mirror columns, and the
-- reconciliation kind deferral. Numbering is PINNED (D-046): 00015 is
-- reserved for Phase-A identities/RBAC and is never used here.
-- Writer map: saruman owns payouts/payout_batches/ledger_*; palantir owns
-- gateway_withdrawals. Applied migrations are checksum-stamped and are
-- never renumbered.

-- Provider-lease safety net (D-057) and the settle-time hold-gate terminal
-- (D-056) need NEEDS_REVIEW as a payout state.
ALTER TYPE payout_status ADD VALUE 'NEEDS_REVIEW' AFTER 'FAILED';

-- Payout completion credits this account (D-048): money that has left the
-- streamer balance for the provider but is not yet bank-settled.
ALTER TYPE ledger_account_type ADD VALUE 'DISBURSED' AFTER 'STREAMER_PAID';

-- The amended §4 completion posting credits DISBURSED under this marker
-- (D-048); the historical §3/§5 STREAMER_PAID_DEBIT legs are unchanged.
ALTER TYPE ledger_entry_type ADD VALUE 'STREAMER_DISBURSED_CREDIT' AFTER 'STREAMER_PAID_DEBIT';

-- Payout claim/lease columns (D-055/D-057): attempts counter for the sweep,
-- lease for provider-attempt liveness, last_event_id for event dedupe.
ALTER TABLE payouts
  ADD COLUMN attempts         INT NOT NULL DEFAULT 0,
  ADD COLUMN lease_expires_at TIMESTAMPTZ,
  ADD COLUMN last_event_id    UUID;

-- batch_id becomes a physical FK (03-withdrawals): claimed FOR UPDATE, never
-- deferred. Intent-preserving vs the old plain-UUID comment (D-046).
ALTER TABLE payouts
  ADD CONSTRAINT fk_payouts_batch FOREIGN KEY (batch_id) REFERENCES payout_batches(id);

-- Batch custody columns (D-054: ak_ voucher custody at tolkien, rows stay
-- saruman-owned for all money effects).
ALTER TABLE payout_batches
  ADD COLUMN submitted_by   UUID,
  ADD COLUMN approved_by    UUID,
  ADD COLUMN paused         BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN resolution_note TEXT;

-- Provider mirror columns (palantir writer; saruman reads only).
ALTER TABLE gateway_withdrawals
  ADD COLUMN provider_name                 VARCHAR(40),
  ADD COLUMN provider_payout_id            VARCHAR(128),
  ADD COLUMN bank_account_ref              VARCHAR(128),
  ADD COLUMN status_checked_at             TIMESTAMPTZ,
  ADD COLUMN last_known_provider_status    VARCHAR(20);

-- Dev pivot is the only provider that has ever written withdrawals.
UPDATE gateway_withdrawals SET provider_name = 'pivot' WHERE provider_name IS NULL;

-- One provider attempt per payout, enforced at the mirror.
CREATE UNIQUE INDEX uq_gw_withdrawal_provider_payout
  ON gateway_withdrawals (provider_name, saruman_payout_id);

-- Recon deferral (D-058): NULL = ledger semantics (the only rows Phase C
-- writes); 'WITHDRAWAL' = mobile-money drift rows, owned by the Phase D
-- nightly recon worker.
ALTER TABLE reconciliation_drift ADD COLUMN kind VARCHAR(20);

-- The balance surface gains the disbursed tracker (D-048).
CREATE OR REPLACE VIEW v_streamer_balances AS
SELECT
  s.id AS streamer_id,
  s.display_name,
  s.is_active,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_PENDING'    THEN la.balance_idr ELSE 0 END), 0) AS pending_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_AVAILABLE'  THEN la.balance_idr ELSE 0 END), 0) AS available_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_PAID'       THEN la.balance_idr ELSE 0 END), 0) AS paid_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'PLATFORM_REVENUE'   THEN la.balance_idr ELSE 0 END), 0) AS platform_idr,
  -- Active holds reduce available
  COALESCE((
    SELECT SUM(amount_idr)
    FROM fund_holds fh
    WHERE fh.streamer_id = s.id AND fh.status = 'ACTIVE' AND fh.target_type = 'STREAMER'
  ), 0) AS held_idr,
  -- CREATE OR REPLACE VIEW can only append columns; disbursed tracks the
  -- money that left the balance for the provider but is not yet bank-settled.
  -- 'DISBURSED' was ALTER TYPE ADD VALUE'd earlier in THIS migration; the
  -- runner applies each migration in one transaction and PostgreSQL forbids
  -- resolving a freshly-added enum label in the same transaction (55P04), so
  -- the comparison goes through a text cast instead.
  COALESCE(SUM(CASE WHEN la.account_type::text = 'DISBURSED'    THEN la.balance_idr ELSE 0 END), 0) AS disbursed_idr
FROM streamers s
LEFT JOIN ledger_accounts la ON la.streamer_id = s.id
GROUP BY s.id, s.display_name, s.is_active;