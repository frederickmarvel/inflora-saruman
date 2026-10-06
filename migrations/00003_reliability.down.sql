-- ============================================================================
-- Phase 1 reliability schema — DOWN migration.
-- Reverses 00003_reliability.up.sql. This migration is dev-only and is
-- only allowed in ENV=dev|test or with ALLOW_DOWN_MIGRATIONS=true.
-- ============================================================================

DROP TABLE IF EXISTS ws_projection_frames;
DROP TABLE IF EXISTS ws_projection_sequences;
DROP TABLE IF EXISTS ws_projection_inbox;
DROP TABLE IF EXISTS event_dead_letters;
DROP TABLE IF EXISTS consumer_inbox;
DROP TABLE IF EXISTS outbox_events;

-- ledger_entries: drop FK + NOT NULL + check, then ledger_transactions.
ALTER TABLE ledger_entries DROP CONSTRAINT IF EXISTS chk_amount_positive;
ALTER TABLE ledger_entries DROP CONSTRAINT IF EXISTS fk_ledger_entries_ledger_transaction_id;
DROP INDEX IF EXISTS idx_ledger_entries_tx;
ALTER TABLE ledger_entries DROP COLUMN IF EXISTS ledger_transaction_id;

DROP TABLE IF EXISTS ledger_transactions;