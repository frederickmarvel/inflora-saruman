-- 00012_ledger_source_tag.up.sql
-- CONTRACT_DRIFT D-032: tag ledger transactions with the assignable revenue
-- stream they belong to, so the creator earnings breakdown (tolkien balance
-- `months` array; React EarningsSummary entity) can split donations /
-- campaigns / clips. Additive and nullable: existing writers keep working,
-- and a row an unknown writer leaves untagged simply stays out of the split.
-- Values must stay in sync with internal/ledger/source.go and the React
-- EarningSource union ('campaigns' | 'clips' | 'donations').

ALTER TABLE ledger_transactions ADD COLUMN source_tag VARCHAR(16);

ALTER TABLE ledger_transactions ADD CONSTRAINT chk_ledger_transactions_source_tag
  CHECK (source_tag IN ('donation', 'campaign', 'clip'));

-- Retro-tag history: the only capture path ever run to date is donation
-- capture, so every existing row is a donation. New writers tag themselves.
UPDATE ledger_transactions SET source_tag = 'donation' WHERE source_tag IS NULL;

-- Serves the earnings scanner: streamer + month window.
CREATE INDEX idx_ledger_transactions_tag_created
  ON ledger_transactions (streamer_id, created_at)
  WHERE source_tag IS NOT NULL;