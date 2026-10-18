-- 00012_ledger_source_tag.down.sql
DROP INDEX IF EXISTS idx_ledger_transactions_tag_created;
ALTER TABLE ledger_transactions DROP CONSTRAINT IF EXISTS chk_ledger_transactions_source_tag;
ALTER TABLE ledger_transactions DROP COLUMN IF EXISTS source_tag;