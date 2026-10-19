-- 00013_supporter_identity.down.sql
-- Identity metadata only: dropping the columns returns rows to anonymous
-- guest tips. No enum changes anywhere in this migration, so the down is
-- complete and clean.
DROP INDEX IF EXISTS idx_donations_supporter;
ALTER TABLE donations DROP COLUMN IF EXISTS claimed_at;
ALTER TABLE donations DROP COLUMN IF EXISTS supporter_id;
DROP TABLE IF EXISTS supporters;