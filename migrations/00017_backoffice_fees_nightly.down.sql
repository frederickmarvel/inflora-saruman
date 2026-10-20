-- 00017_backoffice_fees_nightly.down.sql
-- Reverse of the backoffice migration (dev/test only, like every other
-- down file). Ordering: role_permissions grants -> permission rows ->
-- nightly-batch index -> platform_fee_bps column.
-- Transactionality: like every down file in this repo, this runs inside
-- the runner's own transaction, so no BEGIN/COMMIT appears here.

DELETE FROM role_permissions
WHERE permission_id IN (
  SELECT p.id FROM permissions p
  WHERE p.name IN ('Streamer:Read', 'Streamer:Write', 'MdrRate:Read', 'MdrRate:Write')
)
  AND role_id IN (SELECT r.id FROM roles r WHERE r.name IN ('ADMIN', 'SUPERADMIN'));

DELETE FROM permissions
WHERE name IN ('Streamer:Read', 'Streamer:Write', 'MdrRate:Read', 'MdrRate:Write');

DROP INDEX IF EXISTS uq_payout_batches_nightly_run;

ALTER TABLE streamer_settings DROP COLUMN IF EXISTS platform_fee_bps;