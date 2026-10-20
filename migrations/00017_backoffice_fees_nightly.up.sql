-- 00017_backoffice_fees_nightly.up.sql
-- Marvel backoffice expansion (CONTRACT_DRIFT D-060..D-064, 2026-10-20):
--   1. Per-streamer platform fee (D-060): streamer_settings.platform_fee_bps.
--      Ingest snapshots it into donations.platform_fee_idr/net_idr at intent
--      creation; capture postings already honor those columns.
--   2. Nightly batch marker guard (D-062): at most ONE DRAFT payout_batches
--      row per UTC run_date may carry metadata.batch_kind = 'nightly'
--      (metadata JSONB, 00001). Enforced here by a unique partial index;
--      the nightly worker also checks before creating.
--   3. Four new permission rows for the backoffice surfaces (D-064):
--      Streamer:Read/Write (user list + per-streamer platform fee),
--      MdrRate:Read/Write (mdr_rates admin CRUD, D-061). New permissions
--      created AFTER 00015's role seed are NOT auto-granted, so the
--      ADMIN/SUPERADMIN role_permissions rows are added explicitly
--      (RBAC code is permission-name-data-only; no code change).

ALTER TABLE streamer_settings
  ADD COLUMN platform_fee_bps INTEGER NOT NULL DEFAULT 0
      CONSTRAINT chk_ss_platform_fee_bps CHECK (platform_fee_bps BETWEEN 0 AND 9999);

-- 9999 cap, not 10000: bps*amount/10000 would zero a 1-IDR-minimum net and
-- violate the frozen capture invariant net > 0 (saruman processor.go).

CREATE UNIQUE INDEX uq_payout_batches_nightly_run
  ON payout_batches ((metadata ->> 'run_date'))
  WHERE metadata ->> 'batch_kind' = 'nightly';

INSERT INTO permissions (name) VALUES
  ('Streamer:Read'),
  ('Streamer:Write'),
  ('MdrRate:Read'),
  ('MdrRate:Write');

-- GRANT both backoffice roles the four new permissions. The 00015 seeds
-- enumerate permissions by exclusion, so rows created later need explicit
-- grants (00015 comment: "the code checks permissions, not role names").
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM roles r
JOIN permissions p ON p.name IN (
  'Streamer:Read', 'Streamer:Write', 'MdrRate:Read', 'MdrRate:Write'
)
WHERE r.name IN ('ADMIN', 'SUPERADMIN');