-- 00015_firebase_identities_rbac.down.sql
-- Complete reverse of the Phase-A identities/RBAC schema (dev/test only,
-- like every other down migration).
--
-- Ordering constraints:
--  * v_identities must be dropped before principal_kind (the view pins
--    the type).
--  * the four RBAC tables must be dropped before app_role (roles.name
--    uses the enum).
--  * password_hash NOT NULL is restored only when no Firebase-managed
--    row (NULL password_hash) remains; otherwise the rollback raises a
--    clear exception instead of silently restoring a guard that would
--    reject remaining rows. On a clean dev/test lifecycle the seeded
--    admin still has a password_hash, so the guard passes.
-- The legacy admin_users.role column and its admin_role enum are
-- untouched in both directions (01-firebase-rbac.md §6/§8 rollback
-- window; the enum is retired after the telemetry gate).
-- Transactionality: like every down file in this repo, this runs inside
-- the runner's own transaction (internal/migrate/runner.go BeginTx/
-- Commit around each migration), so no BEGIN/COMMIT appears here.

DROP VIEW IF EXISTS v_identities;

-- Remove exactly the ROLE_MIGRATION audit rows the up file wrote (marked
-- metadata->>'migration'='00015') before dropping the tables they annotate:
-- an up/down/up cycle must re-journal the mapping once, not twice.
DELETE FROM audit_log
WHERE action = 'ROLE_MIGRATION'
  AND metadata->>'migration' = '00015';

DROP TABLE IF EXISTS admin_roles;
DROP TABLE IF EXISTS role_permissions;
DROP TABLE IF EXISTS permissions;
DROP TABLE IF EXISTS roles;

-- Guarded NOT NULL restore (see header note).
DO $$
DECLARE null_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO null_count
  FROM admin_users
  WHERE password_hash IS NULL;
  IF null_count > 0 THEN
    RAISE EXCEPTION
      '00015 down: cannot restore password_hash NOT NULL: % admin_users row(s) have NULL password_hash (Firebase-managed accounts)',
      null_count;
  END IF;
END $$;

ALTER TABLE admin_users ALTER COLUMN password_hash SET NOT NULL;

ALTER TABLE admin_users DROP COLUMN IF EXISTS firebase_uid;
ALTER TABLE streamers   DROP COLUMN IF EXISTS firebase_uid;

DROP TYPE IF EXISTS principal_kind;
DROP TYPE IF EXISTS app_role;