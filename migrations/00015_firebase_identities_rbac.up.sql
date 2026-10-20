-- 00015_firebase_identities_rbac.up.sql
-- CONTRACT_DRIFT D-024/D-025 (implementation):
-- Phase A identities + database-authoritative RBAC. Numbering is PINNED
-- (D-046): 00015 is the Phase-A slot reserved by 00014's header comment.
-- The plan's "00009..00011" filenames (01-firebase-rbac.md) were
-- placeholders written before 00009..00011 were taken by other
-- migrations; the RECOMMENDED column variant (§4) is what ships here:
-- firebase_uid lives on streamers and admin_users (real FKs stay real)
-- and a v_identities view UNIONS them. Roles/permissions/role_permissions
-- are the authorization source; admin_roles binds backoffice operators to
-- roles. The streamer USER role is implicit (every active streamer IS a
-- USER); a streamer_roles table is unnecessary at three-role scope (§4).
-- Writer map: saruman owns all of this schema; tolkien reads it as the
-- authenticated backoffice facade.

-- Step 1: the Phase-A role enum. The legacy admin_role enum
-- {ADMIN, FINOPS, OPS} is deliberately left untouched through the
-- rollback window (01 §6/§8) and is retired after the telemetry gate.

CREATE TYPE app_role AS ENUM ('SUPERADMIN', 'ADMIN', 'USER');

-- Step 2: identity columns (01 §4 RECOMMENDED variant). CITEXT keeps
-- firebase_uid lookups case-insensitive like every other uid/email
-- surface. The extension is declared idempotently and pinned to public:
-- it may already exist in a pre-Phase-A database, and contract-diffing
-- tests that rebuild a bare schema between runs must find it (or create
-- it) in the one schema every apply shares.

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;

ALTER TABLE streamers   ADD COLUMN firebase_uid CITEXT UNIQUE;
ALTER TABLE admin_users ADD COLUMN firebase_uid CITEXT UNIQUE;

-- Firebase-managed backoffice accounts have no local password; the
-- break-glass account model (§8) keeps one. NULL password_hash paired
-- with a firebase_uid is the normal Phase-A row shape.
ALTER TABLE admin_users ALTER COLUMN password_hash DROP NOT NULL;

-- Step 3: principal discriminator + the identity union view. The type
-- must be created before the view references it. (No 55P04 hazard here:
-- that error concerns enum VALUES added inside the same transaction as
-- their first use, not freshly created types.)

CREATE TYPE principal_kind AS ENUM ('STREAMER', 'BACKOFFICE');

-- One place to answer "who is this Firebase user": firebase_uid ->
-- (principal_kind, principal_id). Only rows that actually linked a
-- Firebase identity appear; everyone else stays a profile-table row.
CREATE VIEW v_identities AS
SELECT id,
       firebase_uid,
       'STREAMER'::principal_kind AS principal_kind,
       id                          AS principal_id,
       email,
       created_at,
       updated_at
FROM streamers
WHERE firebase_uid IS NOT NULL
UNION ALL
SELECT id,
       firebase_uid,
       'BACKOFFICE'::principal_kind,
       id,
       email,
       created_at,
       updated_at
FROM admin_users
WHERE firebase_uid IS NOT NULL;

-- Step 4: authorization tables (01 §4). Roles are authoritative in the
-- database; the browser never supplies a role the server trusts.

CREATE TABLE roles (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        app_role NOT NULL UNIQUE,
  description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE permissions (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        VARCHAR(80) NOT NULL UNIQUE,
  description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE role_permissions (
  role_id       UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
  permission_id UUID NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
  PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE admin_roles (
  admin_user_id UUID NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
  role_id       UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
  -- granted_by is NO ACTION on delete deliberately: dropping an operator who
  -- granted roles must fail until the grants are reassigned or removed, so
  -- the audit trail of who-granted-what can never dangle. (Operationally
  -- awkward for account deletion; revisited by the Phase N cleanup rule set.)
  granted_by    UUID NOT NULL REFERENCES admin_users(id),
  granted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (admin_user_id, role_id)
);

-- Step 5: seed roles and the MVP permission matrix exactly per
-- 01-firebase-rbac.md §5. The §5 last row "Refund/Drain/RotateToken
-- (saruman facade)" is three separate permissions — Refund, Drain,
-- RotateToken — because the code checks permissions, not role names
-- (§5: "the code checks permissions, not role names").

INSERT INTO roles (name, description) VALUES
  ('SUPERADMIN', 'Backoffice owner: every permission including user/admin management and audit'),
  ('ADMIN',      'Backoffice operator: donation/hold/payout operations incl. privileged saruman facade'),
  ('USER',       'Streamer: own dashboard resources only');

INSERT INTO permissions (name) VALUES
  ('Me:Read'),
  ('Settings:Write'),
  ('BankAccount:Write'),
  ('OverlayToken:Rotate'),
  ('Payout:Create'),
  ('Donation:Read'),
  ('Donation:ReadAll'),
  ('Donation:Override'),
  ('FundHold:Create'),
  ('FundHold:Read'),
  ('FundHold:Release'),
  ('PayoutBatch:Create'),
  ('PayoutBatch:Execute'),
  ('Payout:Review'),
  ('Receipt:Resend'),
  ('User:Manage'),
  ('Admin:Manage'),
  ('Audit:Read'),
  ('Replay:Trigger'),
  ('Refund'),
  ('Drain'),
  ('RotateToken');

-- USER: the six §5 USER-column permissions (own resources only).
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM roles r
JOIN permissions p ON p.name IN (
  'Me:Read', 'Settings:Write', 'BankAccount:Write', 'OverlayToken:Rotate',
  'Payout:Create', 'Donation:Read'
)
WHERE r.name = 'USER';

-- ADMIN: everything except the four SUPERADMIN-only permissions
-- (User:Manage, Admin:Manage, Audit:Read, Replay:Trigger) and the four
-- streamer-scoped own-resource permissions the §5 matrix marks '—' for
-- every backoffice role (Settings:Write, BankAccount:Write,
-- OverlayToken:Rotate, Payout:Create).
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM roles r
JOIN permissions p ON p.name NOT IN (
  'User:Manage', 'Admin:Manage', 'Audit:Read', 'Replay:Trigger',
  'Settings:Write', 'BankAccount:Write', 'OverlayToken:Rotate', 'Payout:Create'
)
WHERE r.name = 'ADMIN';

-- SUPERADMIN: everything except the four streamer-scoped permissions
-- (§5 marks them '—' for SUPERADMIN as well: they gate streamer-own
-- resources only). The SUPERADMIN in-code implicit grant (httpapi
-- adminHasPerm) applies the same exclusion.
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM roles r
JOIN permissions p ON p.name NOT IN (
  'Settings:Write', 'BankAccount:Write', 'OverlayToken:Rotate', 'Payout:Create'
)
WHERE r.name = 'SUPERADMIN';

-- Step 6: legacy mapping (01 §6). Every existing admin_users row becomes
-- an 'ADMIN' backoffice operator with the full ADMIN set. The per-role
-- equivalent scoping of the §6 FINOPS/OPS profiles is FLATTENED at the
-- Phase-A cutover: every legacy operator receives ADMIN and the source
-- legacy_role is preserved ONLY in the audit metadata below; the scoped
-- FINOPS/OPS equivalents ship with the Phase N telemetry-gated cleanup
-- (after operators are re-provisioned against the matrix row-by-row).
-- Flattening is honest: the legacy admin_users.role column is NEVER read
-- for authorization (D-025) — only admin_roles rows grant permissions —
-- so a legacy FINOPS/OPS row would otherwise resolve to ZERO permissions,
-- which is the correct fail-closed posture until cleanup, not an
-- accidental lockout. Idempotent: re-running the statement never
-- double-grants, and audit rows are only written for newly mapped pairs.
-- granted_by is self-referential because the migration itself is the
-- (system) granter. The audit rows carry the migration marker so the down
-- file can remove exactly them (an up/down/up cycle must not re-journal
-- the same mapping). The legacy admin_users.role column stays untouched
-- (rollback window).

WITH mapped AS (
  INSERT INTO admin_roles (admin_user_id, role_id, granted_by, granted_at)
  SELECT a.id, r.id, a.id, NOW()
  FROM admin_users a
  JOIN roles r ON r.name = 'ADMIN'
  WHERE NOT EXISTS (
    SELECT 1
    FROM admin_roles ar
    WHERE ar.admin_user_id = a.id AND ar.role_id = r.id
  )
  RETURNING admin_user_id, role_id
)
INSERT INTO audit_log (actor_id, actor_type, action, resource_type, resource_id, ip_address, user_agent, metadata)
SELECT m.admin_user_id, 'ADMIN', 'ROLE_MIGRATION', 'ADMIN_ROLE', m.admin_user_id, NULL, NULL,
       jsonb_build_object('role', 'ADMIN', 'legacy_role', a.role::text, 'source', 'migration:00015', 'migration', '00015')
FROM mapped m
JOIN admin_users a ON a.id = m.admin_user_id;