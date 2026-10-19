-- 00013_supporter_identity.up.sql
-- CONTRACT_DRIFT D-035/D-044 (A-3 column variant, owner-acked 2026-10-18):
-- physical supporter identity table so donations can carry an optional
-- supporter_id FK. AUTH IS DEFERRED to Phase A (Firebase ID tokens, owner
-- decision 2026-10-18: no password auth now) — there is deliberately no
-- password_hash column and no supporter session table yet; the frozen
-- token_purpose enum stays exactly (SESSION, OVERLAY). Writers: supporters
-- rows will be written by tolkien (users-surface precedent, like the 00007
-- admin tables) once Phase-A authentication lands; the donations columns are
-- claimed by saruman's internal claim endpoint in the same phase. Until then
-- both are write-free: guest tips stay anonymous, `supporter_id` stays NULL.
-- firebase_uid is the Phase-A v_identities bridge (00015 RBAC/identities).

CREATE TABLE supporters (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email         VARCHAR(320) NOT NULL UNIQUE,   -- app lowercases, streamers parity
  display_name  VARCHAR(80)  NOT NULL,
  firebase_uid  VARCHAR(255) UNIQUE,            -- Phase-A identity bridge (A-3)
  is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
  last_login_at TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE donations
  ADD COLUMN supporter_id UUID REFERENCES supporters(id) ON DELETE SET NULL,
  ADD COLUMN claimed_at    TIMESTAMPTZ;

-- The supporter's own tip-history read: rows with a claimed identity.
CREATE INDEX idx_donations_supporter
  ON donations (supporter_id)
  WHERE supporter_id IS NOT NULL;