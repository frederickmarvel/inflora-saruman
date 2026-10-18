-- Admin identity: role-bearing administrative users and their opaque sessions.
-- Ownership: Saruman owns schema (D-002); Tolkien reads/writes these tables as
-- the authenticated admin facade.

CREATE TYPE admin_role AS ENUM ('ADMIN', 'FINOPS', 'OPS');

CREATE TABLE admin_users (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email           VARCHAR(320) NOT NULL UNIQUE,
  display_name    VARCHAR(80) NOT NULL,
  password_hash   VARCHAR(255) NOT NULL,        -- Argon2id PHC (shared auth)
  role            admin_role NOT NULL DEFAULT 'ADMIN',
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_login_at   TIMESTAMPTZ
);

CREATE TABLE admin_sessions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  admin_user_id   UUID NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
  token_hash      VARCHAR(255) NOT NULL UNIQUE, -- bcrypt(admin token)
  last4           CHAR(4) NOT NULL,
  expires_at      TIMESTAMPTZ NOT NULL,
  last_used_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_at      TIMESTAMPTZ
);

CREATE INDEX idx_admin_sessions_user ON admin_sessions(admin_user_id);
CREATE INDEX idx_admin_sessions_last4 ON admin_sessions(last4);
