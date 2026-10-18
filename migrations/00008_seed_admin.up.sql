-- Development seed: one admin account for local testing.
-- Gated like seed_dev: only applied when --seed is passed AND ENV=dev/test.
-- Password: inflora-admin

INSERT INTO admin_users (id, email, display_name, password_hash, role, is_active)
VALUES (
  '50000000-0000-4000-8000-000000000001',
  'admin@inflora.local',
  'Inflora Admin',
  '$argon2id$v=19$m=65536,t=3,p=2$ciZ0F/Bv0GiSK0Qta5Yrjg$GTaMI4fx4qDM9ADcIDk54+tb/2wVlAOQUPwSVoNRDyU',
  'ADMIN',
  TRUE
)
ON CONFLICT (id) DO NOTHING;
