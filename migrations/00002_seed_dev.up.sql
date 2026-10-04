INSERT INTO streamers (id, email, display_name, password_hash, is_active, is_verified)
VALUES
  ('00000000-0000-4000-8000-000000000001', 'dev-streamer-1@inflora.local', 'Dev Streamer 1', '$2a$12$C6UzMDM.H6dfI/f/IKcEe.5e5Sg9Dk8XpJcQm5N2HnNFXzXQwKkqS', TRUE, TRUE),
  ('00000000-0000-4000-8000-000000000002', 'dev-streamer-2@inflora.local', 'Dev Streamer 2', '$2a$12$C6UzMDM.H6dfI/f/IKcEe.5e5Sg9Dk8XpJcQm5N2HnNFXzXQwKkqS', TRUE, TRUE),
  ('00000000-0000-4000-8000-000000000003', 'dev-streamer-3@inflora.local', 'Dev Streamer 3', '$2a$12$C6UzMDM.H6dfI/f/IKcEe.5e5Sg9Dk8XpJcQm5N2HnNFXzXQwKkqS', TRUE, TRUE),
  ('00000000-0000-4000-8000-000000000004', 'dev-streamer-4@inflora.local', 'Dev Streamer 4', '$2a$12$C6UzMDM.H6dfI/f/IKcEe.5e5Sg9Dk8XpJcQm5N2HnNFXzXQwKkqS', TRUE, TRUE),
  ('00000000-0000-4000-8000-000000000005', 'dev-streamer-5@inflora.local', 'Dev Streamer 5', '$2a$12$C6UzMDM.H6dfI/f/IKcEe.5e5Sg9Dk8XpJcQm5N2HnNFXzXQwKkqS', TRUE, TRUE)
ON CONFLICT (id) DO NOTHING;

INSERT INTO streamer_settings (id, streamer_id)
SELECT
  ('10000000-0000-4000-8000-' || lpad(right(s.id::text, 12), 12, '0'))::uuid,
  s.id
FROM streamers s
WHERE s.id IN (
  '00000000-0000-4000-8000-000000000001',
  '00000000-0000-4000-8000-000000000002',
  '00000000-0000-4000-8000-000000000003',
  '00000000-0000-4000-8000-000000000004',
  '00000000-0000-4000-8000-000000000005'
)
ON CONFLICT (streamer_id) DO NOTHING;

INSERT INTO ledger_accounts (streamer_id, account_type)
SELECT s.id, account_type
FROM streamers s
CROSS JOIN unnest(ARRAY[
  'DONOR_CASH'::ledger_account_type,
  'STREAMER_PENDING'::ledger_account_type,
  'STREAMER_AVAILABLE'::ledger_account_type,
  'STREAMER_PAID'::ledger_account_type,
  'PLATFORM_REVENUE'::ledger_account_type,
  'MDR_REVENUE'::ledger_account_type
]) AS account_type
WHERE s.id IN (
  '00000000-0000-4000-8000-000000000001',
  '00000000-0000-4000-8000-000000000002',
  '00000000-0000-4000-8000-000000000003',
  '00000000-0000-4000-8000-000000000004',
  '00000000-0000-4000-8000-000000000005'
)
ON CONFLICT (streamer_id, account_type, currency) DO NOTHING;

INSERT INTO donations (
  id, streamer_id, intent_id, amount_idr, mdr_idr, gross_charged_idr,
  platform_fee_idr, net_idr, status, settlement_status, payment_method,
  donor_display_name, message, display_duration_sec, expires_at
)
VALUES
  ('20000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000001', '21000000-0000-4000-8000-000000000001', 50000, 350, 50350, 2500, 47500, 'SETTLED', 'SETTLED', 'QRIS', 'Dev Donor', 'Semangat!', 5, NOW() + INTERVAL '1 day'),
  ('20000000-0000-4000-8000-000000000002', '00000000-0000-4000-8000-000000000002', '21000000-0000-4000-8000-000000000002', 100000, 700, 100700, 5000, 95000, 'CHARGED', 'PENDING', 'VIRTUAL_ACCOUNT', 'Anonymous', 'Hello from dev seed', 10, NOW() + INTERVAL '1 day')
ON CONFLICT (id) DO NOTHING;

INSERT INTO fund_holds (id, streamer_id, target_type, amount_idr, reason, status, created_by)
VALUES ('30000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000001', 'STREAMER', 10000, 'Development hold', 'ACTIVE', '00000000-0000-4000-8000-000000000001')
ON CONFLICT (id) DO NOTHING;

INSERT INTO payout_batches (id, status, payout_count, total_amount_idr, created_by)
VALUES ('40000000-0000-4000-8000-000000000001', 'DRAFT', 0, 0, '00000000-0000-4000-8000-000000000001')
ON CONFLICT (id) DO NOTHING;