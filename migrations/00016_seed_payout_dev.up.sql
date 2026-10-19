-- 00016_seed_payout_dev.up.sql
-- Dev-only verified bank rows so the Phase C E2E (dev_withdrawal.py) can
-- close REQUESTED -> PROCESSING -> SETTLED without manual bank verification.
-- seed_ files are only applied with ENV=dev/test AND the --seed flag.
-- NOTE (D-046): 00015 is reserved for Phase-A identities/RBAC. Applied
-- migrations are checksum-stamped and are never renumbered.

INSERT INTO streamer_bank_accounts
  (streamer_id, bank_code, account_number_last4, account_name, is_primary, is_verified, palantir_ref)
SELECT s.id, 'BCA', '0001', s.display_name || ' Payout', TRUE, TRUE, 'pivot-bank-dev-1'
FROM streamers s
WHERE s.id = '00000000-0000-4000-8000-000000000001'
  AND NOT EXISTS (SELECT 1 FROM streamer_bank_accounts WHERE palantir_ref = 'pivot-bank-dev-1');

INSERT INTO streamer_bank_accounts
  (streamer_id, bank_code, account_number_last4, account_name, is_primary, is_verified, palantir_ref)
SELECT s.id, 'BNI', '0002', s.display_name || ' Payout', TRUE, TRUE, 'pivot-bank-dev-2'
FROM streamers s
WHERE s.id = '00000000-0000-4000-8000-000000000002'
  AND NOT EXISTS (SELECT 1 FROM streamer_bank_accounts WHERE palantir_ref = 'pivot-bank-dev-2');