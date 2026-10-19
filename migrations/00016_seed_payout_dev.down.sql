-- 00016_seed_payout_dev.down.sql
DELETE FROM streamer_bank_accounts WHERE palantir_ref IN ('pivot-bank-dev-1', 'pivot-bank-dev-2');