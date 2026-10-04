DROP VIEW IF EXISTS v_pending_settlements;
DROP VIEW IF EXISTS v_donation_funnel;
DROP VIEW IF EXISTS v_recent_donations;
DROP VIEW IF EXISTS v_streamer_balances;

DROP TABLE IF EXISTS webhook_events;
DROP TABLE IF EXISTS gateway_refunds;
DROP TABLE IF EXISTS gateway_withdrawals;
DROP TABLE IF EXISTS gateway_topups;
DROP TABLE IF EXISTS reconciliation_drift;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS fraud_events;
DROP TABLE IF EXISTS mdr_rates;
DROP TABLE IF EXISTS email_receipts;
DROP TABLE IF EXISTS fund_holds;
DROP TABLE IF EXISTS payouts;
DROP TABLE IF EXISTS streamer_bank_accounts;
DROP TABLE IF EXISTS payout_batches;
DROP TABLE IF EXISTS refunds;
DROP TABLE IF EXISTS donations;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS ledger_accounts;
DROP TABLE IF EXISTS idempotency;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS streamer_settings;
DROP TABLE IF EXISTS streamers;

DROP TYPE IF EXISTS email_receipt_status;
DROP TYPE IF EXISTS fund_hold_target;
DROP TYPE IF EXISTS fund_hold_status;
DROP TYPE IF EXISTS fraud_action;
DROP TYPE IF EXISTS fraud_rule;
DROP TYPE IF EXISTS token_purpose;
DROP TYPE IF EXISTS payout_batch_status;
DROP TYPE IF EXISTS payout_status;
DROP TYPE IF EXISTS payment_method;
DROP TYPE IF EXISTS settlement_status;
DROP TYPE IF EXISTS donation_status;
DROP TYPE IF EXISTS ledger_account_type;
DROP TYPE IF EXISTS ledger_entry_type;
DROP TYPE IF EXISTS ledger_direction;

-- pgcrypto is intentionally retained because extensions are database-wide and
-- may be shared by objects outside Inflora's migration history.