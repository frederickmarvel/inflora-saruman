-- ============================================================================
-- Inflora MVP Schema (v2 — with MDR, settlement, holds, batching, receipts)
-- ============================================================================
-- Engine: PostgreSQL 15+
-- Currency: IDR only. amount_idr is BIGINT (no decimals).
-- Money: append-only ledger. Never UPDATE/DELETE ledger_entries.
-- Schema ownership: saruman owns streamer/ledger tables; palantir owns
--   gateway_* tables; tolkien extends users table; ithildin reads only.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- gen_random_uuid()

-- ============================================================================
-- ENUMS
-- ============================================================================

CREATE TYPE ledger_direction AS ENUM ('DEBIT', 'CREDIT');

CREATE TYPE ledger_entry_type AS ENUM (
  'DONATION_DEPOSIT',          -- donor pays money in (gross, includes MDR + platform fee)
  'STREAMER_PENDING_CREDIT',   -- streamer credits after capture, pre-settlement
  'STREAMER_AVAILABLE_CREDIT', -- streamer credits after settlement, available for payout
  'PLATFORM_FEE',              -- platform fee from donation
  'MDR_FEE',                   -- payment gateway MDR (charged to donor but tracked here)
  'STREAMER_PAID_DEBIT',       -- money leaves streamer_available on payout
  'REFUND_DEBIT',              -- refund reverses donation
  'REFUND_CREDIT',             -- refund returns donor money
  'HOLD_DEBIT',                -- fund hold reduces available balance
  'HOLD_CREDIT'                -- fund hold release restores balance
);

CREATE TYPE ledger_account_type AS ENUM (
  'DONOR_CASH',                -- tracks donor-side money (for refund reconcile)
  'STREAMER_PENDING',          -- streamer balance awaiting settlement
  'STREAMER_AVAILABLE',        -- streamer balance available for payout
  'STREAMER_PAID',              -- money already paid out
  'PLATFORM_REVENUE',          -- platform's collected fees
  'MDR_REVENUE'                -- MDR tracked (informational; provider takes this directly)
);

CREATE TYPE donation_status AS ENUM (
  'INTENT_CREATED',
  'CHARGED',          -- captured by provider; settlement pending
  'SETTLED',          -- settled by provider; funds available
  'FAILED',
  'REFUNDED'
);

CREATE TYPE settlement_status AS ENUM (
  'PENDING',          -- captured, waiting for provider settlement
  'SETTLED',          -- provider has settled
  'FAILED'            -- settlement failed
);

CREATE TYPE payment_method AS ENUM (
  'QRIS',
  'VIRTUAL_ACCOUNT',
  'EWALLET_GOPAY',
  'EWALLET_OVO',
  'EWALLET_DANA',
  'CREDIT_CARD',
  'OTHER'
);

CREATE TYPE payout_status AS ENUM (
  'REQUESTED',
  'BATCHED',          -- assigned to a batch, awaiting bulk execute
  'PROCESSING',
  'SETTLED',
  'FAILED'
);

CREATE TYPE payout_batch_status AS ENUM (
  'DRAFT',            -- being assembled by finops
  'EXECUTING',        -- bulk transfer in progress
  'COMPLETED',        -- all payouts in batch settled
  'PARTIAL_FAILED',   -- some payouts failed, some succeeded
  'FAILED'            -- entire batch failed
);

CREATE TYPE token_purpose AS ENUM (
  'SESSION',
  'OVERLAY'
);

CREATE TYPE fraud_rule AS ENUM (
  'VEL_CARD_DONATIONS_PER_MIN',
  'VEL_CARD_DONATIONS_PER_DAY',
  'BLOCKED_CARD',
  'BLOCKED_IP'
);

CREATE TYPE fraud_action AS ENUM (
  'BLOCKED',
  'ALLOWED_BUT_FLAGGED'
);

CREATE TYPE fund_hold_status AS ENUM (
  'ACTIVE',
  'RELEASED',
  'EXPIRED'
);

CREATE TYPE fund_hold_target AS ENUM (
  'STREAMER',         -- hold affects all of streamer's funds
  'DONATION',         -- hold affects specific donation
  'PAYOUT'            -- hold affects specific payout
);

CREATE TYPE email_receipt_status AS ENUM (
  'PENDING',
  'SENT',
  'FAILED',
  'BOUNCED'
);

-- ============================================================================
-- streamers (saruman owns)
-- ============================================================================

CREATE TABLE streamers (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email           VARCHAR(320) NOT NULL UNIQUE,
  display_name    VARCHAR(80) NOT NULL,
  password_hash   VARCHAR(255) NOT NULL,
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  is_verified     BOOLEAN NOT NULL DEFAULT FALSE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_login_at   TIMESTAMPTZ
);

CREATE INDEX idx_streamers_active ON streamers (is_active) WHERE is_active = TRUE;
CREATE INDEX idx_streamers_email ON streamers (lower(email));

-- ============================================================================
-- streamer_settings (saruman owns) — per-streamer config
-- ============================================================================

CREATE TABLE streamer_settings (
  id                            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id                   UUID NOT NULL UNIQUE REFERENCES streamers(id) ON DELETE CASCADE,
  -- Display: how long the donation popup stays on OBS per amount.
  -- duration_sec = donation_amount_idr / display_rate_idr_per_sec, bounded [min, max].
  display_rate_idr_per_sec      INTEGER NOT NULL DEFAULT 10000,
  display_min_sec               INTEGER NOT NULL DEFAULT 5,
  display_max_sec               INTEGER NOT NULL DEFAULT 60,
  -- Toggles
  show_donor_name               BOOLEAN NOT NULL DEFAULT TRUE,
  allow_voice                   BOOLEAN NOT NULL DEFAULT TRUE,
  allow_youtube                 BOOLEAN NOT NULL DEFAULT TRUE,
  auto_play_voice               BOOLEAN NOT NULL DEFAULT TRUE,
  auto_play_youtube             BOOLEAN NOT NULL DEFAULT TRUE,
  -- Notification
  notify_on_donation            BOOLEAN NOT NULL DEFAULT TRUE,
  notify_on_large_donation_idr  BIGINT  NOT NULL DEFAULT 100000,
  -- Limits
  min_donation_idr              BIGINT NOT NULL DEFAULT 1000,
  max_donation_idr              BIGINT NOT NULL DEFAULT 10000000,
  -- Timestamps
  created_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_display_rate_positive CHECK (display_rate_idr_per_sec > 0),
  CONSTRAINT chk_display_min_max CHECK (display_min_sec <= display_max_sec),
  CONSTRAINT chk_min_max_donation CHECK (min_donation_idr <= max_donation_idr)
);

-- ============================================================================
-- sessions (saruman owns; tolkien writes)
-- ============================================================================

CREATE TABLE sessions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id     UUID NOT NULL REFERENCES streamers(id) ON DELETE CASCADE,
  token_hash      VARCHAR(255) NOT NULL,
  last4           CHAR(4) NOT NULL,
  purpose         token_purpose NOT NULL,
  expires_at      TIMESTAMPTZ NOT NULL,
  last_used_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_at      TIMESTAMPTZ,
  CONSTRAINT uq_sessions_token UNIQUE (token_hash)
);

CREATE INDEX idx_sessions_streamer_active ON sessions (streamer_id) WHERE revoked_at IS NULL;
CREATE INDEX idx_sessions_purpose_active ON sessions (purpose) WHERE revoked_at IS NULL;
CREATE INDEX idx_sessions_expiry ON sessions (expires_at) WHERE revoked_at IS NULL;

-- ============================================================================
-- idempotency (saruman owns)
-- ============================================================================

CREATE TABLE idempotency (
  key             VARCHAR(128) PRIMARY KEY,
  endpoint        VARCHAR(80) NOT NULL,
  request_hash    CHAR(64) NOT NULL,
  response_status INT,
  response_body   JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at    TIMESTAMPTZ
);

CREATE INDEX idx_idempotency_created ON idempotency (created_at);

-- ============================================================================
-- ledger_accounts (saruman owns)
-- ============================================================================

CREATE TABLE ledger_accounts (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id     UUID NOT NULL REFERENCES streamers(id),
  account_type    ledger_account_type NOT NULL,
  currency        CHAR(3) NOT NULL DEFAULT 'IDR',
  balance_idr     BIGINT NOT NULL DEFAULT 0,
  version         BIGINT NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_ledger_account UNIQUE (streamer_id, account_type, currency)
);

-- ============================================================================
-- ledger_entries (saruman owns) -- APPEND-ONLY
-- ============================================================================

CREATE TABLE ledger_entries (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id     UUID NOT NULL REFERENCES streamers(id),
  account_id      UUID NOT NULL REFERENCES ledger_accounts(id),
  direction       ledger_direction NOT NULL,
  amount_idr      BIGINT NOT NULL CHECK (amount_idr > 0),
  entry_type      ledger_entry_type NOT NULL,
  external_ref    VARCHAR(128),
  donation_id     UUID,
  payout_id       UUID,
  hold_id         UUID,                            -- if hold-related
  reversal_of     UUID REFERENCES ledger_entries(id),
  correlation_id  UUID,
  metadata        JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ledger_entries_streamer_time ON ledger_entries (streamer_id, created_at DESC);
CREATE INDEX idx_ledger_entries_account_time ON ledger_entries (account_id, created_at DESC);
CREATE INDEX idx_ledger_entries_external_ref ON ledger_entries (external_ref);
CREATE INDEX idx_ledger_entries_correlation ON ledger_entries (correlation_id);
CREATE INDEX idx_ledger_entries_donation ON ledger_entries (donation_id) WHERE donation_id IS NOT NULL;
CREATE INDEX idx_ledger_entries_payout ON ledger_entries (payout_id) WHERE payout_id IS NOT NULL;
CREATE INDEX idx_ledger_entries_hold ON ledger_entries (hold_id) WHERE hold_id IS NOT NULL;

CREATE RULE no_update_ledger_entries AS ON UPDATE TO ledger_entries DO INSTEAD NOTHING;
CREATE RULE no_delete_ledger_entries AS ON DELETE TO ledger_entries DO INSTEAD NOTHING;

-- ============================================================================
-- donations (saruman owns)
-- ============================================================================

CREATE TABLE donations (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id             UUID NOT NULL,
  intent_id               UUID NOT NULL UNIQUE,
  status                  donation_status NOT NULL DEFAULT 'INTENT_CREATED',
  settlement_status       settlement_status NOT NULL DEFAULT 'PENDING',
  -- Amounts
  amount_idr              BIGINT NOT NULL CHECK (amount_idr > 0),        -- donor intended
  mdr_idr                 BIGINT NOT NULL DEFAULT 0 CHECK (mdr_idr >= 0),-- payment gateway fee (charged to donor)
  mdr_rate_bps            INTEGER NOT NULL DEFAULT 0,                     -- basis points used (e.g., 70 = 0.7%)
  platform_fee_idr        BIGINT NOT NULL DEFAULT 0 CHECK (platform_fee_idr >= 0),
  net_idr                 BIGINT NOT NULL DEFAULT 0 CHECK (net_idr >= 0),-- amount_idr - platform_fee
  gross_charged_idr       BIGINT NOT NULL DEFAULT 0,                     -- amount_idr + mdr_idr (donor pays this)
  currency                CHAR(3) NOT NULL DEFAULT 'IDR',
  -- Donor info
  donor_display_name      VARCHAR(80),
  donor_email             VARCHAR(320),                                   -- for receipt
  message                 VARCHAR(500),
  is_anonymous            BOOLEAN NOT NULL DEFAULT FALSE,
  -- Content (optional)
  voice_url               TEXT,                                           -- TTS audio URL
  voice_duration_sec      INTEGER,                                        -- actual TTS length
  youtube_url             TEXT,
  youtube_start_sec       INTEGER,                                        -- clip start
  youtube_end_sec         INTEGER,                                        -- clip end
  -- Display config (snapshot at time of donation)
  display_duration_sec    INTEGER,                                        -- computed: amount / rate
  -- Provider
  provider_name           VARCHAR(40),                                    -- 'midtrans', 'xendit'
  provider_charge_id      VARCHAR(128),
  payment_method          payment_method,                                 -- 'QRIS', 'VA', etc.
  -- Tracking
  client_ip               INET,
  user_agent              VARCHAR(500),
  failure_reason          VARCHAR(40),
  -- Timestamps
  created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  charged_at              TIMESTAMPTZ,
  settled_at               TIMESTAMPTZ,                                    -- when settlement webhook fires
  failed_at               TIMESTAMPTZ,
  refunded_at             TIMESTAMPTZ,
  expires_at              TIMESTAMPTZ NOT NULL,
  metadata                JSONB,
  CONSTRAINT chk_youtube_range_valid CHECK (
    youtube_url IS NULL OR (youtube_start_sec IS NULL AND youtube_end_sec IS NULL)
                          OR (youtube_start_sec IS NOT NULL AND youtube_end_sec IS NOT NULL
                              AND youtube_end_sec > youtube_start_sec)
  )
);

CREATE INDEX idx_donations_streamer_status_time ON donations (streamer_id, status, created_at DESC);
CREATE INDEX idx_donations_settlement_pending
  ON donations (settled_at) WHERE settlement_status = 'PENDING';
CREATE INDEX idx_donations_status_expires
  ON donations (status, expires_at) WHERE status = 'INTENT_CREATED';
CREATE INDEX idx_donations_intent ON donations (intent_id);
CREATE INDEX idx_donations_provider_ref
  ON donations (provider_name, provider_charge_id) WHERE provider_charge_id IS NOT NULL;
CREATE INDEX idx_donations_donor_email ON donations (donor_email) WHERE donor_email IS NOT NULL;

-- ============================================================================
-- refunds (saruman owns)
-- ============================================================================

CREATE TABLE refunds (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  donation_id         UUID NOT NULL REFERENCES donations(id),
  streamer_id         UUID NOT NULL,
  amount_idr          BIGINT NOT NULL CHECK (amount_idr > 0),
  reason              VARCHAR(40) NOT NULL,
  provider_refund_id  VARCHAR(128),
  initiated_by        UUID,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at        TIMESTAMPTZ,
  metadata            JSONB
);

CREATE INDEX idx_refunds_donation ON refunds (donation_id);
CREATE INDEX idx_refunds_streamer_time ON refunds (streamer_id, created_at DESC);

-- ============================================================================
-- payouts (saruman owns)
-- ============================================================================

CREATE TABLE payouts (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id         UUID NOT NULL,
  amount_idr          BIGINT NOT NULL CHECK (amount_idr >= 10000),  -- min withdrawal enforced
  status              payout_status NOT NULL DEFAULT 'REQUESTED',
  batch_id            UUID,                                    -- nullable FK; set when batched
  bank_account_id     UUID NOT NULL,
  provider_payout_id  VARCHAR(128),
  failure_reason      VARCHAR(80),
  requested_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at        TIMESTAMPTZ,
  settled_at          TIMESTAMPTZ,
  metadata            JSONB,
  CONSTRAINT chk_min_withdrawal CHECK (amount_idr >= 10000)
);

CREATE INDEX idx_payouts_streamer_status_time ON payouts (streamer_id, status, requested_at DESC);
CREATE INDEX idx_payouts_status_requested ON payouts (status) WHERE status IN ('REQUESTED', 'BATCHED');
CREATE INDEX idx_payouts_batch ON payouts (batch_id) WHERE batch_id IS NOT NULL;

-- ============================================================================
-- payout_batches (saruman owns) — finops batches multiple payouts
-- ============================================================================

CREATE TABLE payout_batches (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status                payout_batch_status NOT NULL DEFAULT 'DRAFT',
  total_amount_idr      BIGINT NOT NULL DEFAULT 0,
  payout_count          INTEGER NOT NULL DEFAULT 0,
  provider_batch_id     VARCHAR(128),                            -- provider's batch reference
  provider_name         VARCHAR(40),
  created_by            UUID NOT NULL,                            -- finops/admin user id
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  executing_at          TIMESTAMPTZ,
  completed_at          TIMESTAMPTZ,
  failure_reason        VARCHAR(120),
  metadata              JSONB
);

CREATE INDEX idx_payout_batches_status_time ON payout_batches (status, created_at DESC);

-- ============================================================================
-- streamer_bank_accounts (saruman owns)
-- ============================================================================

CREATE TABLE streamer_bank_accounts (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id     UUID NOT NULL REFERENCES streamers(id),
  bank_code       VARCHAR(10) NOT NULL,
  account_number_last4 VARCHAR(4) NOT NULL,
  account_name    VARCHAR(120) NOT NULL,
  is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
  is_verified     BOOLEAN NOT NULL DEFAULT FALSE,
  palantir_ref    VARCHAR(128) NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_bank_primary UNIQUE (streamer_id, is_primary) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX idx_bank_accounts_streamer ON streamer_bank_accounts (streamer_id);

-- ============================================================================
-- fund_holds (saruman owns) — investigation freezes
-- ============================================================================

CREATE TABLE fund_holds (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id     UUID,                             -- nullable (set if target=STREAMER or related)
  donation_id     UUID,                             -- nullable (set if target=DONATION)
  payout_id       UUID,                             -- nullable (set if target=PAYOUT)
  target_type     fund_hold_target NOT NULL,
  amount_idr      BIGINT,                           -- null for whole-streamer hold
  reason          VARCHAR(120) NOT NULL,
  status          fund_hold_status NOT NULL DEFAULT 'ACTIVE',
  created_by      UUID NOT NULL,                    -- finops/admin user id
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at      TIMESTAMPTZ,                      -- auto-release after this
  released_at     TIMESTAMPTZ,
  released_by     UUID,
  resolution_note TEXT,
  metadata        JSONB,
  CONSTRAINT chk_hold_target_consistency CHECK (
    (target_type = 'STREAMER' AND streamer_id IS NOT NULL AND donation_id IS NULL AND payout_id IS NULL)
    OR (target_type = 'DONATION' AND donation_id IS NOT NULL AND streamer_id IS NOT NULL AND payout_id IS NULL)
    OR (target_type = 'PAYOUT'   AND payout_id IS NOT NULL AND streamer_id IS NOT NULL AND donation_id IS NULL)
  )
);

CREATE INDEX idx_fund_holds_streamer_active
  ON fund_holds (streamer_id) WHERE status = 'ACTIVE';
CREATE INDEX idx_fund_holds_donation_active
  ON fund_holds (donation_id) WHERE status = 'ACTIVE';
CREATE INDEX idx_fund_holds_payout_active
  ON fund_holds (payout_id) WHERE status = 'ACTIVE';
CREATE INDEX idx_fund_holds_expires ON fund_holds (expires_at) WHERE status = 'ACTIVE';

-- ============================================================================
-- email_receipts (saruman owns) — tracks sent donation receipts
-- ============================================================================

CREATE TABLE email_receipts (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  donation_id     UUID NOT NULL,
  streamer_id     UUID NOT NULL,
  recipient_email VARCHAR(320) NOT NULL,
  status          email_receipt_status NOT NULL DEFAULT 'PENDING',
  provider_msg_id VARCHAR(128),                     -- email provider's message id
  error_message   TEXT,
  sent_at         TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_email_per_donation UNIQUE (donation_id)
);

CREATE INDEX idx_email_receipts_status ON email_receipts (status, created_at DESC);

-- ============================================================================
-- mdr_rates (saruman owns) — per-provider, per-method MDR config
-- ============================================================================

CREATE TABLE mdr_rates (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  provider_name   VARCHAR(40) NOT NULL,
  payment_method  payment_method NOT NULL,
  mdr_bps         INTEGER NOT NULL,                  -- basis points (e.g., 70 = 0.7%)
  flat_idr        BIGINT NOT NULL DEFAULT 0,        -- optional flat fee
  effective_from  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  effective_to    TIMESTAMPTZ,                      -- null = current
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_mdr_active UNIQUE (provider_name, payment_method, effective_from)
);

-- ============================================================================
-- fraud_events (saruman owns)
-- ============================================================================

CREATE TABLE fraud_events (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id         UUID,
  rule                fraud_rule NOT NULL,
  donor_fingerprint   VARCHAR(120) NOT NULL,
  count               INT NOT NULL,
  window_seconds      INT NOT NULL,
  action_taken        fraud_action NOT NULL,
  metadata            JSONB,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fraud_events_time ON fraud_events (created_at DESC);
CREATE INDEX idx_fraud_events_streamer_time ON fraud_events (streamer_id, created_at DESC);

-- ============================================================================
-- audit_log (saruman owns)
-- ============================================================================

CREATE TABLE audit_log (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_id        UUID,
  actor_type      VARCHAR(20) NOT NULL,
  action          VARCHAR(80) NOT NULL,
  resource_type   VARCHAR(40) NOT NULL,
  resource_id     UUID,
  ip_address      INET,
  user_agent      VARCHAR(500),
  metadata        JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_actor_time ON audit_log (actor_id, created_at DESC);
CREATE INDEX idx_audit_resource_time ON audit_log (resource_type, resource_id, created_at DESC);
CREATE INDEX idx_audit_action_time ON audit_log (action, created_at DESC);

-- ============================================================================
-- reconciliation_drift (saruman owns)
-- ============================================================================

CREATE TABLE reconciliation_drift (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_type        ledger_account_type NOT NULL,
  streamer_id         UUID,
  expected_idr        BIGINT NOT NULL,
  actual_idr          BIGINT NOT NULL,
  diff_idr            BIGINT NOT NULL,
  currency            CHAR(3) NOT NULL DEFAULT 'IDR',
  detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at         TIMESTAMPTZ,
  resolution_note     TEXT
);

CREATE INDEX idx_recon_unresolved
  ON reconciliation_drift (detected_at DESC) WHERE resolved_at IS NULL;

-- ============================================================================
-- gateway_* tables (palantir-gateway owns)
-- ============================================================================

CREATE TABLE gateway_topups (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  palantir_ref        VARCHAR(128) NOT NULL UNIQUE,
  saruman_donation_id UUID NOT NULL,
  streamer_id         UUID NOT NULL,
  amount_idr          BIGINT NOT NULL,
  mdr_idr             BIGINT NOT NULL DEFAULT 0,
  gross_charged_idr   BIGINT NOT NULL,
  provider_name       VARCHAR(40) NOT NULL,
  payment_method      VARCHAR(40),
  provider_charge_id  VARCHAR(128) NOT NULL,
  status              VARCHAR(20) NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  captured_at         TIMESTAMPTZ,
  settled_at          TIMESTAMPTZ
);

CREATE TABLE gateway_withdrawals (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  palantir_ref        VARCHAR(128) NOT NULL UNIQUE,
  saruman_payout_id   UUID NOT NULL,
  saruman_batch_id    UUID,
  streamer_id         UUID NOT NULL,
  amount_idr          BIGINT NOT NULL,
  status              VARCHAR(20) NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  settled_at          TIMESTAMPTZ
);

CREATE TABLE gateway_refunds (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  palantir_ref        VARCHAR(128) NOT NULL UNIQUE,
  saruman_refund_id   UUID NOT NULL,
  donation_id         UUID NOT NULL,
  amount_idr          BIGINT NOT NULL,
  status              VARCHAR(20) NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE webhook_events (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  provider_name       VARCHAR(40) NOT NULL,
  provider_event_id   VARCHAR(128) NOT NULL,
  payload_hash        CHAR(64) NOT NULL,
  payload             JSONB NOT NULL,
  received_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at        TIMESTAMPTZ,
  CONSTRAINT uq_webhook_event UNIQUE (provider_name, provider_event_id)
);

-- ============================================================================
-- VIEWS
-- ============================================================================

-- Streamer available balance (the only kind withdrawable)
CREATE VIEW v_streamer_balances AS
SELECT
  s.id AS streamer_id,
  s.display_name,
  s.is_active,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_PENDING'    THEN la.balance_idr ELSE 0 END), 0) AS pending_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_AVAILABLE'  THEN la.balance_idr ELSE 0 END), 0) AS available_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'STREAMER_PAID'       THEN la.balance_idr ELSE 0 END), 0) AS paid_idr,
  COALESCE(SUM(CASE WHEN la.account_type = 'PLATFORM_REVENUE'   THEN la.balance_idr ELSE 0 END), 0) AS platform_idr,
  -- Active holds reduce available
  COALESCE((
    SELECT SUM(amount_idr)
    FROM fund_holds fh
    WHERE fh.streamer_id = s.id AND fh.status = 'ACTIVE' AND fh.target_type = 'STREAMER'
  ), 0) AS held_idr
FROM streamers s
LEFT JOIN ledger_accounts la ON la.streamer_id = s.id
GROUP BY s.id, s.display_name, s.is_active;

CREATE VIEW v_recent_donations AS
SELECT
  d.streamer_id,
  d.id AS donation_id,
  d.amount_idr,
  d.mdr_idr,
  d.gross_charged_idr,
  d.platform_fee_idr,
  d.net_idr,
  d.status,
  d.settlement_status,
  d.donor_display_name,
  d.is_anonymous,
  d.message,
  d.voice_url,
  d.youtube_url,
  d.display_duration_sec,
  d.created_at,
  d.charged_at,
  d.settled_at
FROM donations d
WHERE d.created_at > NOW() - INTERVAL '30 days'
ORDER BY d.created_at DESC;

CREATE VIEW v_donation_funnel AS
SELECT
  date_trunc('day', created_at) AS day,
  COUNT(*) FILTER (WHERE status = 'INTENT_CREATED') AS intents,
  COUNT(*) FILTER (WHERE status = 'CHARGED') AS captured,
  COUNT(*) FILTER (WHERE settlement_status = 'SETTLED') AS settled,
  COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
  COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded,
  SUM(amount_idr) FILTER (WHERE status IN ('CHARGED','SETTLED','REFUNDED')) AS gross_idr,
  SUM(mdr_idr) FILTER (WHERE status IN ('CHARGED','SETTLED','REFUNDED')) AS mdr_idr,
  SUM(platform_fee_idr) FILTER (WHERE status IN ('CHARGED','SETTLED','REFUNDED')) AS platform_fee_idr,
  SUM(net_idr) FILTER (WHERE settlement_status = 'SETTLED') AS net_paid_idr
FROM donations
GROUP BY date_trunc('day', created_at)
ORDER BY day DESC;

CREATE VIEW v_pending_settlements AS
SELECT
  d.streamer_id,
  COUNT(*) AS count_pending,
  SUM(d.amount_idr) AS amount_pending_idr
FROM donations d
WHERE d.settlement_status = 'PENDING' AND d.status = 'CHARGED'
GROUP BY d.streamer_id;

-- ============================================================================
-- END OF SCHEMA v2
-- ============================================================================