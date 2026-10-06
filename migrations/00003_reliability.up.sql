-- ============================================================================
-- Inflora Phase 1 reliability schema — durable outbox, consumer inbox,
-- dead-letter, ledger transactions, WS projection.
-- Idempotent for additive changes; requires fresh DB for the first apply.
-- This migration is the foundation for Phase 2 shared libraries and every
-- downstream service feature phase.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. ledger_transactions  (Phase 1; posting templates live in
--    almanac/planning/schemas/10-posting-templates.md)
-- ----------------------------------------------------------------------------

CREATE TABLE ledger_transactions (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  operation_key     VARCHAR(128) NOT NULL UNIQUE,    -- frozen UUIDv5 from 11-operation-keys
  correlation_id    UUID,                             -- groups ledger entries from one txn
  causation_id      UUID,                             -- parent operation, if any
  transaction_type  VARCHAR(60) NOT NULL,              -- e.g. DONATION_CAPTURE, PAYOUT_RESERVATION
  streamer_id       UUID,                             -- nullable for global transactions (e.g. PLATFORM_FEE posting)
  reversal_of       UUID REFERENCES ledger_transactions(id), -- self-FK for compensation chains
  metadata          JSONB,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT chk_reversal_consistency CHECK (
    (reversal_of IS NULL)
    OR (reversal_of <> id)
  )
);

CREATE INDEX idx_ledger_tx_streamer_time ON ledger_transactions (streamer_id, created_at DESC);
CREATE INDEX idx_ledger_tx_correlation   ON ledger_transactions (correlation_id);
CREATE INDEX idx_ledger_tx_causation     ON ledger_transactions (causation_id);
CREATE INDEX idx_ledger_tx_reversal      ON ledger_transactions (reversal_of) WHERE reversal_of IS NOT NULL;

-- ----------------------------------------------------------------------------
-- 2. ledger_entries  (Phase 1 update: link to ledger_transactions, add column)
--
-- The ledger_entries table is created in 00001_init and is APPEND-ONLY
-- (CREATE RULE no_update/no_delete). This migration adds ledger_transaction_id
-- and a CHECK that every entry is positive. Existing rows from 00001 are
-- backfilled with synthetic transaction rows so the FK is satisfied.
-- ----------------------------------------------------------------------------

ALTER TABLE ledger_entries
  ADD COLUMN ledger_transaction_id UUID REFERENCES ledger_transactions(id);

-- For any pre-existing rows (e.g. from dev seed data), backfill with one
-- ledger_transactions row per unique (donation_id, streamer_id, created_at).
-- The migration is non-destructive: existing rows remain append-only.
DO $$
DECLARE
  rec record;
  v_op_key VARCHAR(128);
  v_tx_id  UUID;
BEGIN
  FOR rec IN
    SELECT DISTINCT streamer_id, donation_id, payout_id, hold_id, correlation_id
    FROM ledger_entries
    WHERE ledger_transaction_id IS NULL
  LOOP
    v_op_key := 'legacy_' || encode(gen_random_bytes(16), 'hex');
    INSERT INTO ledger_transactions (operation_key, correlation_id, transaction_type, streamer_id, metadata)
    VALUES (
      v_op_key,
      rec.correlation_id,
      'LEGACY_BACKFILL',
      rec.streamer_id,
      jsonb_build_object(
        'donation_id', rec.donation_id,
        'payout_id',   rec.payout_id,
        'hold_id',     rec.hold_id
      )
    )
    RETURNING id INTO v_tx_id;
    UPDATE ledger_entries
      SET ledger_transaction_id = v_tx_id
      WHERE ledger_transaction_id IS NULL
        AND streamer_id   IS NOT DISTINCT FROM rec.streamer_id
        AND donation_id   IS NOT DISTINCT FROM rec.donation_id
        AND payout_id     IS NOT DISTINCT FROM rec.payout_id
        AND hold_id       IS NOT DISTINCT FROM rec.hold_id
        AND correlation_id IS NOT DISTINCT FROM rec.correlation_id;
  END LOOP;
END $$;

-- After backfill the constraint is enforced. New entries MUST reference a tx.
ALTER TABLE ledger_entries
  ALTER COLUMN ledger_transaction_id SET NOT NULL;

ALTER TABLE ledger_entries
  ADD CONSTRAINT chk_amount_positive CHECK (amount_idr > 0);

CREATE INDEX idx_ledger_entries_tx ON ledger_entries (ledger_transaction_id);

-- ----------------------------------------------------------------------------
-- 3. outbox_events  (transactional outbox; Phase 2 dispatcher reads this)
-- ----------------------------------------------------------------------------

CREATE TABLE outbox_events (
  event_id       UUID PRIMARY KEY,                              -- UUIDv4, stable across retries
  producer       VARCHAR(40)  NOT NULL,                          -- 'saruman','palantir','tolkien','ingest','ws-gateway'
  event_type     VARCHAR(120) NOT NULL,                          -- e.g. donation.charged.v1, pg.gateway.topup.completed.v1
  event_version  VARCHAR(16)  NOT NULL,                          -- matches the .v<N> in event_type
  aggregate_type VARCHAR(40)  NOT NULL,                          -- e.g. donation, payout, refund, streamer
  aggregate_id   UUID         NOT NULL,
  streamer_id    UUID,                                            -- nullable for global events
  subject        VARCHAR(200) NOT NULL,                          -- NATS subject, e.g. donation.charged.v1.<streamer_id>
  envelope       JSONB        NOT NULL,                          -- full frozen envelope from 00-frozen-contracts §1
  occurred_at    TIMESTAMPTZ  NOT NULL,                          -- producer-set
  available_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),            -- earliest dispatcher claim
  attempts       INT          NOT NULL DEFAULT 0,
  claimed_at     TIMESTAMPTZ,
  claimed_by     VARCHAR(80),                                     -- worker name
  published_at   TIMESTAMPTZ,
  dead_lettered_at TIMESTAMPTZ,
  last_error     TEXT,

  CONSTRAINT chk_event_version_present CHECK (length(event_version) > 0),
  CONSTRAINT chk_envelope_object       CHECK (jsonb_typeof(envelope) = 'object'),
  CONSTRAINT chk_subject_present       CHECK (length(subject) > 0)
);

-- Dispatcher claims rows that are due and not yet published, ordered by available_at.
CREATE INDEX idx_outbox_due_unpublished
  ON outbox_events (available_at)
  WHERE published_at IS NULL AND dead_lettered_at IS NULL;

-- Operational visibility: claim state and stale-claim recovery.
CREATE INDEX idx_outbox_claimed_at
  ON outbox_events (claimed_at)
  WHERE claimed_at IS NOT NULL AND published_at IS NULL;

-- Per-aggregate dedup (consumer inbox is the canonical boundary, this is an index aid).
CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_type, aggregate_id);

-- ----------------------------------------------------------------------------
-- 4. consumer_inbox  (durable business dedup for every consumer)
-- ----------------------------------------------------------------------------

CREATE TABLE consumer_inbox (
  consumer_name VARCHAR(80)  NOT NULL,     -- e.g. saruman.topup-completed, saruman.withdrawal-completed
  event_id     UUID         NOT NULL,
  event_type   VARCHAR(120) NOT NULL,
  streamer_id  UUID,
  received_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,
  last_error   TEXT,
  attempts     INT          NOT NULL DEFAULT 0,
  PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX idx_inbox_unprocessed
  ON consumer_inbox (consumer_name, received_at)
  WHERE processed_at IS NULL;

-- ----------------------------------------------------------------------------
-- 5. event_dead_letters  (durable, inspectable, replayable)
-- ----------------------------------------------------------------------------

CREATE TABLE event_dead_letters (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  consumer_name    VARCHAR(80)  NOT NULL,
  event_id         UUID         NOT NULL,
  subject          VARCHAR(200) NOT NULL,
  envelope         JSONB        NOT NULL,
  attempt_count    INT          NOT NULL,
  error_class      VARCHAR(80)  NOT NULL,
  last_error       TEXT         NOT NULL,
  first_failed_at  TIMESTAMPTZ  NOT NULL,
  last_failed_at   TIMESTAMPTZ  NOT NULL,
  replayed_at      TIMESTAMPTZ,
  replayed_by      VARCHAR(80),
  replay_notes     TEXT,

  CONSTRAINT uq_dead_letter_event UNIQUE (consumer_name, event_id),
  CONSTRAINT chk_dead_letter_attempts CHECK (attempt_count > 0),
  CONSTRAINT chk_dead_letter_envelope CHECK (jsonb_typeof(envelope) = 'object')
);

CREATE INDEX idx_dead_letters_unreplayed
  ON event_dead_letters (last_failed_at DESC)
  WHERE replayed_at IS NULL;

CREATE INDEX idx_dead_letters_consumer
  ON event_dead_letters (consumer_name, last_failed_at DESC);

-- ----------------------------------------------------------------------------
-- 6. ws_projection_inbox  (durable dedup at WS gateway; D-015)
-- ----------------------------------------------------------------------------

CREATE TABLE ws_projection_inbox (
  consumer_name VARCHAR(80) NOT NULL,   -- 'ws-gateway.donation-charged', etc.
  event_id      UUID         NOT NULL,
  event_type    VARCHAR(120) NOT NULL,
  received_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  processed_at  TIMESTAMPTZ,
  PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX idx_ws_projection_inbox_unprocessed
  ON ws_projection_inbox (consumer_name, received_at)
  WHERE processed_at IS NULL;

-- ----------------------------------------------------------------------------
-- 7. ws_projection_sequences  (per-streamer monotonic seq; D-015)
-- ----------------------------------------------------------------------------

CREATE TABLE ws_projection_sequences (
  streamer_id  UUID PRIMARY KEY,
  next_seq     BIGINT NOT NULL DEFAULT 1,   -- next seq to allocate (seq >= 1)
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_next_seq_positive CHECK (next_seq >= 1)
);

-- ----------------------------------------------------------------------------
-- 8. ws_projection_frames  (durable, replayable WS frame storage; D-015)
-- ----------------------------------------------------------------------------

CREATE TABLE ws_projection_frames (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  streamer_id     UUID NOT NULL,
  seq             BIGINT NOT NULL,                       -- monotonic, starts at 1
  source_event_id UUID NOT NULL,                          -- unique per source delivery
  event_type      VARCHAR(120) NOT NULL,                  -- e.g. donation.charged.v1
  payload         JSONB NOT NULL,                          -- frozen frame JSON
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at      TIMESTAMPTZ,                            -- retention / replay-limit cutoff

  CONSTRAINT uq_ws_frame_seq UNIQUE (streamer_id, seq),
  CONSTRAINT uq_ws_frame_source UNIQUE (source_event_id),
  CONSTRAINT chk_ws_frame_seq_positive CHECK (seq > 0),
  CONSTRAINT chk_ws_frame_payload_object CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX idx_ws_frames_streamer_seq
  ON ws_projection_frames (streamer_id, seq DESC);

CREATE INDEX idx_ws_frames_expires
  ON ws_projection_frames (expires_at)
  WHERE expires_at IS NOT NULL;

-- ----------------------------------------------------------------------------
-- 9. outbox_lease bookkeeping  (helps Phase 2 dispatcher release stale claims)
-- ----------------------------------------------------------------------------

-- (no extra table; dispatcher reads claimed_at / published_at on outbox_events)
-- Add a clock-skew guard: cannot claim rows whose available_at is in the future.
-- Enforced at the application layer (see shared/outbox/dispatcher.go in Phase 2).