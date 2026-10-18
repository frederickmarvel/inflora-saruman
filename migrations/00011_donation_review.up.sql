-- 00011_donation_review.up.sql
-- CONTRACT_DRIFT D-038 (owner-amended 2026-10-18): server-owned donation
-- review state with POST-HOC semantics — every donation airs immediately at
-- charge (the frozen donation_status enum is untouched; overlay airing is
-- never delayed). The server records moderation flags at charge for the
-- creator dashboard badge, and the creator's reject is a replay-safe
-- TAKEDOWN of an already-aired donation (terminal, no restore).

CREATE TYPE donation_review_status AS ENUM (
  'APPROVED',  -- airs / aired: set for every charged donation at charge
  'REJECTED'   -- creator takedown after airing; frame removed from replay
);

ALTER TABLE donations ADD COLUMN review_status donation_review_status;
ALTER TABLE donations ADD COLUMN review_aired_at TIMESTAMPTZ;
ALTER TABLE donations ADD COLUMN review_decided_at TIMESTAMPTZ;
ALTER TABLE donations ADD COLUMN review_flag_reason VARCHAR(40);
ALTER TABLE donations ADD COLUMN review_actor VARCHAR(20);
-- Time of the last COMMITTED donation.reviewed.v1 (takedown) outbox enqueue
-- for this donation. NULL until the creator rejects; drives the at-least-once
-- reconciler so a lost takedown event does not permanently re-air content.
ALTER TABLE donations ADD COLUMN review_event_published_at TIMESTAMPTZ;

-- Review state only exists once money moved; INTENT_CREATED/FAILED rows keep NULLs.
ALTER TABLE donations ADD CONSTRAINT chk_donations_review_charged
  CHECK (review_status IS NULL OR status IN ('CHARGED','SETTLED','REFUNDED'));
-- Every charged donation is APPROVED at charge with its airing time recorded.
ALTER TABLE donations ADD CONSTRAINT chk_donations_review_aired
  CHECK (review_status IS DISTINCT FROM 'APPROVED' OR review_aired_at IS NOT NULL);
-- A takedown carries its decision time and actor.
ALTER TABLE donations ADD CONSTRAINT chk_donations_review_rejected
  CHECK (review_status IS DISTINCT FROM 'REJECTED'
         OR (review_decided_at IS NOT NULL AND review_actor = 'streamer'));
ALTER TABLE donations ADD CONSTRAINT chk_donations_review_actor
  CHECK (review_actor IS NULL OR review_actor = 'streamer');
-- Only takedown decisions publish a reviewed event (approve is an idempotent
-- confirm; charge-initialized APPROVED rows air via donation.charged.v1).
ALTER TABLE donations ADD CONSTRAINT chk_donations_review_event_published
  CHECK (review_event_published_at IS NULL OR review_status = 'REJECTED');

-- Reconciler scan: rejected rows whose takedown event was never committed.
CREATE INDEX idx_donations_review_unpublished
  ON donations (review_decided_at)
  WHERE review_status = 'REJECTED' AND review_event_published_at IS NULL;