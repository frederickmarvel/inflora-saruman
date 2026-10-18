-- 00011_donation_review.down.sql
DROP INDEX IF EXISTS idx_donations_review_unpublished;
ALTER TABLE donations DROP CONSTRAINT IF EXISTS chk_donations_review_event_published;
ALTER TABLE donations DROP CONSTRAINT IF EXISTS chk_donations_review_actor;
ALTER TABLE donations DROP CONSTRAINT IF EXISTS chk_donations_review_rejected;
ALTER TABLE donations DROP CONSTRAINT IF EXISTS chk_donations_review_aired;
ALTER TABLE donations DROP CONSTRAINT IF EXISTS chk_donations_review_charged;
ALTER TABLE donations DROP COLUMN IF EXISTS review_event_published_at;
ALTER TABLE donations DROP COLUMN IF EXISTS review_status;
ALTER TABLE donations DROP COLUMN IF EXISTS review_aired_at;
ALTER TABLE donations DROP COLUMN IF EXISTS review_decided_at;
ALTER TABLE donations DROP COLUMN IF EXISTS review_flag_reason;
ALTER TABLE donations DROP COLUMN IF EXISTS review_actor;
DROP TYPE IF EXISTS donation_review_status;