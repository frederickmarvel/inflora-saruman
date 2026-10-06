ALTER TABLE donations DROP CONSTRAINT IF EXISTS chk_donation_display_pricing_snapshot;
ALTER TABLE donations
  DROP COLUMN IF EXISTS display_max_sec,
  DROP COLUMN IF EXISTS display_min_sec,
  DROP COLUMN IF EXISTS display_rate_idr_per_sec;