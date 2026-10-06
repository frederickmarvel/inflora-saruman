-- Freeze the streamer-selected display pricing on each donation intent.
-- Streamer settings remain mutable, while paid/in-flight donations retain the
-- exact rate and bounds used when their intent was created.
ALTER TABLE donations
  ADD COLUMN display_rate_idr_per_sec BIGINT,
  ADD COLUMN display_min_sec INTEGER,
  ADD COLUMN display_max_sec INTEGER;

ALTER TABLE donations
  ADD CONSTRAINT chk_donation_display_pricing_snapshot CHECK (
    -- Legacy donations may already have a calculated duration but no persisted
    -- rate/bounds. Keep those rows valid while rejecting partial new snapshots.
    -- The three snapshot columns must be all-NULL (legacy) or all-NON-NULL;
    -- any partial combination is rejected. Explicit IS NOT NULL guards are
    -- required so a NULL column yields FALSE (violation) rather than UNKNOWN,
    -- which PostgreSQL CHECK treats as passing.
    (display_rate_idr_per_sec IS NULL AND display_min_sec IS NULL AND display_max_sec IS NULL)
    OR
    (display_rate_idr_per_sec IS NOT NULL
      AND display_min_sec IS NOT NULL
      AND display_max_sec IS NOT NULL
      AND display_rate_idr_per_sec > 0
      AND display_min_sec > 0
      AND display_max_sec >= display_min_sec
      AND display_duration_sec BETWEEN display_min_sec AND display_max_sec)
  );