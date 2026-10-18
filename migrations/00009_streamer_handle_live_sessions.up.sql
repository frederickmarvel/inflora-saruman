-- 00009: streamer handles (public viewer routing) + live sessions (tip-form
-- gating). Drift rows D-033/D-037 filed in almanac CONTRACT_DRIFT §11.
--
-- * handle is the human-facing public identity for /tip/:handle and /u/:handle.
--   Generated server-side at signup by tolkien (slug + random suffix); existing
--   rows get a deterministic-ish backfill with a random suffix.
-- * live_sessions holds the open/ended lifecycle the tip form and rich ingest
--   intent (D-031) key off. Exactly one OPEN session per streamer, enforced by
--   the partial unique index; opening while open is replay-safe in the app layer.

ALTER TABLE streamers ADD COLUMN handle VARCHAR(40);

UPDATE streamers
SET handle = CASE
        WHEN slug = '' THEN 'streamer'
        ELSE slug
    END || '-' || substr(md5(random()::text), 1, 5)
FROM (
    SELECT id,
           left(lower(regexp_replace(display_name, '[^a-zA-Z0-9]+', '-', 'g')), 34) AS slug
    FROM streamers
) s
WHERE streamers.id = s.id
  AND streamers.handle IS NULL;

ALTER TABLE streamers ALTER COLUMN handle SET NOT NULL;
ALTER TABLE streamers ADD CONSTRAINT uq_streamers_handle UNIQUE (handle);

CREATE TABLE live_sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    streamer_id UUID NOT NULL REFERENCES streamers(id),
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at    TIMESTAMPTZ
);

-- One open session per streamer.
CREATE UNIQUE INDEX uq_live_sessions_open
    ON live_sessions (streamer_id) WHERE ended_at IS NULL;

-- Recent-history lookups (public tip-session, analytics).
CREATE INDEX idx_live_sessions_streamer
    ON live_sessions (streamer_id, started_at DESC);