-- 00010: rich tip intent (CONTRACT_DRIFT D-031) + tip-form session gating
-- (D-037). All columns are optional: the legacy donation contract is
-- unaffected. Pricing authority stays in the service path; these fields only
-- record what the viewer sent and which live session it was sent into.

ALTER TABLE donations ADD COLUMN live_session_id UUID;
ALTER TABLE donations ADD COLUMN widget_id VARCHAR(100);
ALTER TABLE donations ADD COLUMN quantity INTEGER;
ALTER TABLE donations ADD COLUMN tier VARCHAR(100);
ALTER TABLE donations ADD COLUMN media JSONB;
ALTER TABLE donations ADD COLUMN call JSONB;

-- Session-scoped donation history (control room, per-stream totals).
CREATE INDEX idx_donations_live_session
    ON donations (live_session_id) WHERE live_session_id IS NOT NULL;