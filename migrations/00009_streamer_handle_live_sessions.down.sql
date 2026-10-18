DROP TABLE IF EXISTS live_sessions;
ALTER TABLE streamers DROP CONSTRAINT IF EXISTS uq_streamers_handle;
ALTER TABLE streamers DROP COLUMN IF EXISTS handle;