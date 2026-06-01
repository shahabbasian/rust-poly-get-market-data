ALTER TABLE new_target_series
ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT true,
ADD COLUMN IF NOT EXISTS auto_detected BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_target_series_enabled ON new_target_series(enabled);
