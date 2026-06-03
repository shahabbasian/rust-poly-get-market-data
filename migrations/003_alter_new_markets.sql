ALTER TABLE new_markets ADD COLUMN IF NOT EXISTS status          VARCHAR(20)  DEFAULT 'upcoming';
ALTER TABLE new_markets ADD COLUMN IF NOT EXISTS winning_outcome VARCHAR(10);
ALTER TABLE new_markets ADD COLUMN IF NOT EXISTS price_to_beat   DOUBLE PRECISION;
ALTER TABLE new_markets ADD COLUMN IF NOT EXISTS last_book_hash  VARCHAR(128);

CREATE INDEX IF NOT EXISTS idx_new_markets_status ON new_markets (status);
