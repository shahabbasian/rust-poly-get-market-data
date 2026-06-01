-- Phase 2: Lifecycle columns, orderbook L2 storage, strike-price audit.

ALTER TABLE new_markets
    ADD COLUMN IF NOT EXISTS status             VARCHAR(16) NOT NULL DEFAULT 'upcoming',
    ADD COLUMN IF NOT EXISTS live_started_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolved_at        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS winning_outcome    TEXT,
    ADD COLUMN IF NOT EXISTS price_to_beat      DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS price_source       VARCHAR(32),
    ADD COLUMN IF NOT EXISTS collector_task_id  UUID;

CREATE INDEX IF NOT EXISTS idx_new_markets_status_start
    ON new_markets (status, start_date);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id           BIGSERIAL PRIMARY KEY,
    market_id    UUID         NOT NULL REFERENCES new_markets(id) ON DELETE CASCADE,
    asset_id     VARCHAR(128) NOT NULL,
    side         VARCHAR(4)   NOT NULL CHECK (side IN ('yes','no')),
    bids         JSONB        NOT NULL,
    asks         JSONB        NOT NULL,
    hash         VARCHAR(128),
    ts_exchange  TIMESTAMPTZ,
    ts_received  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_snapshots_market_ts
    ON orderbook_snapshots (market_id, ts_received);

CREATE TABLE IF NOT EXISTS orderbook_deltas (
    id           BIGSERIAL PRIMARY KEY,
    market_id    UUID         NOT NULL REFERENCES new_markets(id) ON DELETE CASCADE,
    asset_id     VARCHAR(128) NOT NULL,
    side         VARCHAR(4)   NOT NULL CHECK (side IN ('yes','no')),
    price        DOUBLE PRECISION NOT NULL,
    new_size     DOUBLE PRECISION NOT NULL,
    best_bid     DOUBLE PRECISION,
    best_ask     DOUBLE PRECISION,
    hash         VARCHAR(128),
    ts_exchange  TIMESTAMPTZ,
    ts_received  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_deltas_market_ts
    ON orderbook_deltas (market_id, ts_received);

CREATE TABLE IF NOT EXISTS orderbook_trades (
    id            BIGSERIAL PRIMARY KEY,
    market_id     UUID         NOT NULL REFERENCES new_markets(id) ON DELETE CASCADE,
    asset_id      VARCHAR(128) NOT NULL,
    side          VARCHAR(4)   NOT NULL,
    price         DOUBLE PRECISION NOT NULL,
    size          DOUBLE PRECISION NOT NULL,
    fee_rate_bps  INTEGER,
    ts_exchange   TIMESTAMPTZ,
    ts_received   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trades_market_ts
    ON orderbook_trades (market_id, ts_received);

CREATE TABLE IF NOT EXISTS strike_price_attempts (
    id           BIGSERIAL PRIMARY KEY,
    market_id    UUID         NOT NULL REFERENCES new_markets(id) ON DELETE CASCADE,
    attempted_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    source       VARCHAR(32),
    success      BOOLEAN      NOT NULL,
    price        DOUBLE PRECISION,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_strike_attempts_market
    ON strike_price_attempts (market_id, attempted_at DESC);
