-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Target series configuration: which series slugs we care about
CREATE TABLE IF NOT EXISTS new_target_series (
    id SERIAL PRIMARY KEY,
    series_slug TEXT NOT NULL UNIQUE,
    asset_symbol TEXT NOT NULL,       -- BTC, ETH, SOL, XRP
    interval TEXT NOT NULL,           -- 5m, 15m, hourly, 4h
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Markets discovered from Polymarket Gamma API / WS
CREATE TABLE IF NOT EXISTS new_markets (
    id SERIAL PRIMARY KEY,
    polymarket_market_id TEXT NOT NULL UNIQUE,
    condition_id TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    question TEXT,
    description TEXT,
    event_slug TEXT,
    series_slug TEXT,
    asset_symbol TEXT,
    interval TEXT,
    outcomes JSONB,
    outcome_prices JSONB,
    clob_token_ids TEXT[],
    tick_size NUMERIC(20, 18),
    maker_base_fee INTEGER,
    taker_base_fee INTEGER,
    fees_enabled BOOLEAN,
    fee_schedule JSONB,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    closed BOOLEAN NOT NULL DEFAULT FALSE,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    resolution_source TEXT,
    image TEXT,
    icon TEXT,
    volume NUMERIC,
    liquidity NUMERIC,
    open_interest NUMERIC,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    winning_asset_id TEXT,
    winning_outcome TEXT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_new_markets_series_slug ON new_markets(series_slug);
CREATE INDEX IF NOT EXISTS idx_new_markets_asset_symbol ON new_markets(asset_symbol);
CREATE INDEX IF NOT EXISTS idx_new_markets_interval ON new_markets(interval);
CREATE INDEX IF NOT EXISTS idx_new_markets_active ON new_markets(active);
CREATE INDEX IF NOT EXISTS idx_new_markets_closed ON new_markets(closed);
CREATE INDEX IF NOT EXISTS idx_new_markets_end_date ON new_markets(end_date);

-- Individual token/outcome rows per market (Phase 2 reads from here for WebSocket)
CREATE TABLE IF NOT EXISTS new_market_outcomes (
    id SERIAL PRIMARY KEY,
    market_id INTEGER NOT NULL REFERENCES new_markets(id) ON DELETE CASCADE,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    outcome_price NUMERIC(20, 18),
    UNIQUE(market_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_new_outcomes_token_id ON new_market_outcomes(token_id);
CREATE INDEX IF NOT EXISTS idx_new_outcomes_market_id ON new_market_outcomes(market_id);

-- Discovery log: history of how each market was found/resolution
CREATE TABLE IF NOT EXISTS new_market_discovery_log (
    id BIGSERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES new_markets(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,          -- poll, ws_new_market, ws_market_resolved, etc.
    source TEXT NOT NULL,              -- gamma_api, websocket
    payload JSONB,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_new_discovery_log_market_id ON new_market_discovery_log(market_id);
CREATE INDEX IF NOT EXISTS idx_new_discovery_log_recorded_at ON new_market_discovery_log(recorded_at);

-- Seed the target series we care about
INSERT INTO new_target_series (series_slug, asset_symbol, interval, description)
VALUES
    ('btc-updown-5m', 'BTC', '5m', 'BTC Up or Down 5m'),
    ('eth-updown-5m', 'ETH', '5m', 'ETH Up or Down 5m'),
    ('sol-updown-5m', 'SOL', '5m', 'SOL Up or Down 5m'),
    ('xrp-updown-5m', 'XRP', '5m', 'XRP Up or Down 5m'),
    ('doge-updown-5m', 'DOGE', '5m', 'DOGE Up or Down 5m'),
    ('hype-updown-5m', 'HYPE', '5m', 'HYPE Up or Down 5m'),
    ('bnb-updown-5m', 'BNB', '5m', 'BNB Up or Down 5m'),

    ('btc-updown-15m', 'BTC', '15m', 'BTC Up or Down 15m'),
    ('eth-updown-15m', 'ETH', '15m', 'ETH Up or Down 15m'),
    ('sol-updown-15m', 'SOL', '15m', 'SOL Up or Down 15m'),
    ('xrp-updown-15m', 'XRP', '15m', 'XRP Up or Down 15m'),
    ('doge-updown-15m', 'DOGE', '15m', 'DOGE Up or Down 15m'),
    ('hype-updown-15m', 'HYPE', '15m', 'HYPE Up or Down 15m'),
    ('bnb-updown-15m', 'BNB', '15m', 'BNB Up or Down 15m'),

    ('btc-up-or-down-hourly', 'BTC', 'hourly', 'BTC Up or Down Hourly'),
    ('eth-up-or-down-hourly', 'ETH', 'hourly', 'ETH Up or Down Hourly'),
    ('solana-up-or-down-hourly', 'SOL', 'hourly', 'Solana Up or Down Hourly'),
    ('xrp-up-or-down-hourly', 'XRP', 'hourly', 'XRP Up or Down Hourly'),
    ('dogecoin-up-or-down-hourly', 'DOGE', 'hourly', 'DOGE Up or Down Hourly'),
    ('hype-up-or-down-hourly', 'HYPE', 'hourly', 'HYPE Up or Down Hourly'),
    ('bnb-up-or-down-hourly', 'BNB', 'hourly', 'BNB Up or Down Hourly'),

    ('btc-updown-4h', 'BTC', '4h', 'BTC Up or Down 4h'),
    ('eth-updown-4h', 'ETH', '4h', 'ETH Up or Down 4h'),
    ('sol-updown-4h', 'SOL', '4h', 'SOL Up or Down 4h'),
    ('xrp-updown-4h', 'XRP', '4h', 'XRP Up or Down 4h'),
    ('doge-updown-4h', 'DOGE', '4h', 'DOGE Up or Down 4h'),
    ('hype-updown-4h', 'HYPE', '4h', 'HYPE Up or Down 4h'),
    ('bnb-updown-4h', 'BNB', '4h', 'BNB Up or Down 4h')
ON CONFLICT (series_slug) DO NOTHING;
