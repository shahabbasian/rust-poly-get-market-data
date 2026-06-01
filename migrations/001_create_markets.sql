CREATE TABLE IF NOT EXISTS new_markets (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol          VARCHAR(10)  NOT NULL,
    interval        VARCHAR(10)  NOT NULL,
    condition_id    VARCHAR(128),
    token_id_yes    VARCHAR(128),
    token_id_no     VARCHAR(128),
    question        TEXT,
    slug            TEXT         NOT NULL,
    outcomes        TEXT,
    start_date      TIMESTAMPTZ,
    end_date        TIMESTAMPTZ,
    gamma_market_id VARCHAR(64),
    enable_order_book BOOLEAN,
    accepting_orders  BOOLEAN,
    ready           BOOLEAN,
    funded          BOOLEAN,
    order_min_size  DOUBLE PRECISION,
    order_price_min_tick_size DOUBLE PRECISION,
    best_bid        DOUBLE PRECISION,
    best_ask        DOUBLE PRECISION,
    last_trade_price DOUBLE PRECISION,
    volume_clob     DOUBLE PRECISION,
    volume_num      DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT unique_new_token_id_yes UNIQUE (token_id_yes)
);

CREATE INDEX IF NOT EXISTS idx_new_markets_symbol_interval ON new_markets (symbol, interval);
CREATE INDEX IF NOT EXISTS idx_new_markets_start_date        ON new_markets (start_date);
CREATE INDEX IF NOT EXISTS idx_new_markets_token_id_yes       ON new_markets (token_id_yes);
