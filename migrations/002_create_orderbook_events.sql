CREATE TABLE IF NOT EXISTS orderbook_events (
    id            BIGSERIAL PRIMARY KEY,
    token_id_yes  VARCHAR(128) NOT NULL,
    event_type    VARCHAR(30)  NOT NULL,
    payload       JSONB        NOT NULL,
    ws_timestamp  TIMESTAMPTZ,
    received_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_obe_token_id      ON orderbook_events (token_id_yes);
CREATE INDEX IF NOT EXISTS idx_obe_received_at   ON orderbook_events (received_at);
CREATE INDEX IF NOT EXISTS idx_obe_event_type    ON orderbook_events (token_id_yes, event_type);
