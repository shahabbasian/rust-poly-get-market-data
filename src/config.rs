use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub gamma_api_url: String,
    pub scan_interval_secs: u64,
    pub lookahead_hours_5m: u32,
    pub lookahead_hours_15m: u32,
    pub lookahead_hours_1h: u32,
    pub lookahead_hours_4h: u32,
    pub api_delay_ms: u64,

    pub ws_market_host: String,
    pub rtds_host: String,
    pub scheduler_poll_ms: u64,
    pub subscribe_lead_secs: i64,
    pub batch_flush_ms: u64,
    pub batch_max_rows: usize,
    pub strike_window_secs: i64,
    pub teardown_grace_secs: i64,
    pub sampler_poll_ms: u64,
    pub sampler_lead_secs: i64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://user:password@localhost:5432/polymarket".into()),
            gamma_api_url: env::var("GAMMA_API_URL")
                .unwrap_or_else(|_| "https://gamma-api.polymarket.com".into()),
            scan_interval_secs: parse_env("SCAN_INTERVAL_SECS", 120),
            lookahead_hours_5m: parse_env("LOOKAHEAD_HOURS_5M", 6),
            lookahead_hours_15m: parse_env("LOOKAHEAD_HOURS_15M", 12),
            lookahead_hours_1h: parse_env("LOOKAHEAD_HOURS_1H", 24),
            lookahead_hours_4h: parse_env("LOOKAHEAD_HOURS_4H", 72),
            api_delay_ms: parse_env("API_DELAY_MS", 150),

            ws_market_host: env::var("WS_MARKET_HOST")
                .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com".into()),
            rtds_host: env::var("RTDS_HOST")
                .unwrap_or_else(|_| "wss://ws-live-data.polymarket.com".into()),
            scheduler_poll_ms: parse_env("SCHEDULER_POLL_MS", 1000),
            subscribe_lead_secs: parse_env("SUBSCRIBE_LEAD_SECS", 5),
            batch_flush_ms: parse_env("BATCH_FLUSH_MS", 250),
            batch_max_rows: parse_env("BATCH_MAX_ROWS", 1000usize),
            strike_window_secs: parse_env("STRIKE_WINDOW_SECS", 2),
            teardown_grace_secs: parse_env("TEARDOWN_GRACE_SECS", 30),
            sampler_poll_ms: parse_env("SAMPLER_POLL_MS", 1000),
            sampler_lead_secs: parse_env("SAMPLER_LEAD_SECS", 5),
        })
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
