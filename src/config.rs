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
    // Phase 2
    pub ws_reconnect_delay_secs: u64,
    pub lifecycle_interval_secs: u64,
    pub watch_ahead_secs: i64,
    pub orderbook_buffer_size: usize,
    pub batch_insert_size: usize,
    pub batch_flush_interval_ms: u64,
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
            ws_reconnect_delay_secs: parse_env("WS_RECONNECT_DELAY_SECS", 5),
            lifecycle_interval_secs: parse_env("LIFECYCLE_INTERVAL_SECS", 5),
            watch_ahead_secs: parse_env("WATCH_AHEAD_SECS", 60i64),
            orderbook_buffer_size: parse_env("ORDERBOOK_BUFFER_SIZE", 10_000usize),
            batch_insert_size: parse_env("BATCH_INSERT_SIZE", 500usize),
            batch_flush_interval_ms: parse_env("BATCH_FLUSH_INTERVAL_MS", 500u64),
        })
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
