use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub poll_interval_secs: u64,
    pub ws_reconnect_secs: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        let database_url = env::var("DATABASE_URL")
            .context("DATABASE_URL must be set in environment or .env file")?;
        let poll_interval_secs = env::var("POLL_INTERVAL_SECS")
            .unwrap_or_else(|_| "30".to_string())
            .parse()
            .context("POLL_INTERVAL_SECS must be a valid integer")?;
        let ws_reconnect_secs = env::var("WS_RECONNECT_SECS")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .context("WS_RECONNECT_SECS must be a valid integer")?;
        Ok(Config {
            database_url,
            poll_interval_secs,
            ws_reconnect_secs,
        })
    }
}
