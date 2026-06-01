use anyhow::Result;
use polymarket_scanner::price_sampler::sampler::{self, SamplerConfig};
use polymarket_scanner::{config, db};
use polymarket_scanner::models::ASSET_MAP;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    dotenvy::dotenv().ok();

    let cfg = config::Config::from_env()?;
    info!(
        rtds = %cfg.rtds_host,
        lead_s = cfg.sampler_lead_secs,
        window_s = cfg.strike_window_secs,
        "Starting price_sampler"
    );

    let pool = db::create_pool(&cfg.database_url).await?;

    let symbols: Vec<String> = ASSET_MAP.iter().map(|(s, _)| s.to_string()).collect();

    let sampler_cfg = SamplerConfig {
        rtds_host: cfg.rtds_host.clone(),
        poll_interval: Duration::from_millis(cfg.sampler_poll_ms),
        subscribe_lead_secs: cfg.sampler_lead_secs,
        strike_window_secs: cfg.strike_window_secs,
        symbols,
    };

    sampler::run(pool, sampler_cfg).await
}
