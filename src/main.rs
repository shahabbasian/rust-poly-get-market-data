mod config;
mod db;
mod gamma_client;
mod market_repo;
mod models;
mod scanner;
mod slug;

use anyhow::Result;
use tokio::time::{interval, Duration};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    dotenvy::dotenv().ok();

    let config = config::Config::from_env()?;
    info!("Starting polymarket-scanner phase 1");
    info!("Target: {} assets x 4 intervals = 28 markets/cycle", 7);
    info!("Scan interval: {}s", config.scan_interval_secs);

    let pool = db::create_pool(&config.database_url).await?;

    let mut scan_timer = interval(Duration::from_secs(config.scan_interval_secs));

    loop {
        tokio::select! {
            _ = scan_timer.tick() => {
                if let Err(e) = scanner::run_scan(&pool, &config).await {
                    error!(error = %e, "Scan failed");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    info!("Shutdown complete");
    Ok(())
}
