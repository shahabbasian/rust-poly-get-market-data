mod config;
mod db;
mod gamma_client;
mod market_repo;
mod models;
mod scanner;
mod slug;
mod phase2;

use std::time::Duration;

use anyhow::Result;
use tokio::time::interval;
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
    info!("Starting polymarket-scanner phase 1 + phase 2");
    info!("Target: 7 assets x 4 intervals = 28 markets/cycle");
    info!("Scan interval: {}s", config.scan_interval_secs);

    let pool = db::create_pool(&config.database_url).await?;

    let ws_pool = pool.clone();
    let ws_config = config.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = phase2::ws_manager::run_ws(ws_pool.clone(), &ws_config).await {
                error!(error = %e, "Phase 2 WS manager failed, restarting in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });

    let lc_pool = pool.clone();
    let lc_config = config.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(lc_config.lifecycle_interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = phase2::lifecycle::transition_statuses(&lc_pool).await {
                error!(error = %e, "Phase 2 status transition failed");
            }
            if let Err(e) = phase2::lifecycle::compute_price_to_beat_batch(&lc_pool).await {
                error!(error = %e, "Phase 2 price_to_beat failed");
            }
        }
    });

    let rp_pool = pool.clone();
    let rp_config = config.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(rp_config.resolution_poll_interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = phase2::lifecycle::poll_resolutions(&rp_pool, &rp_config).await {
                error!(error = %e, "Phase 2 resolution poll failed");
            }
        }
    });

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
