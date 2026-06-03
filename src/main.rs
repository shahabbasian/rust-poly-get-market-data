mod config;
mod db;
mod gamma_client;
mod market_repo;
mod models;
mod phase2;
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
    info!("Starting polymarket-scanner phase 1 + 2");
    info!("Target: {} assets x 4 intervals = 28 markets/cycle", 7);
    info!("Scan interval: {}s", config.scan_interval_secs);
    info!("Lifecycle interval: {}s", config.lifecycle_interval_secs);

    let pool = db::create_pool(&config.database_url).await?;

    // Phase 2 WebSocket manager
    let ws_pool = pool.clone();
    let ws_config = config.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = phase2::ws_manager::run_ws(ws_pool.clone(), ws_config.clone()).await {
                error!(error = %e, "Phase 2 WS manager failed, restarting in 5s");
                tokio::time::sleep(Duration::from_secs(ws_config.ws_reconnect_delay_secs)).await;
            }
        }
    });

    // Phase 2 lifecycle manager
    let lc_pool = pool.clone();
    let lc_config = config.clone();
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(lc_config.lifecycle_interval_secs));
        loop {
            tick.tick().await;
            if let Err(e) = phase2::lifecycle::refresh_all(lc_pool.clone()).await {
                error!(error = %e, "Phase 2 lifecycle refresh failed");
            }
        }
    });

    // Phase 1 scanner loop
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
