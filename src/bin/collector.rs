use anyhow::Result;
use polymarket_scanner::collector::batch_writer::BatchWriterConfig;
use polymarket_scanner::collector::scheduler::{self, SchedulerConfig};
use polymarket_scanner::{config, db};
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
        ws_host = %cfg.ws_market_host,
        poll_ms = cfg.scheduler_poll_ms,
        lead_s = cfg.subscribe_lead_secs,
        "Starting collector"
    );

    let pool = db::create_pool(&cfg.database_url).await?;

    let sched_cfg = SchedulerConfig {
        ws_host: cfg.ws_market_host.clone(),
        poll_interval: Duration::from_millis(cfg.scheduler_poll_ms),
        subscribe_lead_secs: cfg.subscribe_lead_secs,
        teardown_grace_secs: cfg.teardown_grace_secs,
        batch_cfg: BatchWriterConfig {
            flush_interval: Duration::from_millis(cfg.batch_flush_ms),
            max_rows: cfg.batch_max_rows,
        },
        channel_capacity: cfg.batch_max_rows * 4,
    };

    scheduler::run(pool, sched_cfg).await
}
