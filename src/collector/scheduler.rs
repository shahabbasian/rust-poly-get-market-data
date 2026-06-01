use crate::collector::batch_writer::{BatchWriter, BatchWriterConfig, Event};
use crate::collector::models::MarketRow;
use crate::collector::status;
use crate::collector::ws_task;
use anyhow::Context;
use dashmap::DashMap;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub struct SchedulerConfig {
    pub ws_host: String,
    pub poll_interval: Duration,
    pub subscribe_lead_secs: i64,
    pub teardown_grace_secs: i64,
    pub batch_cfg: BatchWriterConfig,
    pub channel_capacity: usize,
}

pub async fn run(pool: PgPool, cfg: SchedulerConfig) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel::<Event>(cfg.channel_capacity.max(1000));
    let writer_handle = tokio::spawn({
        let pool = pool.clone();
        let cfg = cfg.batch_cfg.clone();
        async move { BatchWriter::new(rx, pool, cfg).run().await }
    });

    let tasks: Arc<DashMap<Uuid, JoinHandle<()>>> = Arc::new(DashMap::new());

    let mut poll = interval(cfg.poll_interval);
    let mut teardown = interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = poll.tick() => {
                if let Err(e) = poll_due(&pool, &cfg, &tx, &tasks).await {
                    error!(error = %e, "poll_due failed");
                }
            }
            _ = teardown.tick() => {
                if let Err(e) = poll_teardown(&pool, &cfg, &tasks).await {
                    error!(error = %e, "poll_teardown failed");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("scheduler: shutdown signal");
                break;
            }
        }
    }

    info!("scheduler: awaiting outstanding tasks");
    for entry in tasks.iter() {
        entry.value().abort();
    }
    drop(tx);
    let _ = writer_handle.await;
    Ok(())
}

async fn poll_due(
    pool: &PgPool,
    cfg: &SchedulerConfig,
    tx: &mpsc::Sender<Event>,
    tasks: &Arc<DashMap<Uuid, JoinHandle<()>>>,
) -> anyhow::Result<()> {
    let rows = sqlx::query_as::<_, MarketRow>(
        r#"
        SELECT id, symbol, interval, condition_id, token_id_yes, token_id_no, start_date, end_date
        FROM new_markets
        WHERE status = 'upcoming'
          AND start_date IS NOT NULL
          AND start_date <= now() + make_interval(secs => $1)
        ORDER BY start_date ASC
        LIMIT 50
        "#,
    )
    .bind(cfg.subscribe_lead_secs as f64)
    .fetch_all(pool)
    .await
    .context("select upcoming markets")?;

    for row in rows {
        let task_id = Uuid::new_v4();
        let claimed = status::try_claim_live(pool, row.id, task_id)
            .await
            .unwrap_or(false);
        if !claimed {
            debug!(market_id = %row.id, "lost claim race (already claimed)");
            continue;
        }
        let market_id = row.id;
        let symbol = row.symbol.clone();
        let interval = row.interval.clone();
        let pool_clone = pool.clone();
        let tx_clone = tx.clone();
        let ws_host = cfg.ws_host.clone();
        let row_clone = row;
        let handle = tokio::spawn(async move {
            if let Err(e) = ws_task::run(row_clone, ws_host, tx_clone, pool_clone).await {
                error!(error = %e, "ws_task failed");
            }
        });
        tasks.insert(market_id, handle);
        info!(market_id = %market_id, symbol = %symbol, interval = %interval, "spawned ws task");
    }

    Ok(())
}

async fn poll_teardown(
    pool: &PgPool,
    cfg: &SchedulerConfig,
    tasks: &Arc<DashMap<Uuid, JoinHandle<()>>>,
) -> anyhow::Result<()> {
    let grace = cfg.teardown_grace_secs as f64;
    let stale: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT id FROM new_markets
        WHERE status = 'live'
          AND end_date IS NOT NULL
          AND end_date <= now() - make_interval(secs => $1)
        "#,
    )
    .bind(grace)
    .fetch_all(pool)
    .await
    .context("select stale live markets")?;

    for id in stale {
        if let Some((_, handle)) = tasks.remove(&id) {
            warn!(market_id = %id, "aborting stale ws task past end_date + grace");
            handle.abort();
        }
    }
    Ok(())
}
