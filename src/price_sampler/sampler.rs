use crate::price_sampler::fallback;
use crate::price_sampler::rtds_client::{
    make_buffers, nearest_tick, rtds_symbols, run_rtds, TickBuffers, TickSource,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};
use uuid::Uuid;

pub struct SamplerConfig {
    pub rtds_host: String,
    pub poll_interval: Duration,
    pub subscribe_lead_secs: i64,
    pub strike_window_secs: i64,
    pub symbols: Vec<String>,
}

/// Poll DB for markets whose start_date is within `subscribe_lead_secs` of now,
/// wait until start_date, attempt to sample from RTDS, fall back to the
/// `chainlink_prices` table.
pub async fn run(pool: PgPool, cfg: SamplerConfig) -> anyhow::Result<()> {
    let buffers: TickBuffers = make_buffers();
    let (sd_tx, sd_rx) = watch::channel(false);

    let buffers_for_rtds = buffers.clone();
    let symbols = cfg.symbols.clone();
    let host = cfg.rtds_host.clone();
    let rtds_handle = tokio::spawn(async move {
        if let Err(e) = run_rtds(&host, &symbols, buffers_for_rtds, sd_rx).await {
            error!(error = %e, "rtds loop exited");
        }
    });

    let mut tick = interval(cfg.poll_interval);
    let mut sampled: HashSet<Uuid> = HashSet::new();

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(e) = poll_once(&pool, &cfg, &buffers, &mut sampled).await {
                    error!(error = %e, "sampler poll failed");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("sampler: shutdown");
                break;
            }
        }
    }

    let _ = sd_tx.send(true);
    let _ = rtds_handle.await;
    Ok(())
}

async fn poll_once(
    pool: &PgPool,
    cfg: &SamplerConfig,
    buffers: &TickBuffers,
    sampled: &mut HashSet<Uuid>,
) -> anyhow::Result<()> {
    let rows: Vec<(Uuid, String, Option<DateTime<Utc>>)> = sqlx::query_as(
        r#"
        SELECT id, symbol, start_date
        FROM new_markets
        WHERE start_date IS NOT NULL
          AND start_date <= now() + make_interval(secs => $1)
          AND start_date >= now() - INTERVAL '10 minutes'
          AND price_to_beat IS NULL
        ORDER BY start_date ASC
        LIMIT 50
        "#,
    )
    .bind(cfg.subscribe_lead_secs as f64)
    .fetch_all(pool)
    .await
    .context("select markets to sample")?;

    for (id, symbol, start_date_opt) in rows {
        if sampled.contains(&id) {
            continue;
        }
        let Some(start_date) = start_date_opt else {
            continue;
        };
        let now = Utc::now();
        if start_date > now {
            // Not yet — wait the remaining time.
            let wait = (start_date - now).to_std().unwrap_or(Duration::from_millis(0));
            sleep(wait + Duration::from_millis(50)).await;
        }
        // First state transition record.
        sampled.insert(id);

        let slash = rtds_symbols(&symbol)
            .map(|m| m.fallback_slash)
            .unwrap_or_else(|| format!("{}/usd", symbol.to_lowercase()));

        // Try RTDS first.
        let preferred = TickSource::Chainlink;
        let tick = nearest_tick(buffers, &slash, start_date, cfg.strike_window_secs, preferred);
        if let Some(t) = tick {
            let source = match t.source {
                TickSource::Chainlink => "rtds_chainlink",
                TickSource::Binance => "rtds_binance",
            };
            if let Err(e) = fallback::write_strike(pool, id, t.value, source).await {
                warn!(market_id = %id, error = %e, "write_strike");
            }
            let _ = fallback::record_attempt(pool, id, source, true, Some(t.value), None).await;
            info!(market_id = %id, symbol = %symbol, source, price = t.value, "strike sampled from RTDS");
            continue;
        }

        // Fallback: chainlink_prices table.
        match fallback::query_nearest(pool, &slash, start_date, cfg.strike_window_secs).await {
            Ok(Some(p)) => {
                let source = "chainlink_prices_table";
                if let Err(e) = fallback::write_strike(pool, id, p, source).await {
                    warn!(market_id = %id, error = %e, "write_strike fallback");
                }
                let _ = fallback::record_attempt(pool, id, source, true, Some(p), None).await;
                info!(market_id = %id, symbol = %symbol, source, price = p, "strike sampled from chainlink_prices");
            }
            Ok(None) => {
                let _ = fallback::record_attempt(
                    pool,
                    id,
                    "chainlink_prices_table",
                    false,
                    None,
                    Some("no tick in window"),
                )
                .await;
                warn!(market_id = %id, symbol = %symbol, "no strike available (RTDS or fallback)");
            }
            Err(e) => {
                let _ = fallback::record_attempt(
                    pool,
                    id,
                    "chainlink_prices_table",
                    false,
                    None,
                    Some(&format!("query error: {e}")),
                )
                .await;
                warn!(market_id = %id, error = %e, "fallback query error");
            }
        }
    }

    Ok(())
}
