use crate::collector::models::{DeltaRow, SnapshotRow, TradeRow};
use anyhow::Context;
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, warn};

#[derive(Debug, Clone)]
pub enum Event {
    Snapshot(SnapshotRow),
    Delta(DeltaRow),
    Trade(TradeRow),
}

pub struct BatchWriterConfig {
    pub flush_interval: Duration,
    pub max_rows: usize,
}

impl Clone for BatchWriterConfig {
    fn clone(&self) -> Self {
        Self {
            flush_interval: self.flush_interval,
            max_rows: self.max_rows,
        }
    }
}

impl Default for BatchWriterConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(250),
            max_rows: 1000,
        }
    }
}

pub struct BatchWriter {
    rx: mpsc::Receiver<Event>,
    pool: PgPool,
    cfg: BatchWriterConfig,
}

impl BatchWriter {
    pub fn new(rx: mpsc::Receiver<Event>, pool: PgPool, cfg: BatchWriterConfig) -> Self {
        Self { rx, pool, cfg }
    }

    pub async fn run(mut self) {
        let mut tick = interval(self.cfg.flush_interval);
        let mut snapshots: Vec<SnapshotRow> = Vec::with_capacity(self.cfg.max_rows);
        let mut deltas: Vec<DeltaRow> = Vec::with_capacity(self.cfg.max_rows);
        let mut trades: Vec<TradeRow> = Vec::with_capacity(self.cfg.max_rows);
        loop {
            tokio::select! {
                biased;
                ev = self.rx.recv() => {
                    match ev {
                        Some(Event::Snapshot(s)) => {
                            if snapshots.len() >= self.cfg.max_rows {
                                if let Err(e) = flush_snapshots(&self.pool, &snapshots).await {
                                    error!(error = %e, "flush_snapshots");
                                }
                                snapshots.clear();
                            }
                            snapshots.push(s);
                        }
                        Some(Event::Delta(d)) => {
                            if deltas.len() >= self.cfg.max_rows {
                                if let Err(e) = flush_deltas(&self.pool, &deltas).await {
                                    error!(error = %e, "flush_deltas");
                                }
                                deltas.clear();
                            }
                            deltas.push(d);
                        }
                        Some(Event::Trade(t)) => {
                            if trades.len() >= self.cfg.max_rows {
                                if let Err(e) = flush_trades(&self.pool, &trades).await {
                                    error!(error = %e, "flush_trades");
                                }
                                trades.clear();
                            }
                            trades.push(t);
                        }
                        None => break,
                    }
                }
                _ = tick.tick() => {
                    if !snapshots.is_empty() {
                        if let Err(e) = flush_snapshots(&self.pool, &snapshots).await {
                            error!(error = %e, "flush_snapshots");
                        }
                        snapshots.clear();
                    }
                    if !deltas.is_empty() {
                        if let Err(e) = flush_deltas(&self.pool, &deltas).await {
                            error!(error = %e, "flush_deltas");
                        }
                        deltas.clear();
                    }
                    if !trades.is_empty() {
                        if let Err(e) = flush_trades(&self.pool, &trades).await {
                            error!(error = %e, "flush_trades");
                        }
                        trades.clear();
                    }
                }
            }
        }
        // drain
        for ev in std::iter::from_fn(|| self.rx.try_recv().ok()) {
            match ev {
                Event::Snapshot(s) => snapshots.push(s),
                Event::Delta(d) => deltas.push(d),
                Event::Trade(t) => trades.push(t),
            }
        }
        if !snapshots.is_empty() {
            if let Err(e) = flush_snapshots(&self.pool, &snapshots).await {
                warn!(error = %e, "final flush_snapshots");
            }
        }
        if !deltas.is_empty() {
            if let Err(e) = flush_deltas(&self.pool, &deltas).await {
                warn!(error = %e, "final flush_deltas");
            }
        }
        if !trades.is_empty() {
            if let Err(e) = flush_trades(&self.pool, &trades).await {
                warn!(error = %e, "final flush_trades");
            }
        }
        debug!("BatchWriter exited");
    }
}

async fn flush_snapshots(pool: &PgPool, rows: &[SnapshotRow]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut market_ids = Vec::with_capacity(rows.len());
    let mut asset_ids = Vec::with_capacity(rows.len());
    let mut sides = Vec::with_capacity(rows.len());
    let mut bids = Vec::with_capacity(rows.len());
    let mut asks = Vec::with_capacity(rows.len());
    let mut hashes = Vec::with_capacity(rows.len());
    let mut ts_exch = Vec::with_capacity(rows.len());
    for r in rows {
        market_ids.push(r.market_id);
        asset_ids.push(r.asset_id.clone());
        sides.push(r.side.clone());
        bids.push(r.bids.clone());
        asks.push(r.asks.clone());
        hashes.push(r.hash.clone());
        ts_exch.push(r.ts_exchange);
    }
    sqlx::query(
        r#"
        INSERT INTO orderbook_snapshots
            (market_id, asset_id, side, bids, asks, hash, ts_exchange)
        SELECT * FROM UNNEST(
            $1::uuid[],
            $2::text[],
            $3::text[],
            $4::jsonb[],
            $5::jsonb[],
            $6::text[],
            $7::timestamptz[]
        )
        "#,
    )
    .bind(&market_ids)
    .bind(&asset_ids)
    .bind(&sides)
    .bind(&bids)
    .bind(&asks)
    .bind(&hashes)
    .bind(&ts_exch)
    .execute(pool)
    .await
    .context("insert orderbook_snapshots")?;
    Ok(())
}

async fn flush_deltas(pool: &PgPool, rows: &[DeltaRow]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut market_ids = Vec::with_capacity(rows.len());
    let mut asset_ids = Vec::with_capacity(rows.len());
    let mut sides = Vec::with_capacity(rows.len());
    let mut prices = Vec::with_capacity(rows.len());
    let mut sizes = Vec::with_capacity(rows.len());
    let mut best_bids = Vec::with_capacity(rows.len());
    let mut best_asks = Vec::with_capacity(rows.len());
    let mut hashes = Vec::with_capacity(rows.len());
    let mut ts_exch = Vec::with_capacity(rows.len());
    for r in rows {
        market_ids.push(r.market_id);
        asset_ids.push(r.asset_id.clone());
        sides.push(r.side.clone());
        prices.push(r.price);
        sizes.push(r.new_size);
        best_bids.push(r.best_bid);
        best_asks.push(r.best_ask);
        hashes.push(r.hash.clone());
        ts_exch.push(r.ts_exchange);
    }
    sqlx::query(
        r#"
        INSERT INTO orderbook_deltas
            (market_id, asset_id, side, price, new_size, best_bid, best_ask, hash, ts_exchange)
        SELECT * FROM UNNEST(
            $1::uuid[],
            $2::text[],
            $3::text[],
            $4::float8[],
            $5::float8[],
            $6::float8[],
            $7::float8[],
            $8::text[],
            $9::timestamptz[]
        )
        "#,
    )
    .bind(&market_ids)
    .bind(&asset_ids)
    .bind(&sides)
    .bind(&prices)
    .bind(&sizes)
    .bind(&best_bids)
    .bind(&best_asks)
    .bind(&hashes)
    .bind(&ts_exch)
    .execute(pool)
    .await
    .context("insert orderbook_deltas")?;
    Ok(())
}

async fn flush_trades(pool: &PgPool, rows: &[TradeRow]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut market_ids = Vec::with_capacity(rows.len());
    let mut asset_ids = Vec::with_capacity(rows.len());
    let mut sides = Vec::with_capacity(rows.len());
    let mut prices = Vec::with_capacity(rows.len());
    let mut sizes = Vec::with_capacity(rows.len());
    let mut fees = Vec::with_capacity(rows.len());
    let mut ts_exch = Vec::with_capacity(rows.len());
    for r in rows {
        market_ids.push(r.market_id);
        asset_ids.push(r.asset_id.clone());
        sides.push(r.side.clone());
        prices.push(r.price);
        sizes.push(r.size);
        fees.push(r.fee_rate_bps);
        ts_exch.push(r.ts_exchange);
    }
    sqlx::query(
        r#"
        INSERT INTO orderbook_trades
            (market_id, asset_id, side, price, size, fee_rate_bps, ts_exchange)
        SELECT * FROM UNNEST(
            $1::uuid[],
            $2::text[],
            $3::text[],
            $4::float8[],
            $5::float8[],
            $6::int4[],
            $7::timestamptz[]
        )
        "#,
    )
    .bind(&market_ids)
    .bind(&asset_ids)
    .bind(&sides)
    .bind(&prices)
    .bind(&sizes)
    .bind(&fees)
    .bind(&ts_exch)
    .execute(pool)
    .await
    .context("insert orderbook_trades")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn decimal_to_f64_works() {
        let d = rust_decimal::Decimal::from_str("123.45").unwrap();
        assert_eq!(crate::collector::models::decimal_to_f64(&d), Some(123.45));
    }

    #[test]
    fn snapshot_buffer_grows() {
        let market_id = Uuid::new_v4();
        let s = SnapshotRow {
            market_id,
            asset_id: "asset".into(),
            side: "yes".into(),
            bids: json!([]),
            asks: json!([]),
            hash: None,
            ts_exchange: None,
        };
        assert_eq!(s.side, "yes");
    }
}
