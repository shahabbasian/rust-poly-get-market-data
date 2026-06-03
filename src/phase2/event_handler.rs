use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::phase2::orderbook_repo::{self, OrderBookEvent};

pub async fn start_event_handler(pool: PgPool, config: &crate::config::Config) -> mpsc::Sender<OrderBookEvent> {
    let (tx, rx) = mpsc::channel(config.orderbook_buffer_size);
    let batch_size = config.batch_insert_size;
    let flush_interval = std::time::Duration::from_millis(config.batch_flush_interval_ms);

    tokio::spawn(async move {
        buffer_writer(rx, pool, batch_size, flush_interval).await;
    });

    tx
}

pub async fn dispatch(
    sender: &mpsc::Sender<OrderBookEvent>,
    event_type: &str,
    _market: &str,
    asset_id: &str,
    payload: serde_json::Value,
    timestamp: Option<DateTime<Utc>>,
    pool: &PgPool,
) {
    let token_id_yes = asset_id.to_string();

    let event = OrderBookEvent {
        token_id_yes: token_id_yes.clone(),
        event_type: event_type.to_string(),
        payload: payload.clone(),
        ws_timestamp: timestamp,
    };

    let (tx_result, db_result) = tokio::join!(
        sender.send(event),
        handle_special_events(event_type, &token_id_yes, &payload, pool),
    );

    if let Err(e) = tx_result {
        warn!(error = %e, "Event channel closed, dropping event");
    }
    if let Err(e) = db_result {
        error!(event_type, error = %e, "Failed to handle special event");
    }
}

async fn handle_special_events(
    event_type: &str,
    token_id_yes: &str,
    payload: &serde_json::Value,
    pool: &PgPool,
) -> anyhow::Result<()> {
    match event_type {
        "book" => {
            if let Some(hash) = payload.get("hash").and_then(|v| v.as_str()) {
                orderbook_repo::update_market_last_book_hash(pool, token_id_yes, hash).await?;
            }
        }
        "last_trade_price" => {
            let price = payload.get("price")
                .and_then(|v| {
                    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                });
            if let Some(price) = price {
                orderbook_repo::update_market_last_trade(pool, token_id_yes, price).await?;
            }
        }
        "best_bid_ask" => {
            let bid = payload.get("best_bid")
                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            let ask = payload.get("best_ask")
                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())));
            if let (Some(bid), Some(ask)) = (bid, ask) {
                orderbook_repo::update_market_bba(pool, token_id_yes, bid, ask).await?;
            }
        }
        "market_resolved" => {
            if let Some(outcome) = payload.get("winning_outcome").and_then(|v| v.as_str()) {
                info!(token_id_yes, outcome, "Market resolved via WebSocket");
                orderbook_repo::update_market_winning_outcome(pool, token_id_yes, outcome).await?;
            } else {
                warn!(token_id_yes, "market_resolved event missing winning_outcome");
            }
        }
        _ => {}
    }
    Ok(())
}

async fn buffer_writer(
    mut rx: mpsc::Receiver<OrderBookEvent>,
    pool: PgPool,
    batch_size: usize,
    flush_interval: std::time::Duration,
) {
    let mut buffer: Vec<OrderBookEvent> = Vec::with_capacity(batch_size);

    loop {
        let events_needed = batch_size.saturating_sub(buffer.len());
        if events_needed == 0 {
            flush_batch(&pool, &mut buffer).await;
            continue;
        }

        let recv_future = recv_n(&mut rx, events_needed, &mut buffer);
        match tokio::time::timeout(flush_interval, recv_future).await {
            Ok(false) => {
                flush_batch(&pool, &mut buffer).await;
                break;
            }
            Ok(true) => {
                if buffer.len() >= batch_size {
                    flush_batch(&pool, &mut buffer).await;
                }
            }
            Err(_) => {
                flush_batch(&pool, &mut buffer).await;
            }
        }
    }
}

async fn recv_n(rx: &mut mpsc::Receiver<OrderBookEvent>, n: usize, buffer: &mut Vec<OrderBookEvent>) -> bool {
    for _ in 0..n {
        match rx.recv().await {
            Some(event) => buffer.push(event),
            None => return false,
        }
    }
    true
}

async fn flush_batch(pool: &PgPool, buffer: &mut Vec<OrderBookEvent>) {
    if buffer.is_empty() {
        return;
    }
    let batch: Vec<_> = std::mem::replace(buffer, Vec::with_capacity(buffer.capacity()));
    match orderbook_repo::insert_orderbook_events_batch(pool, &batch).await {
        Ok(()) => {
            debug!(count = batch.len(), "Batch inserted orderbook_events");
        }
        Err(e) => {
            error!(count = batch.len(), error = %e, "Batch insert failed");
        }
    }
}
