use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{debug, error, trace};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookEvent {
    pub token_id_yes: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub ws_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct BatchInsertRequest {
    pub events: Vec<OrderBookEvent>,
}

pub fn spawn_event_writer(
    pool: PgPool,
    batch_size: usize,
    flush_interval_ms: u64,
) -> mpsc::Sender<OrderBookEvent> {
    let (tx, mut rx) = mpsc::channel::<OrderBookEvent>(10000);

    tokio::spawn(async move {
        let mut buffer = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(flush_interval_ms));

        loop {
            tokio::select! {
                biased;
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        if let Err(e) = flush_batch(&pool, &buffer).await {
                            error!(error = %e, "Batch flush failed");
                        }
                        buffer.clear();
                    }
                }
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                            buffer.push(event);
                            if buffer.len() >= batch_size {
                                if let Err(e) = flush_batch(&pool, &buffer).await {
                                    error!(error = %e, "Batch flush failed");
                                }
                                buffer.clear();
                            }
                        }
                        None => {
                            // channel closed
                            if !buffer.is_empty() {
                                if let Err(e) = flush_batch(&pool, &buffer).await {
                                    error!(error = %e, "Final batch flush failed");
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
    });

    tx
}

async fn flush_batch(pool: &PgPool, events: &[OrderBookEvent]) -> anyhow::Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let token_ids: Vec<String> = events.iter().map(|e| e.token_id_yes.clone()).collect();
    let event_types: Vec<String> = events.iter().map(|e| e.event_type.clone()).collect();
    let payloads: Vec<serde_json::Value> = events.iter().map(|e| e.payload.clone()).collect();
    let ws_timestamps: Vec<Option<DateTime<Utc>>> = events.iter().map(|e| e.ws_timestamp).collect();

    sqlx::query(
        r#"
        INSERT INTO orderbook_events (token_id_yes, event_type, payload, ws_timestamp)
        SELECT * FROM UNNEST ($1::VARCHAR[], $2::VARCHAR[], $3::JSONB[], $4::TIMESTAMPTZ[])
        "#,
    )
    .bind(&token_ids)
    .bind(&event_types)
    .bind(&payloads)
    .bind(&ws_timestamps)
    .execute(pool)
    .await?;

    trace!(count = events.len(), "Flushed orderbook events batch");
    Ok(())
}

pub async fn update_last_book_hash(
    pool: &PgPool,
    token_id_yes: &str,
    hash: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE new_markets
        SET last_book_hash = $1, updated_at = now()
        WHERE token_id_yes = $2
        "#,
    )
    .bind(hash)
    .bind(token_id_yes)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn update_market_bba(
    pool: &PgPool,
    token_id_yes: &str,
    best_bid: f64,
    best_ask: f64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE new_markets
        SET best_bid = $1, best_ask = $2, updated_at = now()
        WHERE token_id_yes = $3
        "#,
    )
    .bind(best_bid)
    .bind(best_ask)
    .bind(token_id_yes)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn update_last_trade(
    pool: &PgPool,
    token_id_yes: &str,
    price: f64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE new_markets
        SET last_trade_price = $1, updated_at = now()
        WHERE token_id_yes = $2
        "#,
    )
    .bind(price)
    .bind(token_id_yes)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn set_market_resolved(
    pool: &PgPool,
    token_id_yes: &str,
    winning_outcome: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE new_markets
        SET winning_outcome = $1, status = 'completed', updated_at = now()
        WHERE token_id_yes = $2
        "#,
    )
    .bind(winning_outcome)
    .bind(token_id_yes)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn transition_status(
    pool: &PgPool,
    token_id_yes: &str,
    new_status: &str,
) -> anyhow::Result<()> {
    debug!(token_id_yes, new_status, "Transitioning market status");
    sqlx::query(
        r#"
        UPDATE new_markets
        SET status = $1, updated_at = now()
        WHERE token_id_yes = $2
        "#,
    )
    .bind(new_status)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_price_to_beat(
    pool: &PgPool,
    token_id_yes: &str,
    price: f64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE new_markets
        SET price_to_beat = $1, updated_at = now()
        WHERE token_id_yes = $2
        "#,
    )
    .bind(price)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}
