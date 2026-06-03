use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::models::MarketRecord;

pub struct OrderBookEvent {
    pub token_id_yes: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub ws_timestamp: Option<DateTime<Utc>>,
}

pub async fn insert_orderbook_events_batch(pool: &PgPool, events: &[OrderBookEvent]) -> anyhow::Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut placeholders = Vec::with_capacity(events.len());
    for i in 0..events.len() {
        let base = i * 4;
        placeholders.push(format!("(${}, ${}, ${}, ${})", base + 1, base + 2, base + 3, base + 4));
    }

    let query_str = format!(
        "INSERT INTO orderbook_events (token_id_yes, event_type, payload, ws_timestamp) VALUES {}",
        placeholders.join(", ")
    );

    let mut query = sqlx::query(&query_str);
    for event in events {
        query = query
            .bind(&event.token_id_yes)
            .bind(&event.event_type)
            .bind(&event.payload)
            .bind(event.ws_timestamp);
    }

    query.execute(pool).await?;
    Ok(())
}

pub async fn update_market_status(pool: &PgPool, token_id_yes: &str, status: &str) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE new_markets SET status = $1, updated_at = now() WHERE token_id_yes = $2"
    )
    .bind(status)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_winning_outcome(pool: &PgPool, token_id_yes: &str, outcome: &str) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE new_markets SET winning_outcome = $1, status = 'completed', updated_at = now() WHERE token_id_yes = $2"
    )
    .bind(outcome)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_price_to_beat(pool: &PgPool, token_id_yes: &str, price: f64) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE new_markets SET price_to_beat = $1, updated_at = now() WHERE token_id_yes = $2"
    )
    .bind(price)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_last_book_hash(pool: &PgPool, token_id_yes: &str, hash: &str) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE new_markets SET last_book_hash = $1, updated_at = now() WHERE token_id_yes = $2"
    )
    .bind(hash)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_bba(pool: &PgPool, token_id_yes: &str, best_bid: f64, best_ask: f64) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE new_markets SET best_bid = $1, best_ask = $2, updated_at = now() WHERE token_id_yes = $3"
    )
    .bind(best_bid)
    .bind(best_ask)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_last_trade(pool: &PgPool, token_id_yes: &str, price: f64) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE new_markets SET last_trade_price = $1, updated_at = now() WHERE token_id_yes = $2"
    )
    .bind(price)
    .bind(token_id_yes)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_markets_for_watch(pool: &PgPool, watch_ahead_secs: i64) -> anyhow::Result<Vec<MarketRecord>> {
    let now = Utc::now();
    let cutoff = now + chrono::Duration::seconds(watch_ahead_secs);

    let rows = sqlx::query_as::<_, MarketRecord>(
        r#"
        SELECT * FROM new_markets
        WHERE status = 'active'
           OR (status = 'upcoming' AND start_date <= $1)
           OR (status IS NULL AND start_date IS NOT NULL AND start_date <= $1)
        ORDER BY start_date ASC NULLS LAST
        "#,
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_markets_past_end_date(pool: &PgPool) -> anyhow::Result<Vec<MarketRecord>> {
    let rows = sqlx::query_as::<_, MarketRecord>(
        r#"
        SELECT * FROM new_markets
        WHERE status = 'active'
          AND end_date IS NOT NULL
          AND end_date <= now()
          AND winning_outcome IS NULL
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_chainlink_price(pool: &PgPool, symbol: &str, target_timestamp: i64) -> anyhow::Result<Option<f64>> {
    let row: Option<(Option<f64>,)> = sqlx::query_as(
        r#"
        SELECT CAST(price AS DOUBLE PRECISION) FROM chainlink_prices
        WHERE symbol = $1
          AND ABS(timestamp - $2) < 60000
        ORDER BY ABS(timestamp - $2) ASC
        LIMIT 1
        "#,
    )
    .bind(symbol)
    .bind(target_timestamp)
    .fetch_optional(pool)
    .await?;

    Ok(row.and_then(|(price,)| price))
}
