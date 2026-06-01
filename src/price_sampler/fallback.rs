use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackSource {
    ChainlinkTable,
}

impl FallbackSource {
    pub fn as_str(self) -> &'static str {
        "chainlink_prices"
    }
}

/// Look up the price closest to `start_date` (±2s) from the local
/// `chainlink_prices` table. `symbol` is the slash format (e.g. "doge/usd").
pub async fn query_nearest(
    pool: &PgPool,
    symbol: &str,
    start: DateTime<Utc>,
    window_secs: i64,
) -> anyhow::Result<Option<f64>> {
    let start_ms: i64 = start.timestamp_millis();
    let window_ms: i64 = window_secs * 1000;
    let row: Option<(f64,)> = sqlx::query_as(
        r#"
        SELECT price::float8
        FROM chainlink_prices
        WHERE symbol = $1
          AND timestamp >= ($2::bigint) - $3::bigint
          AND timestamp <= ($2::bigint) + $3::bigint
        ORDER BY abs(timestamp - $2::bigint) ASC
        LIMIT 1
        "#,
    )
    .bind(symbol)
    .bind(start_ms)
    .bind(window_ms)
    .fetch_optional(pool)
    .await
    .context("query chainlink_prices")?;
    Ok(row.map(|(p,)| p))
}

pub async fn record_attempt(
    pool: &PgPool,
    market_id: Uuid,
    source: &str,
    success: bool,
    price: Option<f64>,
    note: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO strike_price_attempts (market_id, source, success, price, note)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(market_id)
    .bind(source)
    .bind(success)
    .bind(price)
    .bind(note)
    .execute(pool)
    .await
    .context("insert strike_price_attempts")?;
    Ok(())
}

pub async fn write_strike(
    pool: &PgPool,
    market_id: Uuid,
    price: f64,
    source: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE new_markets
        SET price_to_beat = $2,
            price_source  = $3
        WHERE id = $1
          AND price_to_beat IS NULL
        "#,
    )
    .bind(market_id)
    .bind(price)
    .bind(source)
    .execute(pool)
    .await
    .context("update new_markets.price_to_beat")?;
    Ok(())
}
