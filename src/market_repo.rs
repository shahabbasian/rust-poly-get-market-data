use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::models::MarketRecord;

pub async fn upsert_market(pool: &PgPool, record: &MarketRecord) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO new_markets (
            id, symbol, interval, condition_id, token_id_yes, token_id_no,
            question, slug, outcomes, start_date, end_date, gamma_market_id,
            enable_order_book, accepting_orders, ready, funded,
            order_min_size, order_price_min_tick_size, best_bid, best_ask,
            last_trade_price, volume_clob, volume_num, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11, $12,
            $13, $14, $15, $16,
            $17, $18, $19, $20,
            $21, $22, $23, $24, $25
        )
        ON CONFLICT ON CONSTRAINT unique_new_token_id_yes DO UPDATE SET
            condition_id = EXCLUDED.condition_id,
            token_id_no = EXCLUDED.token_id_no,
            question = EXCLUDED.question,
            slug = EXCLUDED.slug,
            outcomes = EXCLUDED.outcomes,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            gamma_market_id = EXCLUDED.gamma_market_id,
            enable_order_book = EXCLUDED.enable_order_book,
            accepting_orders = EXCLUDED.accepting_orders,
            ready = EXCLUDED.ready,
            funded = EXCLUDED.funded,
            order_min_size = EXCLUDED.order_min_size,
            order_price_min_tick_size = EXCLUDED.order_price_min_tick_size,
            best_bid = EXCLUDED.best_bid,
            best_ask = EXCLUDED.best_ask,
            last_trade_price = EXCLUDED.last_trade_price,
            volume_clob = EXCLUDED.volume_clob,
            volume_num = EXCLUDED.volume_num,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(record.id)
    .bind(&record.symbol)
    .bind(&record.interval)
    .bind(&record.condition_id)
    .bind(&record.token_id_yes)
    .bind(&record.token_id_no)
    .bind(&record.question)
    .bind(&record.slug)
    .bind(&record.outcomes)
    .bind(record.start_date)
    .bind(record.end_date)
    .bind(&record.gamma_market_id)
    .bind(record.enable_order_book)
    .bind(record.accepting_orders)
    .bind(record.ready)
    .bind(record.funded)
    .bind(record.order_min_size)
    .bind(record.order_price_min_tick_size)
    .bind(record.best_bid)
    .bind(record.best_ask)
    .bind(record.last_trade_price)
    .bind(record.volume_clob)
    .bind(record.volume_num)
    .bind(record.created_at)
    .bind(record.updated_at)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_upcoming_markets(
    pool: &PgPool,
    from: DateTime<Utc>,
) -> anyhow::Result<Vec<MarketRecord>> {
    let rows = sqlx::query_as::<_, MarketRecord>(
        r#"
        SELECT * FROM new_markets
        WHERE start_date >= $1
           OR start_date IS NULL
        ORDER BY start_date ASC NULLS LAST
        "#,
    )
    .bind(from)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn count_markets(pool: &PgPool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM new_markets")
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}
