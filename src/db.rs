use crate::models::MarketUpsertData;
use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url)
            .await
            .context("Failed to connect to Postgres")?;
        Ok(Db { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn upsert_market(
        &self,
        data: &MarketUpsertData,
    ) -> Result<i32> {
        let row = sqlx::query(
            r#"
            INSERT INTO new_markets (
                polymarket_market_id, condition_id, slug, question, description,
                event_slug, series_slug, asset_symbol, interval,
                outcomes, outcome_prices, clob_token_ids,
                tick_size, maker_base_fee, taker_base_fee, fees_enabled, fee_schedule,
                active, closed, archived,
                start_date, end_date, resolution_source, image, icon,
                volume, liquidity, open_interest,
                created_at, updated_at, resolved_at, winning_asset_id, winning_outcome,
                last_synced_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12,
                $13, $14, $15, $16, $17,
                $18, $19, $20,
                $21, $22, $23, $24, $25,
                $26, $27, $28,
                $29, $30, $31, $32, $33,
                NOW()
            )
            ON CONFLICT (slug) DO UPDATE SET
                condition_id = EXCLUDED.condition_id,
                question = EXCLUDED.question,
                description = EXCLUDED.description,
                event_slug = EXCLUDED.event_slug,
                series_slug = EXCLUDED.series_slug,
                asset_symbol = EXCLUDED.asset_symbol,
                interval = EXCLUDED.interval,
                outcomes = EXCLUDED.outcomes,
                outcome_prices = EXCLUDED.outcome_prices,
                clob_token_ids = EXCLUDED.clob_token_ids,
                tick_size = EXCLUDED.tick_size,
                maker_base_fee = EXCLUDED.maker_base_fee,
                taker_base_fee = EXCLUDED.taker_base_fee,
                fees_enabled = EXCLUDED.fees_enabled,
                fee_schedule = EXCLUDED.fee_schedule,
                active = EXCLUDED.active,
                closed = EXCLUDED.closed,
                archived = EXCLUDED.archived,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                resolution_source = EXCLUDED.resolution_source,
                image = EXCLUDED.image,
                icon = EXCLUDED.icon,
                volume = EXCLUDED.volume,
                liquidity = EXCLUDED.liquidity,
                open_interest = EXCLUDED.open_interest,
                updated_at = EXCLUDED.updated_at,
                resolved_at = EXCLUDED.resolved_at,
                winning_asset_id = EXCLUDED.winning_asset_id,
                winning_outcome = EXCLUDED.winning_outcome,
                last_synced_at = NOW()
            RETURNING id
            "#
        )
        .bind(&data.polymarket_market_id)
        .bind(&data.condition_id)
        .bind(&data.slug)
        .bind(&data.question)
        .bind(&data.description)
        .bind(&data.event_slug)
        .bind(&data.series_slug)
        .bind(&data.asset_symbol)
        .bind(&data.interval)
        .bind(&data.outcomes.as_ref().map(|v| serde_json::to_value(v).unwrap()))
        .bind(&data.outcome_prices.as_ref().map(|v| serde_json::to_value(v).unwrap()))
        .bind(&data.clob_token_ids)
        .bind(&data.tick_size)
        .bind(&data.maker_base_fee)
        .bind(&data.taker_base_fee)
        .bind(&data.fees_enabled)
        .bind(&data.fee_schedule)
        .bind(data.active)
        .bind(data.closed)
        .bind(data.archived)
        .bind(&data.start_date)
        .bind(&data.end_date)
        .bind(&data.resolution_source)
        .bind(&data.image)
        .bind(&data.icon)
        .bind(&data.volume)
        .bind(&data.liquidity)
        .bind(&data.open_interest)
        .bind(&data.created_at)
        .bind(&data.updated_at)
        .bind(&data.resolved_at)
        .bind(&data.winning_asset_id)
        .bind(&data.winning_outcome)
        .fetch_one(&self.pool)
        .await
        .context("Failed to upsert market")?;

        let market_id: i32 = row.try_get("id")?;
        Ok(market_id)
    }

    pub async fn upsert_outcomes(
        &self,
        market_id: i32,
        token_ids: &[ String ],
        outcomes: &[ String ],
        outcome_prices: &[ f64 ],
    ) -> Result<()> {
        if token_ids.len() != outcomes.len() {
            anyhow::bail!("Token IDs and outcomes length mismatch");
        }

        for (idx, token_id) in token_ids.iter().enumerate() {
            let outcome = outcomes.get(idx).cloned().unwrap_or_default();
            let price = outcome_prices.get(idx).copied();

            sqlx::query(
                r#"
                INSERT INTO new_market_outcomes (market_id, token_id, outcome, outcome_price)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (market_id, token_id) DO UPDATE SET
                    outcome = EXCLUDED.outcome,
                    outcome_price = EXCLUDED.outcome_price
                "#
            )
            .bind(market_id)
            .bind(token_id)
            .bind(outcome)
            .bind(price)
            .execute(&self.pool)
            .await
            .context("Failed to upsert outcome")?;
        }

        Ok(())
    }

    pub async fn resolve_market(
        &self,
        polymarket_market_id: &str,
        winning_asset_id: &str,
        winning_outcome: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE new_markets
            SET active = FALSE,
                closed = TRUE,
                resolved_at = NOW(),
                winning_asset_id = $1,
                winning_outcome = $2,
                last_synced_at = NOW()
            WHERE polymarket_market_id = $3
            "#,
        )
        .bind(winning_asset_id)
        .bind(winning_outcome)
        .bind(polymarket_market_id)
        .execute(&self.pool)
        .await
        .context("Failed to resolve market")?;
        Ok(())
    }

    pub async fn insert_discovery_log(
        &self,
        market_id: Option<i32>,
        event_type: &str,
        source: &str,
        payload: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO new_market_discovery_log (market_id, event_type, source, payload)
            VALUES ($1, $2, $3, $4)
            "#
        )
        .bind(market_id)
        .bind(event_type)
        .bind(source)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("Failed to insert discovery log")?;
        Ok(())
    }

    pub async fn get_target_series(
        &self,
    ) -> Result<Vec<(i32, String, String, String)>> {
        let rows = sqlx::query(
            "SELECT id, series_slug, asset_symbol, interval FROM new_target_series ORDER BY id"
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch target series")?;

        let mut result = Vec::new();
        for row in rows {
            result.push((
                row.try_get::<i32, _>("id")?,
                row.try_get::<String, _>("series_slug")?,
                row.try_get::<String, _>("asset_symbol")?,
                row.try_get::<String, _>("interval")?,
            ));
        }
        Ok(result)
    }
}
