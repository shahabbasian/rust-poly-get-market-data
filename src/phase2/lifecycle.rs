use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::{debug, info, warn};

use super::orderbook_repo::{set_price_to_beat, transition_status};

/// Lightweight struct for lifecycle queries.
/// Avoids pulling heavy columns like `outcomes` (JSONB) from the DB.
#[derive(Debug, sqlx::FromRow)]
pub struct MarketLite {
    pub token_id_yes: String,
    pub token_id_no: String,
    pub condition_id: String,
    pub symbol: String,
    pub status: Option<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub price_to_beat: Option<f64>,
}

impl MarketLite {
    fn status_str(&self) -> &str {
        self.status.as_deref().unwrap_or("upcoming")
    }
}

pub async fn refresh_all(pool: PgPool) -> anyhow::Result<()> {
    let now = Utc::now();
    let ahead = chrono::Duration::seconds(
        std::env::var("WATCH_AHEAD_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60),
    );

    // Fetch only the columns we need
    let rows = sqlx::query_as::<_, MarketLite>(
        r#"
        SELECT token_id_yes, token_id_no, condition_id, symbol,
               status, start_date, end_date, price_to_beat
        FROM new_markets
        WHERE status IN ('upcoming', 'active')
           OR status IS NULL
        ORDER BY start_date ASC
        "#,
    )
    .fetch_all(&pool)
    .await?;

    let mut watch_list: Vec<MarketLite> = Vec::new();

    for mut market in rows {
        let current = market.status_str();
        let start = market.start_date;
        let end = market.end_date;

        let new_status = if let Some(end_dt) = end {
            if end_dt <= now {
                "completed"
            } else if let Some(start_dt) = start {
                if start_dt <= now {
                    "active"
                } else {
                    "upcoming"
                }
            } else {
                "upcoming"
            }
        } else if let Some(start_dt) = start {
            if start_dt <= now {
                "active"
            } else {
                "upcoming"
            }
        } else {
            "upcoming"
        };

        // Transition status if changed
        if current != new_status {
            info!(
                token_id_yes = %market.token_id_yes,
                from = current,
                to = new_status,
                "Market status transition"
            );
            transition_status(&pool, &market.token_id_yes, new_status).await?;
            market.status = Some(new_status.to_string());
        }

        // On transition to active, compute price_to_beat if missing
        if new_status == "active" && market.price_to_beat.is_none() {
            if let Some(start_dt) = start {
                if let Some(symbol) = map_symbol_to_chainlink(&market.symbol) {
                    match compute_price_to_beat(&pool, symbol, start_dt).await {
                        Some(price) => {
                            info!(
                                token_id_yes = %market.token_id_yes,
                                price,
                                "Computed price_to_beat"
                            );
                            set_price_to_beat(&pool, &market.token_id_yes, price).await?;
                            market.price_to_beat = Some(price);
                        }
                        None => {
                            warn!(
                                token_id_yes = %market.token_id_yes,
                                symbol,
                                "No chainlink price found for price_to_beat"
                            );
                        }
                    }
                }
            }
        }

        // Determine if this market should be on the watch list
        let should_watch = if new_status == "active" {
            true
        } else if new_status == "upcoming" {
            if let Some(start_dt) = start {
                start_dt <= now + ahead
            } else {
                false
            }
        } else {
            false
        };

        if should_watch {
            watch_list.push(market);
        }
    }

    // Also include any recently completed markets still without winning_outcome
    let _ = check_completed_resolution(&pool).await;

    debug!(watch_count = watch_list.len(), "Lifecycle refresh done");
    Ok(())
}

fn map_symbol_to_chainlink(symbol: &str) -> Option<&'static str> {
    let lower = symbol.to_lowercase();
    match lower.as_str() {
        "btc" => Some("btc/usd"),
        "eth" => Some("eth/usd"),
        "sol" => Some("sol/usd"),
        "xrp" => Some("xrp/usd"),
        "doge" => Some("doge/usd"),
        "hype" => Some("hype/usd"),
        "bnb" => Some("bnb/usd"),
        _ => None,
    }
}

async fn compute_price_to_beat(
    pool: &PgPool,
    symbol: &str,
    start_date: DateTime<Utc>,
) -> Option<f64> {
    let target_ts = start_date.timestamp_millis();

    // Try exact or closest within ±60s
    let row: Option<(f64,)> = sqlx::query_as(
        r#"
        SELECT price FROM chainlink_prices
        WHERE symbol = $1
          AND ABS(timestamp - $2) < 60000
        ORDER BY ABS(timestamp - $2) ASC
        LIMIT 1
        "#,
    )
    .bind(symbol)
    .bind(target_ts)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    row.map(|r| r.0)
}

pub async fn get_watch_list(pool: &PgPool, ahead_secs: i64) -> anyhow::Result<Vec<MarketLite>> {
    let now = Utc::now();
    let ahead = chrono::Duration::seconds(ahead_secs);

    let rows = sqlx::query_as::<_, MarketLite>(
        r#"
        SELECT token_id_yes, token_id_no, condition_id, symbol,
               status, start_date, end_date, price_to_beat
        FROM new_markets
        WHERE status = 'active'
           OR (status = 'upcoming' AND start_date IS NOT NULL AND start_date <= $1)
        ORDER BY start_date ASC
        "#,
    )
    .bind(now + ahead)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

async fn check_completed_resolution(pool: &PgPool) -> anyhow::Result<()> {
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT slug FROM new_markets
        WHERE status = 'completed'
          AND winning_outcome IS NULL
          AND end_date IS NOT NULL
          AND end_date <= now()
        "#,
    )
    .fetch_all(pool)
    .await?;

    for (slug,) in rows {
        debug!(
            slug = %slug,
            "Completed market awaiting resolution outcome"
        );
    }

    Ok(())
}

pub async fn set_winning_outcome_from_gamma(
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
