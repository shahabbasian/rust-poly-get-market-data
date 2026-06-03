use chrono::Utc;
use sqlx::PgPool;
use tracing::{debug, error, info, warn};

use crate::models::MarketRecord;
use crate::phase2::orderbook_repo;

const CHAINLINK_SYMBOL_MAP: &[(&str, &str)] = &[
    ("btc", "btc/usd"),
    ("eth", "eth/usd"),
    ("sol", "sol/usd"),
    ("xrp", "xrp/usd"),
    ("doge", "doge/usd"),
    ("hype", "hype/usd"),
    ("bnb", "bnb/usd"),
];

fn chainlink_symbol(symbol: &str) -> &str {
    CHAINLINK_SYMBOL_MAP
        .iter()
        .find(|(s, _)| s.eq_ignore_ascii_case(symbol))
        .map(|(_, target)| *target)
        .unwrap_or(symbol)
}


pub async fn transition_statuses(pool: &PgPool) -> anyhow::Result<()> {
    let now = Utc::now();

    let upcoming: Vec<MarketRecord> = sqlx::query_as::<_, MarketRecord>(
        r#"
        SELECT * FROM new_markets
        WHERE (status = 'upcoming' OR status IS NULL)
          AND start_date IS NOT NULL
          AND start_date <= $1
        "#,
    )
    .bind(now)
    .fetch_all(pool)
    .await?;

    for market in &upcoming {
        info!(
            token_yes = %market.token_id_yes,
            slug = %market.slug,
            "Market transitioning upcoming → active"
        );
        if let Err(e) = orderbook_repo::update_market_status(pool, &market.token_id_yes, "active").await {
            error!(token_yes = %market.token_id_yes, error = %e, "Failed to set status=active");
        }
    }

    let to_complete: Vec<MarketRecord> = sqlx::query_as::<_, MarketRecord>(
        r#"
        SELECT * FROM new_markets
        WHERE status = 'active'
          AND end_date IS NOT NULL
          AND end_date <= $1
          AND winning_outcome IS NULL
        "#,
    )
    .bind(now)
    .fetch_all(pool)
    .await?;

    for market in &to_complete {
        debug!(
            token_yes = %market.token_id_yes,
            slug = %market.slug,
            "Market past end_date, awaiting resolution"
        );
    }

    Ok(())
}

pub async fn compute_price_to_beat_batch(pool: &PgPool) -> anyhow::Result<()> {
    let candidates: Vec<MarketRecord> = sqlx::query_as::<_, MarketRecord>(
        r#"
        SELECT * FROM new_markets
        WHERE status = 'active'
          AND price_to_beat IS NULL
          AND start_date IS NOT NULL
        "#,
    )
    .fetch_all(pool)
    .await?;

    for market in &candidates {
        let Some(start_date) = market.start_date else {
            continue;
        };

        let chain_sym = chainlink_symbol(&market.symbol);
        let target_ts = start_date.timestamp_millis();

        match orderbook_repo::get_chainlink_price(pool, chain_sym, target_ts).await {
            Ok(Some(price)) => {
                if let Err(e) = orderbook_repo::update_market_price_to_beat(pool, &market.token_id_yes, price).await {
                    error!(token_yes = %market.token_id_yes, error = %e, "Failed to set price_to_beat");
                } else {
                    info!(
                        token_yes = %market.token_id_yes,
                        symbol = %market.symbol,
                        chain_sym,
                        price,
                        "Computed price_to_beat"
                    );
                }
            }
            Ok(None) => {
                debug!(
                    token_yes = %market.token_id_yes,
                    symbol = %market.symbol,
                    chain_sym,
                    "No chainlink price found within ±60s of start_date"
                );
            }
            Err(e) => {
                warn!(
                    token_yes = %market.token_id_yes,
                    symbol = %market.symbol,
                    error = %e,
                    "Failed to query chainlink price"
                );
            }
        }
    }

    Ok(())
}

pub async fn poll_resolutions(pool: &PgPool, config: &crate::config::Config) -> anyhow::Result<()> {
    let markets = orderbook_repo::get_markets_past_end_date(pool).await?;

    if markets.is_empty() {
        return Ok(());
    }

    let gamma_client = crate::gamma_client::GammaClient::new(config);

    for market in &markets {
        debug!(
            slug = %market.slug,
            "Polling Gamma API for market resolution"
        );

        match gamma_client.get_market_by_slug(&market.slug).await {
            Ok(Some(resp)) => {
                if resp.closed == Some(true) {
                    let outcome = determine_winner(&resp);

                    if outcome != "Unknown" {
                        match orderbook_repo::update_market_winning_outcome(pool, &market.token_id_yes, &outcome).await {
                            Ok(()) => {
                                info!(
                                    token_yes = %market.token_id_yes,
                                    slug = %market.slug,
                                    outcome,
                                    "Market resolved (Gamma fallback)"
                                );
                            }
                            Err(e) => {
                                error!(token_yes = %market.token_id_yes, error = %e, "Failed to set winning_outcome");
                            }
                        }
                    } else {
                        debug!(slug = %market.slug, "Market closed but outcome unclear, will retry next poll");
                    }
                }
            }
            Ok(None) => {
                debug!(slug = %market.slug, "Market not found on Gamma API");
            }
            Err(e) => {
                warn!(slug = %market.slug, error = %e, "Gamma API error during resolution poll");
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(config.api_delay_ms)).await;
    }

    Ok(())
}

fn determine_winner(
    resp: &crate::models::GammaMarketResponse,
) -> String {
    let outcome_prices_str = resp.outcome_prices.as_deref().unwrap_or("[]");
    let outcomes_str = resp.outcomes.as_deref().unwrap_or("[]");

    let outcomes: Vec<String> = serde_json::from_str(outcomes_str).unwrap_or_default();
    let price_values: Vec<String> = serde_json::from_str(outcome_prices_str).unwrap_or_default();
    let prices: Vec<f64> = price_values
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    if prices.len() == 2 && outcomes.len() == 2 {
        let d0 = (prices[0] - 1.0).abs();
        let d1 = (prices[1] - 1.0).abs();
        if (d0 - d1).abs() < 0.001 {
            return "Unknown".to_string();
        }
        if d0 < d1 {
            outcomes[0].clone()
        } else {
            outcomes[1].clone()
        }
    } else {
        "Unknown".to_string()
    }
}
