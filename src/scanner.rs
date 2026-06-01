use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::gamma_client::{parse_clob_token_ids, parse_optional_datetime, GammaClient};
use crate::market_repo::upsert_market;
use crate::models::MarketRecord;
use crate::slug::candidate_slugs;

pub async fn run_scan(
    pool: &sqlx::PgPool,
    config: &Config,
) -> anyhow::Result<()> {
    info!("Starting market discovery scan");

    let lookahead: HashMap<&str, u32> = HashMap::from([
        ("5m", config.lookahead_hours_5m),
        ("15m", config.lookahead_hours_15m),
        ("1h", config.lookahead_hours_1h),
        ("4h", config.lookahead_hours_4h),
    ]);

    let slugs = candidate_slugs(&lookahead);
    info!("Generated {} candidate slugs", slugs.len());

    let gamma = GammaClient::new(config);
    let now = chrono::Utc::now();
    let mut discovered = 0u64;
    let mut not_found = 0u64;

    for (slug, symbol, interval, _full_name) in &slugs {
        match gamma.get_market_by_slug(slug).await {
            Ok(Some(market)) => {
                let token_ids = parse_clob_token_ids(market.clob_token_ids.as_deref());
                let start_date = parse_optional_datetime(market.start_date.as_deref());
                let end_date = parse_optional_datetime(market.end_date.as_deref());

                let record = MarketRecord {
                    id: uuid::Uuid::new_v4(),
                    symbol: symbol.clone(),
                    interval: interval.clone(),
                    condition_id: market.condition_id.unwrap_or_default(),
                    token_id_yes: token_ids.yes.unwrap_or_default(),
                    token_id_no: token_ids.no.unwrap_or_default(),
                    question: market.question.clone(),
                    slug: slug.clone(),
                    outcomes: market.outcomes.clone(),
                    start_date,
                    end_date,
                    gamma_market_id: market.id.clone(),
                    enable_order_book: market.enable_order_book,
                    accepting_orders: market.accepting_orders,
                    ready: market.ready,
                    funded: market.funded,
                    order_min_size: market.order_min_size,
                    order_price_min_tick_size: market.order_price_min_tick_size,
                    best_bid: market.best_bid,
                    best_ask: market.best_ask,
                    last_trade_price: market.last_trade_price,
                    volume_clob: market.volume_clob,
                    volume_num: market.volume_num,
                    created_at: now,
                    updated_at: now,
                };

                if let Err(e) = upsert_market(pool, &record).await {
                    warn!(slug, error = %e, "Failed to upsert market");
                } else {
                    discovered += 1;
                    debug!(
                        slug,
                        symbol,
                        interval,
                        token_yes = %record.token_id_yes,
                        "Market discovered and stored"
                    );
                }
            }
            Ok(None) => {
                not_found += 1;
                debug!(slug, "Market not found (not yet created)");
            }
            Err(e) => {
                warn!(slug, error = %e, "API error");
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(config.api_delay_ms)).await;
    }

    let total = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM new_markets")
        .fetch_one(pool)
        .await
        .unwrap_or(0);

    info!(
        discovered,
        not_found,
        total_markets = total,
        "Scan completed"
    );

    Ok(())
}
