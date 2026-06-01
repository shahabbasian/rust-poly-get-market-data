use polymarket_market_discovery::config::Config;
use polymarket_market_discovery::db::Db;
use polymarket_market_discovery::gamma::GammaClient;
use polymarket_market_discovery::models::{MarketUpsertData, PolymarketEvent, PolymarketMarket};
use tracing_subscriber::EnvFilter;

fn parse_json_array_string(s: &Option<String>) -> Option<Vec<String>> {
    s.as_ref()
        .and_then(|v| serde_json::from_str::<Vec<String>>(v).ok())
}

fn parse_json_number_array(s: &Option<String>) -> Option<Vec<f64>> {
    s.as_ref()
        .and_then(|v| serde_json::from_str::<Vec<f64>>(v).ok())
}

fn parse_tick_size(v: &Option<f64>) -> Option<f64> {
    // Some API responses return EPSILON zeroes; round to readable amount
    v.map(|x| (x * 1_000_000.0).round() / 1_000_000.0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Config::from_env()?;
    let db = Db::connect(&config.database_url).await?;
    tracing::info!("Connected to DB");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let db_ws = db.clone();
    let ws_handle = tokio::spawn(async move {
        if let Err(e) = polymarket_market_discovery::ws::WsClient::run(db_ws, shutdown_rx, config.ws_reconnect_secs).await {
            tracing::error!("WS runner exited: {}", e);
        }
    });

    let gamma = GammaClient::new();

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(config.poll_interval_secs));

    loop {
        interval.tick().await;

        let series = match db.get_target_series().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to get target series: {}", e);
                continue;
            }
        };

        let slugs: Vec<String> = series.iter().map(|(_, slug, _, _)| slug.clone()).collect();

        tracing::info!("Polling {} target series...", slugs.len());

        match gamma.fetch_all_active_markets_for_series(&slugs, 500).await {
            Ok(markets) => {
                tracing::info!("Fetched {} markets", markets.len());
                for market in markets {
                    // Determine series_slug / asset_symbol / interval from event metadata
                    let event = market.event.as_ref();
                    let series_slug = event.and_then(|e| e.seriesSlug.clone());
                    let event_slug = event.map(|e| e.slug.clone());

                    // Try to map asset_symbol and interval from target_series known config
                    let (asset_symbol, interval) = series
                        .iter()
                        .find(|(_, slug, _, _)| series_slug.as_ref() == Some(slug))
                        .map(|(_, _, sym, int)| (Some(sym.clone()), Some(int.clone())))
                        .unwrap_or((None, None));

                    let outcomes_vec = parse_json_array_string(&market.outcomes);
                    let prices_vec = parse_json_number_array(&market.outcomePrices);
                    let token_ids = parse_json_array_string(&market.clobTokenIds);

                    let upsert = MarketUpsertData {
                        polymarket_market_id: market.id.clone(),
                        condition_id: market.conditionId.clone().unwrap_or_default(),
                        slug: market.slug.clone(),
                        question: market.question.clone(),
                        description: market.description.clone(),
                        event_slug,
                        series_slug: series_slug.clone(),
                        asset_symbol,
                        interval,
                        outcomes: outcomes_vec.clone(),
                        outcome_prices: prices_vec.clone(),
                        clob_token_ids: token_ids.clone(),
                        tick_size: parse_tick_size(&market.tick_size),
                        maker_base_fee: market.makerBaseFee,
                        taker_base_fee: market.takerBaseFee,
                        fees_enabled: market.feesEnabled,
                        fee_schedule: market.feeSchedule.clone(),
                        active: market.active.unwrap_or(false),
                        closed: market.closed.unwrap_or(false),
                        archived: market.archived.unwrap_or(false),
                        start_date: market.startDate,
                        end_date: market.endDate,
                        resolution_source: market.resolutionSource.clone(),
                        image: market.image.clone(),
                        icon: market.icon.clone(),
                        volume: market.volume,
                        liquidity: market.liquidity,
                        open_interest: market.openInterest,
                        created_at: market.createdAt,
                        updated_at: market.updatedAt,
                        resolved_at: None,
                        winning_asset_id: None,
                        winning_outcome: None,
                    };

                    match db.upsert_market(&upsert).await {
                        Ok(market_id) => {
                            if let Some(ref tokens) = token_ids {
                                let out = outcomes_vec.as_deref().unwrap_or(&[]);
                                let prc = prices_vec.as_deref().unwrap_or(&[]);
                                if let Err(e) = db.upsert_outcomes(market_id, tokens, out, prc).await {
                                    tracing::error!("Failed to upsert outcomes for {}: {}", market.slug, e);
                                } else {
                                    tracing::debug!("Upserted market {} (id={})", market.slug, market_id);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to upsert market {}: {}", market.slug, e);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to fetch markets from Gamma API: {}", e);
            }
        }
    }

    let _ = shutdown_tx.send(true);
    let _ = ws_handle.await;
    Ok(())
}
