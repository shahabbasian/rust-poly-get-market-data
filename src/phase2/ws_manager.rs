use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use polymarket_client_sdk_v2::clob::ws::Client as WsClient;
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BestBidAsk, BookUpdate, LastTradePrice, MarketResolved, PriceChange, PriceChangeBatchEntry,
    TickSizeChange,
};
use polymarket_client_sdk_v2::types::U256;
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::models::MarketRecord;
use crate::phase2::event_handler;
use crate::phase2::orderbook_repo::{self, OrderBookEvent};

fn build_asset_to_yes_map(markets: &[MarketRecord]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(markets.len() * 2);
    for m in markets {
        if !m.token_id_yes.is_empty() {
            if !m.token_id_no.is_empty() {
                map.insert(m.token_id_no.clone(), m.token_id_yes.clone());
            }
            map.insert(m.token_id_yes.clone(), m.token_id_yes.clone());
        }
    }
    map
}

pub async fn run_ws(pool: PgPool, config: &crate::config::Config) -> anyhow::Result<()> {
    let sender = event_handler::start_event_handler(pool.clone(), config).await;

    loop {
        info!("Phase 2 WS: connecting...");
        match connect_and_stream(pool.clone(), config, sender.clone()).await {
            Ok(()) => {
                info!("Phase 2 WS: stream ended normally, reconnecting...");
            }
            Err(e) => {
                error!(error = %e, "Phase 2 WS: connection error, reconnecting in {}s", config.ws_reconnect_delay_secs);
                tokio::time::sleep(std::time::Duration::from_secs(config.ws_reconnect_delay_secs)).await;
            }
        }
    }
}

async fn connect_and_stream(
    pool: PgPool,
    config: &crate::config::Config,
    sender: mpsc::Sender<OrderBookEvent>,
) -> anyhow::Result<()> {
    let markets = orderbook_repo::get_markets_for_watch(&pool, config.watch_ahead_secs as i64).await?;

    let asset_ids: Vec<U256> = gather_asset_ids(&markets);
    let asset_to_yes = build_asset_to_yes_map(&markets);

    if asset_ids.is_empty() {
        warn!("No markets to watch, waiting for lifecycle to populate");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        return Ok(());
    }

    info!(count = asset_ids.len(), market_count = markets.len(), "Subscribing to WebSocket");

    let ws_client = WsClient::default();

    let mut bba_stream = Box::pin(ws_client.subscribe_best_bid_ask(asset_ids.clone())?);
    let mut market_res_stream = Box::pin(ws_client.subscribe_market_resolutions(asset_ids.clone())?);
    let mut book_stream = Box::pin(ws_client.subscribe_orderbook(asset_ids.clone())?);
    let mut price_stream = Box::pin(ws_client.subscribe_prices(asset_ids.clone())?);
    let mut last_trade_stream = Box::pin(ws_client.subscribe_last_trade_price(asset_ids.clone())?);
    let mut tick_size_stream = Box::pin(ws_client.subscribe_tick_size_change(asset_ids.clone())?);

    let refresh_pool = pool.clone();
    let refresh_config = config.clone();

    let mut refresh_interval = tokio::time::interval(std::time::Duration::from_secs(30));

    loop {
        tokio::select! {
            result = book_stream.next() => {
                match result {
                    Some(Ok(book)) => {
                        let payload = book_to_json(&book);
                        let ts = millis_to_dt(book.timestamp);
                        let token_id_yes = resolve_yes(&asset_to_yes, &book.asset_id.to_string());
                        dispatch_event(&sender, &pool, "book",
                            &book.market.to_string(), &token_id_yes, payload, ts).await;
                    }
                    Some(Err(e)) => error!(error = %e, "Book stream error"),
                    None => { info!("Book stream ended"); return Ok(()); }
                }
            }

            result = price_stream.next() => {
                match result {
                    Some(Ok(price)) => {
                        for entry in &price.price_changes {
                            let payload = price_change_to_json(&price, entry);
                            let ts = millis_to_dt(price.timestamp);
                            let token_id_yes = resolve_yes(&asset_to_yes, &entry.asset_id.to_string());
                            dispatch_event(&sender, &pool, "price_change",
                                &price.market.to_string(), &token_id_yes, payload, ts).await;
                        }
                    }
                    Some(Err(e)) => error!(error = %e, "Price stream error"),
                    None => { info!("Price stream ended"); return Ok(()); }
                }
            }

            result = last_trade_stream.next() => {
                match result {
                    Some(Ok(ltp)) => {
                        let payload = last_trade_to_json(&ltp);
                        let ts = millis_to_dt(ltp.timestamp);
                        let token_id_yes = resolve_yes(&asset_to_yes, &ltp.asset_id.to_string());
                        dispatch_event(&sender, &pool, "last_trade_price",
                            &ltp.market.to_string(), &token_id_yes, payload, ts).await;
                    }
                    Some(Err(e)) => error!(error = %e, "Last trade stream error"),
                    None => { info!("Last trade stream ended"); return Ok(()); }
                }
            }

            result = tick_size_stream.next() => {
                match result {
                    Some(Ok(tsc)) => {
                        let payload = tick_size_to_json(&tsc);
                        let ts = millis_to_dt(tsc.timestamp);
                        let token_id_yes = resolve_yes(&asset_to_yes, &tsc.asset_id.to_string());
                        dispatch_event(&sender, &pool, "tick_size_change",
                            &tsc.market.to_string(), &token_id_yes, payload, ts).await;
                    }
                    Some(Err(e)) => error!(error = %e, "Tick size stream error"),
                    None => { info!("Tick size stream ended"); return Ok(()); }
                }
            }

            result = bba_stream.next() => {
                match result {
                    Some(Ok(bba)) => {
                        let payload = bba_to_json(&bba);
                        let ts = millis_to_dt(bba.timestamp);
                        let token_id_yes = resolve_yes(&asset_to_yes, &bba.asset_id.to_string());
                        dispatch_event(&sender, &pool, "best_bid_ask",
                            &bba.market.to_string(), &token_id_yes, payload, ts).await;
                    }
                    Some(Err(e)) => error!(error = %e, "BestBidAsk stream error"),
                    None => { info!("BestBidAsk stream ended"); return Ok(()); }
                }
            }

            result = market_res_stream.next() => {
                match result {
                    Some(Ok(mr)) => {
                        let payload = market_resolved_to_json(&mr);
                        let ts = millis_to_dt(mr.timestamp);
                        let asset_id = mr.asset_ids.first().map(|a| a.to_string()).unwrap_or_default();
                        let token_id_yes = resolve_yes(&asset_to_yes, &asset_id);
                        dispatch_event(&sender, &pool, "market_resolved",
                            &mr.market.to_string(), &token_id_yes, payload, ts).await;
                    }
                    Some(Err(e)) => error!(error = %e, "MarketResolved stream error"),
                    None => { info!("MarketResolved stream ended"); return Ok(()); }
                }
            }

            _ = refresh_interval.tick() => {
                let Ok(updated_markets) = orderbook_repo::get_markets_for_watch(
                    &refresh_pool, refresh_config.watch_ahead_secs as i64
                ).await else {
                    warn!("Failed to refresh watch list");
                    continue;
                };

                let new_ids: Vec<U256> = gather_asset_ids(&updated_markets)
                    .into_iter()
                    .filter(|id| !asset_ids.contains(id))
                    .collect();

                if !new_ids.is_empty() {
                    info!(new_count = new_ids.len(), "New markets detected, restarting WS connection");
                    return Ok(());
                }
            }
        }
    }
}

fn resolve_yes(map: &HashMap<String, String>, asset_id: &str) -> String {
    map.get(asset_id).cloned().unwrap_or_else(|| asset_id.to_string())
}

async fn dispatch_event(
    sender: &mpsc::Sender<OrderBookEvent>,
    pool: &PgPool,
    event_type: &str,
    market: &str,
    asset_id: &str,
    payload: Value,
    ts: Option<DateTime<Utc>>,
) {
    event_handler::dispatch(sender, event_type, market, asset_id, payload, ts, pool).await;
}

fn gather_asset_ids(markets: &[MarketRecord]) -> Vec<U256> {
    markets
        .iter()
        .flat_map(|m| {
            let mut ids = Vec::new();
            if !m.token_id_yes.is_empty() {
                if let Ok(id) = U256::from_str(&m.token_id_yes) {
                    ids.push(id);
                }
            }
            if !m.token_id_no.is_empty() {
                if let Ok(id) = U256::from_str(&m.token_id_no) {
                    ids.push(id);
                }
            }
            ids
        })
        .collect()
}

fn millis_to_dt(ts: i64) -> Option<DateTime<Utc>> {
    chrono::DateTime::from_timestamp_millis(ts)
}

fn book_to_json(book: &BookUpdate) -> Value {
    json!({
        "event_type": "book",
        "asset_id": book.asset_id.to_string(),
        "market": book.market.to_string(),
        "timestamp": book.timestamp.to_string(),
        "bids": book.bids.iter().map(|l| json!({"price": l.price.to_string(), "size": l.size.to_string()})).collect::<Vec<_>>(),
        "asks": book.asks.iter().map(|l| json!({"price": l.price.to_string(), "size": l.size.to_string()})).collect::<Vec<_>>(),
        "hash": book.hash,
    })
}

fn price_change_to_json(price: &PriceChange, entry: &PriceChangeBatchEntry) -> Value {
    json!({
        "event_type": "price_change",
        "market": price.market.to_string(),
        "timestamp": price.timestamp.to_string(),
        "asset_id": entry.asset_id.to_string(),
        "price": entry.price.to_string(),
        "size": entry.size.as_ref().map(|s| s.to_string()),
        "side": format!("{:?}", entry.side),
        "hash": entry.hash,
        "best_bid": entry.best_bid.as_ref().map(|b| b.to_string()),
        "best_ask": entry.best_ask.as_ref().map(|a| a.to_string()),
    })
}

fn last_trade_to_json(ltp: &LastTradePrice) -> Value {
    json!({
        "event_type": "last_trade_price",
        "asset_id": ltp.asset_id.to_string(),
        "market": ltp.market.to_string(),
        "price": ltp.price.to_string(),
        "side": ltp.side.as_ref().map(|s| format!("{:?}", s)),
        "size": ltp.size.as_ref().map(|s| s.to_string()),
        "fee_rate_bps": ltp.fee_rate_bps.as_ref().map(|f| f.to_string()),
        "timestamp": ltp.timestamp.to_string(),
    })
}

fn tick_size_to_json(tsc: &TickSizeChange) -> Value {
    json!({
        "event_type": "tick_size_change",
        "asset_id": tsc.asset_id.to_string(),
        "market": tsc.market.to_string(),
        "old_tick_size": tsc.old_tick_size.to_string(),
        "new_tick_size": tsc.new_tick_size.to_string(),
        "timestamp": tsc.timestamp.to_string(),
    })
}

fn bba_to_json(bba: &BestBidAsk) -> Value {
    json!({
        "event_type": "best_bid_ask",
        "market": bba.market.to_string(),
        "asset_id": bba.asset_id.to_string(),
        "best_bid": bba.best_bid.to_string(),
        "best_ask": bba.best_ask.to_string(),
        "spread": bba.spread.to_string(),
        "timestamp": bba.timestamp.to_string(),
    })
}

fn market_resolved_to_json(mr: &MarketResolved) -> Value {
    json!({
        "event_type": "market_resolved",
        "id": mr.id,
        "question": mr.question,
        "market": mr.market.to_string(),
        "slug": mr.slug,
        "description": mr.description,
        "assets_ids": mr.asset_ids.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        "outcomes": mr.outcomes,
        "winning_asset_id": mr.winning_asset_id.to_string(),
        "winning_outcome": mr.winning_outcome,
        "timestamp": mr.timestamp.to_string(),
    })
}
