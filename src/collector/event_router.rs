use crate::collector::batch_writer::Event;
use crate::collector::models::{
    decimal_to_f64, json_array_of_levels, DeltaRow, MarketRow, SnapshotRow, TradeRow, WsLevel,
};
use chrono::{DateTime, TimeZone, Utc};
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BookUpdate, LastTradePrice, MarketResolved, OrderBookLevel, PriceChange,
};
use polymarket_client_sdk_v2::types::U256;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::warn;

pub fn side_for(asset_id: &str, market: &MarketRow) -> &'static str {
    if asset_id == market.token_id_yes {
        "yes"
    } else if asset_id == market.token_id_no {
        "no"
    } else {
        "yes"
    }
}

fn asset_id_to_string(id: U256) -> String {
    id.to_string()
}

fn ts_ms_to_dt(ms: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms).single()
}

fn levels_to_value(levels: &[OrderBookLevel]) -> serde_json::Value {
    let mapped: Vec<WsLevel> = levels
        .iter()
        .map(|l| WsLevel {
            price: l.price,
            size: l.size,
        })
        .collect();
    json_array_of_levels(&mapped)
}

pub fn on_book(market: &MarketRow, book: &BookUpdate) -> Event {
    let asset_id = asset_id_to_string(book.asset_id);
    let side = side_for(&asset_id, market);
    Event::Snapshot(SnapshotRow {
        market_id: market.id,
        asset_id,
        side: side.to_owned(),
        bids: levels_to_value(&book.bids),
        asks: levels_to_value(&book.asks),
        hash: book.hash.clone(),
        ts_exchange: ts_ms_to_dt(book.timestamp),
    })
}

pub fn on_price_change(market: &MarketRow, pc: &PriceChange) -> Vec<Event> {
    pc.price_changes
        .iter()
        .map(|entry| {
            let asset_id = asset_id_to_string(entry.asset_id);
            let side = side_for(&asset_id, market);
            let new_size = entry.size.as_ref().and_then(decimal_to_f64).unwrap_or(0.0);
            let price = decimal_to_f64(&entry.price).unwrap_or(0.0);
            Event::Delta(DeltaRow {
                market_id: market.id,
                asset_id,
                side: side.to_owned(),
                price,
                new_size,
                best_bid: entry.best_bid.as_ref().and_then(decimal_to_f64),
                best_ask: entry.best_ask.as_ref().and_then(decimal_to_f64),
                hash: entry.hash.clone(),
                ts_exchange: ts_ms_to_dt(pc.timestamp),
            })
        })
        .collect()
}

pub fn on_trade(market: &MarketRow, tr: &LastTradePrice) -> Event {
    let asset_id = asset_id_to_string(tr.asset_id);
    let side_str = match tr.side.as_ref().map(|s| format!("{s:?}").to_lowercase()) {
        Some(v) if v.contains("buy") => "buy",
        Some(v) if v.contains("sell") => "sell",
        _ => "unknown",
    };
    Event::Trade(TradeRow {
        market_id: market.id,
        asset_id,
        side: side_str.to_owned(),
        price: decimal_to_f64(&tr.price).unwrap_or(0.0),
        size: tr.size.as_ref().and_then(decimal_to_f64).unwrap_or(0.0),
        fee_rate_bps: tr
            .fee_rate_bps
            .as_ref()
            .and_then(decimal_to_f64)
            .map(|v| v as i32),
        ts_exchange: ts_ms_to_dt(tr.timestamp),
    })
}

/// Look up which side the winning asset represents. Returns Some("Yes"/"No") on match.
pub fn winning_side(market: &MarketRow, mr: &MarketResolved) -> Option<String> {
    let winner = mr.winning_asset_id.to_string();
    if winner == market.token_id_yes {
        Some("Yes".to_owned())
    } else if winner == market.token_id_no {
        Some("No".to_owned())
    } else {
        warn!(
            market_id = %market.id,
            winning_asset_id = %winner,
            yes = %market.token_id_yes,
            no = %market.token_id_no,
            "winning asset did not match either token id"
        );
        None
    }
}

pub fn parse_u256(s: &str) -> anyhow::Result<U256> {
    Ok(U256::from_str(s)?)
}

pub fn send_all(tx: &mpsc::Sender<Event>, events: Vec<Event>) {
    for e in events {
        if let Err(err) = tx.try_send(e) {
            warn!(error = %err, "batch writer channel full or closed");
        }
    }
}

pub fn send_one(tx: &mpsc::Sender<Event>, ev: Event) {
    if let Err(err) = tx.try_send(ev) {
        warn!(error = %err, "batch writer channel full or closed");
    }
}
