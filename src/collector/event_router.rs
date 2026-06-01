use crate::collector::batch_writer::Event;
use crate::collector::models::{
    decimal_to_f64, json_array_of_levels, DeltaRow, MarketRow, SnapshotRow, TradeRow, WsLevel,
};
use chrono::{DateTime, TimeZone, Utc};
use polymarket_client_sdk_v2::clob::types::Side as TradeSide;
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BookUpdate, LastTradePrice, MarketResolved, OrderBookLevel, PriceChange,
};
use polymarket_client_sdk_v2::types::U256;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::warn;

/// Map a WS event's asset_id to the market's yes/no side.
///
/// This should be infallible at runtime: we only subscribe to the two token IDs
/// we own (`token_id_yes` and `token_id_no`), so an unknown asset_id means a
/// programming error or a corrupted event from the WS. We log + return an
/// `Option` so the caller can decide to drop the event rather than silently
/// mislabel it.
pub fn side_for(asset_id: &str, market: &MarketRow) -> Option<&'static str> {
    if asset_id == market.token_id_yes {
        Some("yes")
    } else if asset_id == market.token_id_no {
        Some("no")
    } else {
        None
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

pub fn on_book(market: &MarketRow, book: &BookUpdate) -> Option<Event> {
    let asset_id = asset_id_to_string(book.asset_id);
    let side = side_for(&asset_id, market)?;
    Some(Event::Snapshot(SnapshotRow {
        market_id: market.id,
        asset_id,
        side: side.to_owned(),
        bids: levels_to_value(&book.bids),
        asks: levels_to_value(&book.asks),
        hash: book.hash.clone(),
        ts_exchange: ts_ms_to_dt(book.timestamp),
    }))
}

pub fn on_price_change(market: &MarketRow, pc: &PriceChange) -> Vec<Event> {
    let mut out = Vec::with_capacity(pc.price_changes.len());
    for entry in &pc.price_changes {
        let asset_id = asset_id_to_string(entry.asset_id);
        let Some(side) = side_for(&asset_id, market) else {
            warn!(
                market_id = %market.id,
                asset_id = %asset_id,
                "dropping price_change: asset_id matches neither token_yes nor token_no"
            );
            continue;
        };
        let new_size = entry.size.as_ref().and_then(decimal_to_f64).unwrap_or(0.0);
        let price = decimal_to_f64(&entry.price).unwrap_or(0.0);
        out.push(Event::Delta(DeltaRow {
            market_id: market.id,
            asset_id,
            side: side.to_owned(),
            price,
            new_size,
            best_bid: entry.best_bid.as_ref().and_then(decimal_to_f64),
            best_ask: entry.best_ask.as_ref().and_then(decimal_to_f64),
            hash: entry.hash.clone(),
            ts_exchange: ts_ms_to_dt(pc.timestamp),
        }));
    }
    out
}

pub fn on_trade(market: &MarketRow, tr: &LastTradePrice) -> Event {
    let asset_id = asset_id_to_string(tr.asset_id);
    let side_str = match tr.side {
        Some(TradeSide::Buy) => "buy",
        Some(TradeSide::Sell) => "sell",
        Some(TradeSide::Unknown) | None => "unknown",
        // `Side` is `#[non_exhaustive]`; treat any future variant as unknown.
        Some(_) => "unknown",
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
