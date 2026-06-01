use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow)]
pub struct MarketRow {
    pub id: Uuid,
    pub symbol: String,
    pub interval: String,
    pub condition_id: Option<String>,
    pub token_id_yes: String,
    pub token_id_no: String,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SnapshotRow {
    pub market_id: Uuid,
    pub asset_id: String,
    pub side: String,
    pub bids: Value,
    pub asks: Value,
    pub hash: Option<String>,
    pub ts_exchange: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct DeltaRow {
    pub market_id: Uuid,
    pub asset_id: String,
    pub side: String,
    pub price: f64,
    pub new_size: f64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub hash: Option<String>,
    pub ts_exchange: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct TradeRow {
    pub market_id: Uuid,
    pub asset_id: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub fee_rate_bps: Option<i32>,
    pub ts_exchange: Option<DateTime<Utc>>,
}

pub fn decimal_to_f64(d: &Decimal) -> Option<f64> {
    d.to_string().parse::<f64>().ok()
}

pub fn json_array_of_levels(levels: &[WsLevel]) -> Value {
    Value::Array(
        levels
            .iter()
            .map(|l| {
                serde_json::json!({
                    "price": l.price.to_string(),
                    "size":  l.size.to_string(),
                })
            })
            .collect(),
    )
}

pub struct WsLevel {
    pub price: Decimal,
    pub size: Decimal,
}
