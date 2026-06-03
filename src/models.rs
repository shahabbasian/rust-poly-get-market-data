use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct MarketRecord {
    pub id: Uuid,
    pub symbol: String,
    pub interval: String,
    pub condition_id: String,
    pub token_id_yes: String,
    pub token_id_no: String,
    pub question: Option<String>,
    pub slug: String,
    pub outcomes: Option<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub gamma_market_id: Option<String>,
    pub enable_order_book: Option<bool>,
    pub accepting_orders: Option<bool>,
    pub ready: Option<bool>,
    pub funded: Option<bool>,
    pub order_min_size: Option<f64>,
    pub order_price_min_tick_size: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub last_trade_price: Option<f64>,
    pub volume_clob: Option<f64>,
    pub volume_num: Option<f64>,
    pub status: Option<String>,
    pub winning_outcome: Option<String>,
    pub price_to_beat: Option<f64>,
    pub last_book_hash: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClobTokenIds {
    pub yes: Option<String>,
    pub no: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GammaMarketResponse {
    #[serde(default)]
    pub id: Option<String>,
    pub condition_id: Option<String>,
    pub slug: Option<String>,
    pub question: Option<String>,
    pub outcomes: Option<String>,
    #[serde(rename = "outcomePrices")]
    pub outcome_prices: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    #[serde(rename = "startDate")]
    pub start_date: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    #[serde(rename = "enableOrderBook")]
    pub enable_order_book: Option<bool>,
    #[serde(rename = "acceptingOrders")]
    pub accepting_orders: Option<bool>,
    pub ready: Option<bool>,
    pub funded: Option<bool>,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<String>,
    #[serde(rename = "orderMinSize")]
    pub order_min_size: Option<f64>,
    #[serde(rename = "orderPriceMinTickSize")]
    pub order_price_min_tick_size: Option<f64>,
    #[serde(rename = "bestBid")]
    pub best_bid: Option<f64>,
    #[serde(rename = "bestAsk")]
    pub best_ask: Option<f64>,
    #[serde(rename = "lastTradePrice")]
    pub last_trade_price: Option<f64>,
    #[serde(rename = "volumeClob")]
    pub volume_clob: Option<f64>,
    #[serde(rename = "volumeNum")]
    pub volume_num: Option<f64>,
}

pub const ASSET_MAP: &[(&str, &str)] = &[
    ("btc", "bitcoin"),
    ("eth", "ethereum"),
    ("sol", "solana"),
    ("xrp", "xrp"),
    ("doge", "dogecoin"),
    ("hype", "hype"),
    ("bnb", "bnb"),
];

pub const INTERVAL_CONFIG: &[(&str, u32)] = &[
    ("5m", 5 * 60),
    ("15m", 15 * 60),
    ("1h", 60 * 60),
    ("4h", 4 * 60 * 60),
];
