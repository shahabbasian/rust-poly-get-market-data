use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketMarket {
    pub id: String,
    pub conditionId: Option<String>,
    pub slug: String,
    pub question: Option<String>,
    pub description: Option<String>,
    pub outcomes: Option<String>,        // JSON array string, e.g. "[\"Up\",\"Down\"]"
    pub outcomePrices: Option<String>,   // JSON array string
    pub clobTokenIds: Option<String>,   // JSON array string of token IDs
    pub tick_size: Option<f64>,
    pub makerBaseFee: Option<i32>,
    pub takerBaseFee: Option<i32>,
    pub feesEnabled: Option<bool>,
    pub feeSchedule: Option<serde_json::Value>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub archived: Option<bool>,
    pub startDate: Option<DateTime<Utc>>,
    pub endDate: Option<DateTime<Utc>>,
    pub resolutionSource: Option<String>,
    pub image: Option<String>,
    pub icon: Option<String>,
    pub volume: Option<f64>,
    pub liquidity: Option<f64>,
    pub openInterest: Option<f64>,
    pub createdAt: Option<DateTime<Utc>>,
    pub updatedAt: Option<DateTime<Utc>>,
    pub event: Option<PolymarketEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketEvent {
    pub id: String,
    pub slug: String,
    pub title: Option<String>,
    pub seriesSlug: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketUpsertData {
    pub polymarket_market_id: String,
    pub condition_id: String,
    pub slug: String,
    pub question: Option<String>,
    pub description: Option<String>,
    pub event_slug: Option<String>,
    pub series_slug: Option<String>,
    pub asset_symbol: Option<String>,
    pub interval: Option<String>,
    pub outcomes: Option<Vec<String>>,
    pub outcome_prices: Option<Vec<f64>>,
    pub clob_token_ids: Option<Vec<String>>,
    pub tick_size: Option<f64>,
    pub maker_base_fee: Option<i32>,
    pub taker_base_fee: Option<i32>,
    pub fees_enabled: Option<bool>,
    pub fee_schedule: Option<serde_json::Value>,
    pub active: bool,
    pub closed: bool,
    pub archived: bool,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub resolution_source: Option<String>,
    pub image: Option<String>,
    pub icon: Option<String>,
    pub volume: Option<f64>,
    pub liquidity: Option<f64>,
    pub open_interest: Option<f64>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub winning_asset_id: Option<String>,
    pub winning_outcome: Option<String>,
}

// WebSocket events from Polymarket Market Channel
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum WsMarketEvent {
    #[serde(rename = "new_market")]
    NewMarket {
        #[serde(rename = "id")]
        market_id: String,
        question: String,
        market: String, // condition id
        slug: String,
        description: Option<String>,
        #[serde(rename = "assets_ids")]
        assets_ids: Vec<String>,
        outcomes: Vec<String>,
        #[serde(rename = "event_message")]
        event_message: Option<serde_json::Value>,
        #[serde(rename = "clob_token_ids")]
        clob_token_ids: Option<Vec<String>>,
        #[serde(rename = "order_price_min_tick_size")]
        order_price_min_tick_size: Option<String>,
        #[serde(rename = "taker_base_fee")]
        taker_base_fee: Option<String>,
        #[serde(rename = "fees_enabled")]
        fees_enabled: Option<bool>,
        #[serde(rename = "fee_schedule")]
        fee_schedule: Option<serde_json::Value>,
        active: Option<bool>,
        #[serde(rename = "condition_id")]
        condition_id: Option<String>,
        timestamp: String,
    },
    #[serde(rename = "market_resolved")]
    MarketResolved {
        #[serde(rename = "id")]
        market_id: String,
        market: String,
        #[serde(rename = "assets_ids")]
        assets_ids: Vec<String>,
        #[serde(rename = "winning_asset_id")]
        winning_asset_id: String,
        winning_outcome: String,
        timestamp: String,
    },
    #[serde(rename = "book")]
    Book {
        #[serde(rename = "asset_id")]
        asset_id: String,
        market: String,
        bids: Vec<WsLevel>,
        asks: Vec<WsLevel>,
        timestamp: String,
    },
    #[serde(rename = "price_change")]
    PriceChange {
        market: String,
        #[serde(rename = "price_changes")]
        price_changes: Vec<PriceChangeEntry>,
        timestamp: String,
    },
    #[serde(rename = "tick_size_change")]
    TickSizeChange {
        #[serde(rename = "asset_id")]
        asset_id: String,
        market: String,
        #[serde(rename = "old_tick_size")]
        old_tick_size: String,
        #[serde(rename = "new_tick_size")]
        new_tick_size: String,
        timestamp: String,
    },
    #[serde(rename = "last_trade_price")]
    LastTradePrice {
        #[serde(rename = "asset_id")]
        asset_id: String,
        market: String,
        price: String,
        size: String,
        side: String,
        timestamp: String,
    },
    #[serde(rename = "best_bid_ask")]
    BestBidAsk {
        #[serde(rename = "asset_id")]
        asset_id: String,
        market: String,
        #[serde(rename = "best_bid")]
        best_bid: String,
        #[serde(rename = "best_ask")]
        best_ask: String,
        spread: String,
        timestamp: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChangeEntry {
    #[serde(rename = "asset_id")]
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: String,
    pub hash: String,
    #[serde(rename = "best_bid")]
    pub best_bid: Option<String>,
    #[serde(rename = "best_ask")]
    pub best_ask: Option<String>,
}
