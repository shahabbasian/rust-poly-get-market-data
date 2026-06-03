use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use crate::phase2::orderbook_repo::{
    set_market_resolved, update_last_book_hash, update_last_trade, update_market_bba, OrderBookEvent,
};

use sqlx::PgPool;

/// Handles a single raw WebSocket event JSON.
/// `token_id_map` maps asset_id -> (token_id_yes, token_id_no)
pub async fn dispatch_ws_event(
    pool: &PgPool,
    sender: &mpsc::Sender<OrderBookEvent>,
    event_json: serde_json::Value,
    token_id_map: &std::collections::HashMap<String, (String, String)>,
    condition_id_map: &std::collections::HashMap<String, String>,
) {
    let event_type = event_json
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    trace!(event = %event_json, "WS event received");

    // Determine which side (Yes/No) this event is for
    let asset_id = event_json
        .get("asset_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let token_id_yes = if let Some(ref aid) = asset_id {
        token_id_map.get(aid).map(|(yes, _no)| yes.clone())
    } else {
        None
    };

    // Some events (e.g. new_market, market_resolved) may not have asset_id but have condition_id / market field
    let token_id_yes_fallback = if token_id_yes.is_none() {
        event_json
            .get("market")
            .and_then(|v| v.as_str())
            .and_then(|m| condition_id_map.get(m).cloned())
    } else {
        None
    };

    let token_id_yes = token_id_yes.or(token_id_yes_fallback);

    // Build ws_timestamp from event timestamp if present
    let ws_timestamp = event_json
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let ob_event = OrderBookEvent {
        token_id_yes: token_id_yes.clone().unwrap_or_default(),
        event_type: event_type.clone(),
        payload: event_json.clone(),
        ws_timestamp,
    };

    // Send to batch writer regardless
    if let Err(e) = sender.send(ob_event).await {
        error!(error = %e, "Failed to send event to writer channel");
    }

    // Handle side effects
    match event_type.as_str() {
        "book" => {
            if let Some(ref yes) = token_id_yes {
                if let Some(hash) = event_json.get("hash").and_then(|v| v.as_str()) {
                    if let Err(e) = update_last_book_hash(pool, yes, hash).await {
                        warn!(error = %e, "Failed to update last_book_hash");
                    }
                }
            }
        }
        "best_bid_ask" => {
            if let Some(ref yes) = token_id_yes {
                let best_bid = event_json.get("best_bid").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let best_ask = event_json.get("best_ask").and_then(|v| v.as_f64()).unwrap_or(0.0);
                if let Err(e) = update_market_bba(pool, yes, best_bid, best_ask).await {
                    warn!(error = %e, "Failed to update best_bid/ask");
                }
            }
        }
        "last_trade_price" => {
            if let Some(ref yes) = token_id_yes {
                if let Some(price) = event_json.get("price").and_then(|v| v.as_f64()) {
                    if let Err(e) = update_last_trade(pool, yes, price).await {
                        warn!(error = %e, "Failed to update last_trade_price");
                    }
                }
            }
        }
        "market_resolved" => {
            if let Some(ref yes) = token_id_yes {
                let winning_outcome = event_json
                    .get("winning_outcome")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Yes");
                info!(token_id_yes = %yes, outcome = %winning_outcome, "Market resolved via WS");
                if let Err(e) = set_market_resolved(pool, yes, winning_outcome).await {
                    warn!(error = %e, "Failed to set market resolved");
                }
            }
        }
        "new_market" => {
            debug!("new_market event received");
        }
        "tick_size_change" => {
            debug!("tick_size_change event received");
        }
        "price_change" => {
            // Already stored; no extra side effect for now
        }
        _ => {
            trace!(event_type = %event_type, "Unhandled event type");
        }
    }
}

/// Insert a fallback REST snapshot event when a gap is detected.
pub async fn insert_fallback_book_event(
    sender: &mpsc::Sender<OrderBookEvent>,
    token_id_yes: &str,
    snapshot_json: serde_json::Value,
) {
    let ob_event = OrderBookEvent {
        token_id_yes: token_id_yes.to_string(),
        event_type: "book_rest_fallback".to_string(),
        payload: snapshot_json,
        ws_timestamp: Some(chrono::Utc::now()),
    };

    if let Err(e) = sender.send(ob_event).await {
        error!(error = %e, "Failed to send fallback event to writer channel");
    }
}
