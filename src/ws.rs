use crate::models::WsMarketEvent;
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use url::Url;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

pub struct WsClient;

impl WsClient {
    pub async fn run(
        db: crate::db::Db,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        reconnect_secs: u64,
    ) -> Result<()> {
        let mut first_run = true;
        loop {
            if shutdown.has_changed().unwrap_or(true) && *shutdown.borrow() {
                tracing::info!("WS shutdown requested");
                break;
            }

            if !first_run {
                tracing::info!("Reconnecting WS in {}s...", reconnect_secs);
                sleep(Duration::from_secs(reconnect_secs)).await;
            }
            first_run = false;

            match Self::connect_and_listen(db.clone(), &mut shutdown).await {
                Ok(()) => {
                    tracing::info!("WS listener exited cleanly");
                }
                Err(e) => {
                    tracing::error!("WS listener error: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn connect_and_listen(
        db: crate::db::Db,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let url = Url::parse(WS_URL).context("Invalid WS URL")?;
        let (ws_stream, _) = connect_async(url.as_str()).await.context("Failed to connect WS")?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe with empty assets_ids but custom_feature_enabled to receive new_market and market_resolved
        let sub = serde_json::json!({
            "assets_ids": [],
            "type": "market",
            "custom_feature_enabled": true,
        });
        write.send(Message::Text(sub.to_string())).await.context("Failed to send subscription")?;
        tracing::info!("WS subscribed with custom_feature_enabled");

        let mut ping_interval = interval(Duration::from_secs(10));
        let mut ping_interval_tick = std::pin::pin!(ping_interval.tick());

        loop {
            tokio::select! {
                _ = ping_interval_tick.as_mut() => {
                    if let Err(e) = write.send(Message::Text("PING".to_string())).await {
                        tracing::error!("WS ping failed: {}", e);
                        break;
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if text == "PONG" {
                                tracing::debug!("WS PONG received");
                                continue;
                            }
                            if let Err(e) = Self::handle_message(&db, &text).await {
                                tracing::error!("WS message handling error: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            tracing::warn!("WS closed: {:?}", frame);
                            break;
                        }
                        Some(Ok(_)) => {
                            // Ignore other message types
                        }
                        Some(Err(e)) => {
                            tracing::error!("WS read error: {}", e);
                            break;
                        }
                        None => {
                            tracing::warn!("WS stream ended");
                            break;
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("WS received shutdown");
                        break;
                    }
                }
            }
        }

        let _ = write.close().await;
        Ok(())
    }

    async fn handle_message(db: &crate::db::Db, text: &str) -> Result<()> {
        let event: WsMarketEvent = serde_json::from_str(text)
            .with_context(|| format!("Failed to parse WS event: {}", text))?;

        match event {
            WsMarketEvent::NewMarket { .. } => {
                tracing::info!("WS new_market event received, upserting market");
                // TODO: Parse and upsert into DB
                // Convert to MarketUpsertData and call db.upsert_market / db.upsert_outcomes
            }
            WsMarketEvent::MarketResolved {
                market_id,
                winning_asset_id,
                winning_outcome,
                ..
            } => {
                tracing::info!(
                    "WS market_resolved event received: market_id={} winner={}",
                    market_id, winning_asset_id
                );
                if let Err(e) = db.resolve_market(&market_id, &winning_asset_id, &winning_outcome).await {
                    tracing::error!("Failed to resolve market {}: {}", market_id, e);
                } else {
                    let _ = db.insert_discovery_log(None, "ws_market_resolved", "websocket", Some(serde_json::Value::String(text.to_string()))).await;
                }
            }
            WsMarketEvent::Book { asset_id, bids, asks, timestamp: _, .. } => {
                tracing::debug!("WS book for asset_id={} bids={} asks={}", asset_id, bids.len(), asks.len());
            }
            WsMarketEvent::PriceChange { market, price_changes, timestamp: _ } => {
                tracing::debug!("WS price_change market={} changes={}", market, price_changes.len());
            }
            WsMarketEvent::TickSizeChange { asset_id, old_tick_size, new_tick_size, .. } => {
                tracing::debug!("WS tick_size_change asset={} {} -> {}", asset_id, old_tick_size, new_tick_size);
            }
            WsMarketEvent::LastTradePrice { asset_id, price, size, side, .. } => {
                tracing::debug!("WS last_trade asset_id={} price={} size={} side={}", asset_id, price, size, side);
            }
            WsMarketEvent::BestBidAsk { asset_id, best_bid, best_ask, spread, .. } => {
                tracing::debug!("WS best_bid_ask asset_id={} bid={} ask={} spread={}", asset_id, best_bid, best_ask, spread);
            }
        }

        Ok(())
    }
}
