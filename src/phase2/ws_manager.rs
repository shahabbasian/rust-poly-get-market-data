use std::time::Duration;

use futures::{SinkExt, StreamExt as _};
use sqlx::PgPool;
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, trace, warn};

use crate::config::Config;
use crate::phase2::event_handler::dispatch_ws_event;
use crate::phase2::lifecycle::get_watch_list;
use crate::phase2::orderbook_repo::spawn_event_writer;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

pub async fn run_ws(pool: PgPool, config: Config) -> anyhow::Result<()> {
    let writer = spawn_event_writer(pool.clone(), config.batch_insert_size, config.batch_flush_interval_ms);

    let mut last_asset_ids: Vec<String> = Vec::new();

    loop {
        let watch_list = match get_watch_list(&pool, config.watch_ahead_secs).await {
            Ok(list) => list,
            Err(e) => {
                error!(error = %e, "Failed to get watch list");
                tokio::time::sleep(Duration::from_secs(config.ws_reconnect_delay_secs)).await;
                continue;
            }
        };

        let mut asset_ids_str = Vec::new();
        let mut token_id_map = std::collections::HashMap::new(); // asset_id -> (yes, no)
        let mut condition_id_map = std::collections::HashMap::new(); // condition_id -> token_id_yes

        for market in &watch_list {
            if !market.token_id_yes.is_empty() {
                asset_ids_str.push(market.token_id_yes.clone());
                token_id_map.insert(
                    market.token_id_yes.clone(),
                    (market.token_id_yes.clone(), market.token_id_no.clone()),
                );
            }
            if !market.token_id_no.is_empty() {
                asset_ids_str.push(market.token_id_no.clone());
                token_id_map.insert(
                    market.token_id_no.clone(),
                    (market.token_id_yes.clone(), market.token_id_no.clone()),
                );
            }
            if !market.condition_id.is_empty() {
                condition_id_map.insert(market.condition_id.clone(), market.token_id_yes.clone());
            }
        }

        if asset_ids_str.is_empty() {
            info!("No markets to watch, sleeping 30s");
            tokio::time::sleep(Duration::from_secs(30)).await;
            continue;
        }

        let mut last_sorted = last_asset_ids.clone();
        let mut current_sorted = asset_ids_str.clone();
        last_sorted.sort();
        current_sorted.sort();

        let changed = last_sorted != current_sorted;
        if !changed && !last_asset_ids.is_empty() {
            tokio::time::sleep(Duration::from_secs(config.ws_reconnect_delay_secs)).await;
            continue;
        }

        last_asset_ids = asset_ids_str.clone();

        if let Err(e) = connect_and_stream(
            &pool,
            &asset_ids_str,
            &token_id_map,
            &condition_id_map,
            &writer,
            &config,
        )
        .await
        {
            error!(error = %e, "WS connection error");
        }

        warn!(
            delay = config.ws_reconnect_delay_secs,
            "Reconnecting WS"
        );
        tokio::time::sleep(Duration::from_secs(config.ws_reconnect_delay_secs)).await;
    }
}

async fn connect_and_stream(
    pool: &PgPool,
    asset_ids: &[String],
    token_id_map: &std::collections::HashMap<String, (String, String)>,
    condition_id_map: &std::collections::HashMap<String, String>,
    writer: &tokio::sync::mpsc::Sender<crate::phase2::orderbook_repo::OrderBookEvent>,
    _config: &Config,
) -> anyhow::Result<()> {
    let (ws_stream, response) = connect_async(WS_URL).await?;
    info!(status = ?response.status(), "WS connected");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe message
    let subscribe_msg = serde_json::json!({
        "assets_ids": asset_ids,
        "type": "market",
        "custom_feature_enabled": true,
    });
    write.send(Message::Text(subscribe_msg.to_string())).await?;
    info!(count = asset_ids.len(), "Sent subscription");

    // Heartbeat: PING every 10 seconds
    let mut heartbeat = interval(Duration::from_secs(10));

    // Also refresh watch list every 30s
    let mut refresh_timer = interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            biased;
            _ = heartbeat.tick() => {
                write.send(Message::Text(r#"{"type":"ping"}"#.to_string())).await?;
                trace!("Sent PING");
            }
            _ = refresh_timer.tick() => {
                // Break the loop to refresh asset list
                info!("Refreshing watch list");
                break;
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        trace!(len = text.len(), "WS text received");
                        match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(json) => {
                                // Skip simple pong replies
                                if json.get("type").and_then(|v| v.as_str()) == Some("pong") {
                                    trace!("Received PONG");
                                    continue;
                                }
                                dispatch_ws_event(pool, writer, json, token_id_map, condition_id_map).await;
                            }
                            Err(e) => {
                                warn!(error = %e, text = %text, "Failed to parse WS message");
                            }
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        trace!(len = bin.len(), "WS binary received");
                    }
                    Some(Ok(Message::Close(frame))) => {
                        warn!(frame = ?frame, "WS close received");
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        trace!("Received PONG");
                    }
                    Some(Ok(Message::Frame(_))) => {
                        trace!("Received FRAME");
                    }
                    Some(Err(e)) => {
                        error!(error = %e, "WS read error");
                        break;
                    }
                    None => {
                        info!("WS stream ended");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
