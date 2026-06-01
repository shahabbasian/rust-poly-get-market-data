use crate::collector::batch_writer::Event;
use crate::collector::event_router;
use crate::collector::models::MarketRow;
use crate::collector::status;
use anyhow::Context;
use futures::StreamExt;
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BookUpdate, LastTradePrice, MarketResolved, PriceChange,
};
use polymarket_client_sdk_v2::clob::ws::Client as WsClient;
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{error, info, warn};

#[derive(Debug)]
enum WsEvent {
    Book(BookUpdate),
    Prices(PriceChange),
    Trade(LastTradePrice),
    Resolved(MarketResolved),
}

pub async fn run(
    market: MarketRow,
    ws_host: String,
    tx: mpsc::Sender<Event>,
    pool: sqlx::PgPool,
) -> anyhow::Result<()> {
    let yes_u = event_router::parse_u256(&market.token_id_yes).context("parse token_id_yes as U256")?;
    let no_u = event_router::parse_u256(&market.token_id_no).context("parse token_id_no as U256")?;
    let assets = vec![yes_u, no_u];

    let cfg = WsConfig::default();
    let client = WsClient::new(&ws_host, cfg).context("create ws client")?;

    let (evt_tx, mut evt_rx) = mpsc::channel::<WsEvent>(64);

    {
        let client2 = client.clone();
        let assets2 = assets.clone();
        let tx2 = evt_tx.clone();
        tokio::spawn(async move {
            #[allow(unused_mut)]
            let mut s = match client2.subscribe_orderbook(assets2) {
                Ok(s) => s,
                Err(e) => { error!(error = %e, "subscribe orderbook"); return; }
            };
            tokio::pin!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(b) => { if tx2.send(WsEvent::Book(b)).await.is_err() { break; } }
                    Err(e) => { error!(error = %e, "book stream err"); break; }
                }
            }
        });
    }
    {
        let client2 = client.clone();
        let assets2 = assets.clone();
        let tx2 = evt_tx.clone();
        tokio::spawn(async move {
            let mut s = match client2.subscribe_prices(assets2) {
                Ok(s) => s,
                Err(e) => { error!(error = %e, "subscribe prices"); return; }
            };
            tokio::pin!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(p) => { if tx2.send(WsEvent::Prices(p)).await.is_err() { break; } }
                    Err(e) => { error!(error = %e, "prices stream err"); break; }
                }
            }
        });
    }
    {
        let client2 = client.clone();
        let assets2 = assets.clone();
        let tx2 = evt_tx.clone();
        tokio::spawn(async move {
            let mut s = match client2.subscribe_last_trade_price(assets2) {
                Ok(s) => s,
                Err(e) => { error!(error = %e, "subscribe last_trade_price"); return; }
            };
            tokio::pin!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(t) => { if tx2.send(WsEvent::Trade(t)).await.is_err() { break; } }
                    Err(e) => { error!(error = %e, "trade stream err"); break; }
                }
            }
        });
    }
    {
        let client2 = client.clone();
        let assets2 = assets.clone();
        let tx2 = evt_tx.clone();
        tokio::spawn(async move {
            let mut s = match client2.subscribe_market_resolutions(assets2) {
                Ok(s) => s,
                Err(e) => { error!(error = %e, "subscribe market_resolutions"); return; }
            };
            tokio::pin!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(r) => { if tx2.send(WsEvent::Resolved(r)).await.is_err() { break; } }
                    Err(e) => { error!(error = %e, "resolution stream err"); break; }
                }
            }
        });
    }
    drop(evt_tx);

    let deadline = market
        .end_date
        .map(|e| e + chrono::Duration::seconds(30))
        .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::hours(24));

    info!(market_id = %market.id, symbol = %market.symbol, interval = %market.interval, "ws task running");

    let market_id = market.id;

    loop {
        let now = chrono::Utc::now();
        if now >= deadline {
            info!(market_id = %market_id, "ws task deadline reached");
            break;
        }
        let remaining = (deadline - now).to_std().unwrap_or(Duration::from_secs(1));
        let recv = evt_rx.recv();
        let event = match timeout(remaining, recv).await {
            Ok(Some(e)) => e,
            Ok(None) => {
                warn!(market_id = %market_id, "all ws sub-streams ended");
                break;
            }
            Err(_) => {
                info!(market_id = %market_id, "ws task timed out (deadline)");
                break;
            }
        };
        match event {
            WsEvent::Book(b) => {
                event_router::send_one(&tx, event_router::on_book(&market, &b));
            }
            WsEvent::Prices(p) => {
                event_router::send_all(&tx, event_router::on_price_change(&market, &p));
            }
            WsEvent::Trade(t) => {
                event_router::send_one(&tx, event_router::on_trade(&market, &t));
            }
            WsEvent::Resolved(r) => {
                if let Some(winning_outcome) = event_router::winning_side(&market, &r) {
                    if let Err(e) = status::mark_resolved(&pool, market_id, &winning_outcome).await {
                        error!(market_id = %market_id, error = %e, "mark_resolved failed");
                    } else {
                        info!(market_id = %market_id, winning_outcome, "market resolved");
                    }
                }
                drop(tx);
                break;
            }
        }
    }

    Ok(())
}
