use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use polymarket_client_sdk_v2::rtds::Client as RtdsClient;
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct PriceTick {
    pub symbol: String,
    pub ts_ms: i64,
    pub value: f64,
    pub source: TickSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickSource {
    Chainlink,
    Binance,
}

pub type TickBuffers = Arc<parking_lot::Mutex<BTreeMap<String, Vec<PriceTick>>>>;

pub fn make_buffers() -> TickBuffers {
    Arc::new(parking_lot::Mutex::new(BTreeMap::new()))
}

pub fn push_tick(buf: &TickBuffers, tick: PriceTick, max: usize) {
    let mut g = buf.lock();
    let entry = g.entry(tick.symbol.clone()).or_default();
    entry.push(tick);
    if entry.len() > max {
        let drop_n = entry.len() - max;
        entry.drain(0..drop_n);
    }
}

pub struct RtdsSymbols {
    pub binance: Option<String>,
    pub chainlink: Option<String>,
    pub fallback_slash: String,
}

pub fn rtds_symbols(symbol: &str) -> Option<RtdsSymbols> {
    let s = symbol.to_lowercase();
    let binance = match s.as_str() {
        "btc" => Some("btcusdt".to_owned()),
        "eth" => Some("ethusdt".to_owned()),
        "sol" => Some("solusdt".to_owned()),
        "xrp" => Some("xrpusdt".to_owned()),
        _ => None,
    };
    let chainlink = match s.as_str() {
        "btc" => Some("btc/usd".to_owned()),
        "eth" => Some("eth/usd".to_owned()),
        "sol" => Some("sol/usd".to_owned()),
        "xrp" => Some("xrp/usd".to_owned()),
        _ => None,
    };
    let fallback_slash = format!("{s}/usd");
    Some(RtdsSymbols { binance, chainlink, fallback_slash })
}

pub async fn run_rtds(
    host: &str,
    symbols: &[String],
    buffers: TickBuffers,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let cfg = WsConfig::default();
    let client = RtdsClient::new(host, cfg).context("create rtds client")?;

    let binance_syms: Vec<String> = symbols
        .iter()
        .filter_map(|s| rtds_symbols(s).and_then(|m| m.binance))
        .collect();
    let chainlink_syms: Vec<String> = symbols
        .iter()
        .filter_map(|s| rtds_symbols(s).and_then(|m| m.chainlink))
        .collect();

    let (tx, mut rx) = mpsc::channel::<PriceTick>(1024);

    if !binance_syms.is_empty() {
        let client2 = client.clone();
        let syms = binance_syms.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let mut s = match client2.subscribe_crypto_prices(Some(syms)) {
                Ok(s) => s,
                Err(e) => { error!(error = %e, "subscribe binance"); return; }
            };
            tokio::pin!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(c) => {
                        let tick = PriceTick {
                            symbol: c.symbol,
                            ts_ms: c.timestamp,
                            value: decimal_to_f64(&c.value).unwrap_or(0.0),
                            source: TickSource::Binance,
                        };
                        if tx2.send(tick).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "binance stream err");
                        break;
                    }
                }
            }
        });
    }

    for s_sym in chainlink_syms.iter() {
        let client2 = client.clone();
        let sym = s_sym.clone();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            let mut s = match client2.subscribe_chainlink_prices(Some(sym)) {
                Ok(s) => s,
                Err(e) => { error!(error = %e, "subscribe chainlink"); return; }
            };
            tokio::pin!(s);
            while let Some(item) = s.next().await {
                match item {
                    Ok(c) => {
                        let tick = PriceTick {
                            symbol: c.symbol,
                            ts_ms: c.timestamp,
                            value: decimal_to_f64(&c.value).unwrap_or(0.0),
                            source: TickSource::Chainlink,
                        };
                        if tx2.send(tick).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "chainlink stream err");
                        break;
                    }
                }
            }
        });
    }

    drop(tx);
    info!(binance = !binance_syms.is_empty(), chainlink = chainlink_syms.len(), "rtds loop running");

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("rtds: shutdown");
                    break;
                }
            }
            msg = rx.recv() => {
                match msg {
                    Some(tick) => {
                        debug!(symbol = %tick.symbol, value = tick.value, "tick");
                        push_tick(&buffers, tick, 4096);
                    }
                    None => {
                        warn!("all rtds streams ended");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn nearest_tick(
    buffers: &TickBuffers,
    slash_symbol: &str,
    start: DateTime<Utc>,
    window_secs: i64,
    preferred: TickSource,
) -> Option<PriceTick> {
    let g = buffers.lock();
    let Some(ticks) = g.get(slash_symbol) else {
        return None;
    };
    let start_ms = start.timestamp_millis();
    let lo = start_ms - window_secs * 1000;
    let hi = start_ms + window_secs * 1000;
    let preferred_tick = ticks
        .iter()
        .filter(|t| t.source == preferred && t.ts_ms >= lo && t.ts_ms <= hi)
        .min_by_key(|t| (t.ts_ms - start_ms).abs());
    if let Some(t) = preferred_tick {
        return Some(t.clone());
    }
    let other = if preferred == TickSource::Chainlink {
        TickSource::Binance
    } else {
        TickSource::Chainlink
    };
    ticks
        .iter()
        .filter(|t| t.source == other && t.ts_ms >= lo && t.ts_ms <= hi)
        .min_by_key(|t| (t.ts_ms - start_ms).abs())
        .cloned()
}

pub fn decimal_to_f64(d: &Decimal) -> Option<f64> {
    Decimal::to_string(d).parse::<f64>().ok()
}

pub fn parse_decimal(s: &str) -> Option<Decimal> {
    Decimal::from_str(s).ok()
}
