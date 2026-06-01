use crate::models::PolymarketMarket;
use anyhow::{Context, Result};
use serde_json::Value;

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

pub struct GammaClient {
    client: reqwest::Client,
}

impl GammaClient {
    pub fn new() -> Self {
        GammaClient {
            client: reqwest::Client::new(),
        }
    }

    /// Fetch active markets by series slug, ordered by end_date ascending so upcoming markets appear first.
    pub async fn fetch_markets_by_series(
        &self,
        series_slug: &str,
        limit: i32,
    ) -> Result<Vec<PolymarketMarket>> {
        let url = format!(
            "{}/markets?limit={}&order=end_date&ascending=true&closed=false",
            GAMMA_API_BASE, limit
        );
        let resp = self.client.get(&url).send().await.context("Failed to fetch markets")?;
        let text = resp.text().await.context("Failed to read response text")?;
        let markets: Vec<PolymarketMarket> = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse markets JSON: {}", &text[..text.len().min(500)]))?;
        // Filter by series slug in-memory (Gamma API doesn't expose series_slug filter directly on /markets)
        let filtered: Vec<_> = markets.into_iter().filter(|m| {
            m.event.as_ref().map(|e| e.seriesSlug.as_ref()) == Some(Some(&series_slug.to_string()))
        }).collect();
        Ok(filtered)
    }

    /// Fetch a series by its slug, then extract all nested markets from events.
    /// Each market is paired with the series slug so callers don't need to inspect
    /// `market.event` (which may be absent or incomplete in nested responses).
    pub async fn fetch_series_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<Vec<(String, PolymarketMarket)>>> {
        let url = format!("{}/series?slug={}&limit=1", GAMMA_API_BASE, slug);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch series by slug")?;
        let text = resp.text().await.context("Failed to read series response")?;
        let series: Vec<Value> = serde_json::from_str(&text).with_context(|| {
            format!(
                "Failed to parse series JSON: {}",
                &text[..text.len().min(500)]
            )
        })?;
        if series.is_empty() {
            return Ok(None);
        }
        let s = &series[0];
        let events = s.get("events").and_then(|e| e.as_array()).cloned().unwrap_or_default();

        let mut all_markets = Vec::new();
        for event in events {
            let markets = event
                .get("markets")
                .and_then(|m| m.as_array())
                .cloned()
                .unwrap_or_default();
            for m in markets {
                let parsed: PolymarketMarket =
                    serde_json::from_value(m).context("Failed to parse nested market")?;
                all_markets.push((slug.to_string(), parsed));
            }
        }
        Ok(Some(all_markets))
    }

    /// Fetch all active markets from the series endpoint by target series slugs.
    pub async fn fetch_all_active_markets_for_series(
        &self,
        series_slugs: &[String],
        _limit: i32,
    ) -> Result<Vec<(String, PolymarketMarket)>> {
        let mut all = Vec::new();
        for slug in series_slugs {
            match self.fetch_series_by_slug(slug).await {
                Ok(Some(markets)) => {
                    all.extend(markets);
                }
                Ok(None) => {
                    tracing::warn!("No series found for slug: {}", slug);
                }
                Err(e) => {
                    tracing::error!("Error fetching series for slug {}: {}", slug, e);
                }
            }
        }
        Ok(all)
    }

    /// Discover "Up or Down" series on the Gamma API and return their metadata.
    /// Queries `/series?active=true` and filters by title/slug containing up/down language.
    pub async fn discover_updown_series(
        &self,
    ) -> Result<Vec<(String, String, String)>> {
        let url = format!("{}/series?limit=500&active=true", GAMMA_API_BASE);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch series list")?;
        let text = resp.text().await.context("Failed to read series list")?;
        let series: Vec<Value> = serde_json::from_str(&text).with_context(|| {
            format!(
                "Failed to parse series list JSON: {}",
                &text[..text.len().min(500)]
            )
        })?;

        let mut discovered = Vec::new();
        for s in series {
            let title = s
                .get("title")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_lowercase();
            let slug = s
                .get("slug")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_lowercase();

            // Only care about Up or Down / Updown series
            if !title.contains("up or down") && !title.contains("updown") && !slug.contains("updown")
            {
                continue;
            }

            let (asset, interval) = parse_series_meta(&slug, &title);
            discovered.push((slug, asset, interval));
        }
        Ok(discovered)
    }
}

/// Parse asset symbol and interval from series slug/title.
/// Slug patterns: eth-up-or-down-daily, btc-up-or-down-hourly, etc.
fn parse_series_meta(slug: &str, _title: &str) -> (String, String) {
    let parts: Vec<&str> = slug.split('-').collect();

    // Asset symbol: first word in slug
    let asset = parts.first().unwrap_or(&"").to_uppercase();

    // Heuristic: look for interval keywords in the slug
    let interval = if slug.contains("daily") {
        "daily"
    } else if slug.contains("weekly") {
        "weekly"
    } else if slug.contains("monthly") {
        "monthly"
    } else if slug.contains("hourly") {
        "hourly"
    } else if slug.contains("-5m") || slug.ends_with("-5m") {
        "5m"
    } else if slug.contains("-15m") || slug.ends_with("-15m") {
        "15m"
    } else if slug.contains("-4h") || slug.ends_with("-4h") {
        "4h"
    } else {
        "unknown"
    };

    (asset, interval.to_string())
}
