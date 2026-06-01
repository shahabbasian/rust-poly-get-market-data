use crate::models::{MarketUpsertData, PolymarketMarket};
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

    /// Fetch an event by its slug, which includes nested markets for a series.
    pub async fn fetch_event_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<Vec<PolymarketMarket>>> {
        let url = format!("{}/events?slug={}&limit=1", GAMMA_API_BASE, slug);
        let resp = self.client.get(&url).send().await.context("Failed to fetch event by slug")?;
        let text = resp.text().await.context("Failed to read event response")?;
        let events: Vec<Value> = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse events JSON: {}", &text[..text.len().min(500)]))?;
        if events.is_empty() {
            return Ok(None);
        }
        let event = &events[0];
        let markets = event.get("markets").and_then(|m| m.as_array()).cloned().unwrap_or_default();
        let parsed: Vec<PolymarketMarket> = markets.into_iter()
            .map(|v| serde_json::from_value(v))
            .collect::<Result<_, _>>()
            .context("Failed to parse nested markets")?;
        Ok(Some(parsed))
    }

    /// Fetch all active markets from the events endpoint and filter by target series slugs.
    pub async fn fetch_all_active_markets_for_series(
        &self,
        series_slugs: &[ String ],
        limit: i32,
    ) -> Result<Vec<PolymarketMarket>> {
        let mut all = Vec::new();
        for slug in series_slugs {
            match self.fetch_event_by_slug(slug).await {
                Ok(Some(markets)) => {
                    all.extend(markets);
                }
                Ok(None) => {
                    tracing::warn!("No event found for series slug: {}", slug);
                }
                Err(e) => {
                    tracing::error!("Error fetching event for slug {}: {}", slug, e);
                }
            }
        }
        Ok(all)
    }
}
