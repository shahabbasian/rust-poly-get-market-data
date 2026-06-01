use chrono::{DateTime, Utc};
use reqwest::Client as HttpClient;
use tracing::debug;

use crate::config::Config;
use crate::models::{ClobTokenIds, GammaMarketResponse};

#[derive(Debug, Clone)]
pub struct GammaClient {
    http: HttpClient,
    base_url: String,
}

impl GammaClient {
    pub fn new(config: &Config) -> Self {
        Self {
            http: HttpClient::builder()
                .timeout(std::time::Duration::from_secs(15))
                .user_agent("polymarket-scanner/0.1")
                .build()
                .expect("Failed to build HTTP client"),
            base_url: config.gamma_api_url.clone(),
        }
    }

    pub async fn get_market_by_slug(&self, slug: &str) -> anyhow::Result<Option<GammaMarketResponse>> {
        let url = format!("{}/markets/slug/{}", self.base_url, slug);

        let resp = self.http.get(&url).send().await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            debug!(slug, "Market not found");
            return Ok(None);
        }

        let market: GammaMarketResponse = resp.error_for_status()?.json().await?;
        Ok(Some(market))
    }
}

pub fn parse_clob_token_ids(raw: Option<&str>) -> ClobTokenIds {
    let raw = match raw {
        Some(s) => s,
        None => return ClobTokenIds::default(),
    };

    let trimmed = raw.trim_matches('"').trim_matches('\'');
    let parts: Vec<&str> = trimmed.split(',').map(|s| s.trim().trim_matches('"').trim_matches('\'')).collect();

    ClobTokenIds {
        yes: parts.first().map(|s| s.to_string()),
        no: parts.get(1).map(|s| s.to_string()),
    }
}

pub fn parse_optional_datetime(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|s| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|d| d.with_timezone(&Utc))
            .or_else(|| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ")
                    .ok()
                    .map(|naive| DateTime::from_naive_utc_and_offset(naive, Utc))
            })
    })
}
