use chrono::prelude::*;

use crate::models::{ASSET_MAP, INTERVAL_CONFIG};

pub fn short_slug(symbol: &str, interval: &str, epoch_ts: i64) -> String {
    format!("{}-updown-{}-{}", symbol, interval, epoch_ts)
}

pub fn hourly_slug(full_name: &str, naive_et: NaiveDateTime) -> String {
    let month = naive_et.format("%B").to_string().to_lowercase();
    let day = naive_et.day();
    let year_num = naive_et.year();
    let hour24 = naive_et.hour();
    let (h12, ampm) = match hour24 {
        0 => (12, "am"),
        1..=11 => (hour24, "am"),
        12 => (12, "pm"),
        13..=23 => (hour24 - 12, "pm"),
        _ => (12, "am"),
    };

    format!(
        "{}-up-or-down-{}-{}-{}-{}{}-et",
        full_name, month, day, year_num, h12, ampm
    )
}

pub fn candidate_slugs(
    lookahead: &std::collections::HashMap<&str, u32>,
) -> Vec<(String, String, String, String)> {
    let now = Utc::now();
    let mut slugs = Vec::new();

    for (symbol, full_name) in ASSET_MAP {
        for (interval, interval_secs) in INTERVAL_CONFIG {
            let lookahead_hours = *lookahead.get(interval).unwrap_or(&24);
            let lookahead_dur = chrono::Duration::hours(lookahead_hours as i64);
            let lookbehind_dur = chrono::Duration::minutes(5);
            let interval_dur = chrono::Duration::seconds(*interval_secs as i64);

            let from_ts = now - lookbehind_dur;
            let to_ts = now + lookahead_dur;

            let mut ts = round_up(from_ts, interval_dur);
            while ts <= to_ts {
                let slug = if interval == &"1h" {
                    let et = ts.with_timezone(&chrono_tz::US::Eastern);
                    let naive_et = et.naive_local();
                    hourly_slug(full_name, naive_et)
                } else {
                    short_slug(symbol, interval, ts.timestamp())
                };

                slugs.push((
                    slug,
                    symbol.to_string(),
                    interval.to_string(),
                    full_name.to_string(),
                ));

                ts = ts + interval_dur;
            }
        }
    }

    slugs.sort_by(|a, b| a.0.cmp(&b.0));
    slugs.dedup_by(|a, b| a.0 == b.0);
    slugs
}

fn round_up(dt: DateTime<Utc>, duration: chrono::Duration) -> DateTime<Utc> {
    let secs = dt.timestamp();
    let dur_secs = duration.num_seconds();
    let remainder = secs % dur_secs;
    let aligned = if remainder == 0 { secs } else { secs - remainder + dur_secs };
    Utc.timestamp_opt(aligned, 0).single().unwrap_or(dt)
}
