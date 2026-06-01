use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Upcoming,
    Live,
    Resolved,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Upcoming => "upcoming",
            Status::Live => "live",
            Status::Resolved => "resolved",
        }
    }
}

impl FromStr for Status {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "upcoming" => Status::Upcoming,
            "live" => Status::Live,
            "resolved" => Status::Resolved,
            _ => return Err(()),
        })
    }
}

/// Atomically claim a market: transition `upcoming → live` and return true if we won the race.
pub async fn try_claim_live(pool: &PgPool, id: Uuid, task_id: Uuid) -> anyhow::Result<bool> {
    let res = sqlx::query(
        r#"
        UPDATE new_markets
        SET status = 'live',
            live_started_at = now(),
            collector_task_id = $2
        WHERE id = $1 AND status = 'upcoming'
        "#,
    )
    .bind(id)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

pub async fn mark_resolved(
    pool: &PgPool,
    id: Uuid,
    winning_outcome: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE new_markets
        SET status = 'resolved',
            resolved_at = now(),
            winning_outcome = $2
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(winning_outcome)
    .execute(pool)
    .await?;
    Ok(())
}
