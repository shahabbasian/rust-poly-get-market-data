use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

pub async fn create_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;

    info!("Connected to PostgreSQL");
    Ok(pool)
}


