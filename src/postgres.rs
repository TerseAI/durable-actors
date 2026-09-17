use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, Pool, Runtime};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Config, NoTls, Row, config::SslMode, types::ToSql};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[derive(Clone)]
pub(crate) struct PostgresDatabase {
    pool: Pool,
    initialized: Arc<tokio::sync::OnceCell<()>>,
}

impl PostgresDatabase {
    #[cfg(test)]
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let database = Self::lazy(url)?;
        database.initialize().await?;
        Ok(database)
    }

    pub(crate) fn lazy(url: &str) -> Result<Self> {
        Ok(Self {
            pool: connection_pool(url)?,
            initialized: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    async fn initialize(&self) -> Result<()> {
        self.initialized
            .get_or_try_init(|| async {
                let mut client = self.pool.get().await.context("connect to PostgreSQL")?;
                embedded::migrations::runner()
                    .run_async(&mut **client)
                    .await
                    .context("run durable-object PostgreSQL migrations")?;
                anyhow::Ok(())
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn query_opt(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.query_opt(&statement, params).await?)
    }

    #[cfg(test)]
    pub(crate) async fn query_one(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.query_one(&statement, params).await?)
    }

    pub(crate) async fn execute(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        self.initialize().await?;
        let client = self
            .pool
            .get()
            .await
            .context("acquire PostgreSQL connection")?;
        let statement = client.prepare_cached(query).await?;
        Ok(client.execute(&statement, params).await?)
    }
}

fn connection_pool(url: &str) -> Result<Pool> {
    let config = Config::from_str(url).context("parse PostgreSQL connection URL")?;
    let manager = match config.get_ssl_mode() {
        SslMode::Disable => Manager::new(config, NoTls),
        _ => {
            let connector = TlsConnector::builder()
                .build()
                .context("build PostgreSQL TLS connector")?;
            Manager::new(config, MakeTlsConnector::new(connector))
        }
    };
    Ok(Pool::builder(manager)
        .max_size(8)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(5)))
        .create_timeout(Some(Duration::from_secs(5)))
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_startup_does_not_connect_to_postgres() -> Result<()> {
        let database =
            PostgresDatabase::lazy("postgresql://localhost:1/unavailable?sslmode=disable")?;
        assert_eq!(database.pool.status().size, 0);
        Ok(())
    }

    #[tokio::test]
    async fn independent_queries_can_use_different_database_connections() -> Result<()> {
        let Ok(url) = std::env::var("DURABLE_OBJECT_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let database = PostgresDatabase::connect(&url).await?;
        let (first, second) = tokio::try_join!(
            database.query_one("SELECT pg_backend_pid(), pg_sleep(0.05)", &[]),
            database.query_one("SELECT pg_backend_pid(), pg_sleep(0.05)", &[]),
        )?;
        assert_ne!(first.get::<_, i32>(0), second.get::<_, i32>(0));
        Ok(())
    }
}
