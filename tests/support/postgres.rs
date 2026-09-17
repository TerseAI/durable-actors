use std::{panic::AssertUnwindSafe, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, Pool, Runtime};
use futures_util::FutureExt;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Config, NoTls, config::SslMode};

mod embedded {
    refinery::embed_migrations!("migrations");
}

pub struct TestDatabase {
    pub pool: Pool,
    pub url: String,
    admin: deadpool_postgres::Object,
    schema: String,
}

pub async fn with_postgres(test: impl AsyncFnOnce(&TestDatabase) -> Result<()>) -> Result<()> {
    with_postgres_schema(async |database| {
        let mut client = database.pool.get().await?;
        embedded::migrations::runner()
            .run_async(&mut **client)
            .await?;
        drop(client);
        test(database).await
    })
    .await
}

pub async fn with_postgres_schema(
    test: impl AsyncFnOnce(&TestDatabase) -> Result<()>,
) -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_OBJECT_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let database = TestDatabase::create(&url).await?;
    let outcome = AssertUnwindSafe(async { test(&database).await })
        .catch_unwind()
        .await;
    let cleanup = database.cleanup().await;
    if let Err(error) = &cleanup {
        eprintln!("{error:#}");
    }
    match outcome {
        Ok(result) => result.and(cleanup),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

impl TestDatabase {
    async fn create(url: &str) -> Result<Self> {
        let admin = connection_pool(url)?
            .get()
            .await
            .context("connect to test PostgreSQL")?;
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());
        let url = schema_url(url, &schema)?;
        let pool = connection_pool(&url)?;
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await?;
        Ok(Self {
            pool,
            url,
            admin,
            schema,
        })
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close();
        self.admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .context("clean up PostgreSQL test schema")
    }
}

fn schema_url(url: &str, schema: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(url)?;
    // PostgreSQL URI parameters use percent encoding, not form encoding.
    let query = url.query().map(|query| query.replace('+', "%2B"));
    url.set_query(query.as_deref());
    let mut options = String::new();
    let parameters: Vec<_> = url
        .query_pairs()
        .filter_map(|(key, value)| {
            if key == "options" {
                options.push_str(&value);
                options.push(' ');
                None
            } else {
                Some((key.into_owned(), value.into_owned()))
            }
        })
        .collect();
    options.push_str(&format!("-csearch_path={schema}"));
    url.query_pairs_mut()
        .clear()
        .extend_pairs(parameters)
        .append_pair("options", &options);
    let query = url.query().unwrap().replace('+', "%20");
    url.set_query(Some(&query));
    Ok(url.into())
}

fn connection_pool(url: &str) -> Result<Pool> {
    let config = Config::from_str(url)?;
    let manager = match config.get_ssl_mode() {
        SslMode::Disable => Manager::new(config, NoTls),
        _ => Manager::new(
            config,
            MakeTlsConnector::new(TlsConnector::builder().build()?),
        ),
    };
    Ok(Pool::builder(manager)
        .max_size(4)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(5)))
        .create_timeout(Some(Duration::from_secs(5)))
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_urls_preserve_postgres_connection_options() -> Result<()> {
        let url = schema_url(
            "postgresql://localhost/test?options=-cstatement_timeout%3D10000%20-csearch_path%3Dpublic&application_name=fixture+check",
            "test_scope",
        )?;
        let config = Config::from_str(&url)?;
        assert_eq!(
            config.get_options(),
            Some("-cstatement_timeout=10000 -csearch_path=public -csearch_path=test_scope")
        );
        assert_eq!(config.get_application_name(), Some("fixture+check"));
        Ok(())
    }
}
