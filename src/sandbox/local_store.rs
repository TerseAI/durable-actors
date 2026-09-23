use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};

use super::EnsureHostRequest;
use crate::{actor::ActorKey, host_leases::HostLease};

pub(super) struct LocalHostStore(Arc<Mutex<Connection>>);

pub(super) struct Reservation {
    pub token: String,
    pub created: bool,
}

pub(super) struct HostRecord {
    pub actor: ActorKey,
    pub region: String,
    pub status: String,
    pub lease: Option<HostLease>,
    pub epoch: u64,
    pub error: Option<String>,
}

impl LocalHostStore {
    pub async fn open(path: PathBuf) -> Result<Self> {
        let connection = tokio::task::spawn_blocking(move || {
            let mut connection = Connection::open(path).context("open local host reservations")?;
            connection.busy_timeout(Duration::from_secs(5))?;
            connection.pragma_update(None, "journal_mode", "WAL")?;
            let transaction = connection.transaction()?;
            // The local data-directory lock excludes other runtimes; old process records are stale.
            transaction.execute_batch("
                CREATE TABLE IF NOT EXISTS runtime (stopping INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS deployments (config TEXT PRIMARY KEY, retiring INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS hosts (
                    token TEXT PRIMARY KEY, host_id TEXT NOT NULL UNIQUE,
                    config TEXT NOT NULL, region TEXT NOT NULL, actor TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('starting', 'ready', 'retiring', 'failed')),
                    lease TEXT, epoch TEXT NOT NULL DEFAULT '0', error TEXT,
                    UNIQUE(config, region, actor)
                );
                DELETE FROM hosts;
                DELETE FROM deployments;
                DELETE FROM runtime;
                INSERT INTO runtime VALUES (0);
            ")?;
            transaction.commit()?;
            anyhow::Ok(connection)
        }).await??;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    pub async fn reserve(&self, request: &EnsureHostRequest) -> Result<Reservation> {
        let config = request.host_config_key.clone();
        let region = request.canonical_region.clone();
        let actor = serde_json::to_string(
            request
                .actor
                .as_ref()
                .context("local actor identity missing")?,
        )?;
        let host = request.host_id.as_str().to_owned();
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            let stopping: bool = transaction.query_row("SELECT stopping FROM runtime", [], |row| row.get(0))?;
            ensure!(!stopping, "local runtime is shutting down");
            transaction.execute("INSERT INTO deployments (config) VALUES (?1) ON CONFLICT DO NOTHING", [&config])?;
            let retiring: bool = transaction.query_row("SELECT retiring FROM deployments WHERE config = ?1", [&config], |row| row.get(0))?;
            ensure!(!retiring, "local deployment is restarting");
            transaction.execute("DELETE FROM hosts WHERE config = ?1 AND region = ?2 AND actor = ?3 AND status = 'failed'", params![config, region, actor])?;
            let token = uuid::Uuid::new_v4().to_string();
            let created = transaction.execute("INSERT INTO hosts (token, host_id, config, region, actor, status) VALUES (?1, ?2, ?3, ?4, ?5, 'starting') ON CONFLICT(config, region, actor) DO NOTHING", params![token, host, config, region, actor])? == 1;
            let token = transaction.query_row("SELECT token FROM hosts WHERE config = ?1 AND region = ?2 AND actor = ?3", params![config, region, actor], |row| row.get(0))?;
            transaction.commit()?;
            Ok(Reservation { token, created })
        }).await
    }

    pub async fn get(&self, token: &str) -> Result<Option<HostRecord>> {
        self.lookup("token", token).await
    }

    pub async fn host(&self, host: &str) -> Result<Option<HostRecord>> {
        self.lookup("host_id", host).await
    }

    pub async fn publish(&self, token: &str, lease: &HostLease, epoch: u64) -> Result<bool> {
        let token = token.to_owned();
        let lease = serde_json::to_string(lease)?;
        let epoch = epoch.to_string();
        self.run(move |connection| Ok(connection.execute("UPDATE hosts SET status = 'ready', lease = ?2, epoch = ?3 WHERE token = ?1 AND status = 'starting'", params![token, lease, epoch])? == 1)).await
    }

    pub async fn finish(&self, token: &str, error: &str) -> Result<()> {
        let token = token.to_owned();
        let error = error.to_owned();
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "DELETE FROM hosts WHERE token = ?1 AND status = 'retiring'",
                [&token],
            )?;
            transaction.execute(
                "UPDATE hosts SET status = 'failed', lease = NULL, error = ?2 WHERE token = ?1",
                params![token, error],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn retire(&self, token: &str) -> Result<()> {
        let token = token.to_owned();
        self.run(move |connection| {
            connection.execute("UPDATE hosts SET status = 'retiring' WHERE token = ?1 AND status IN ('starting', 'ready')", [token])?;
            Ok(())
        }).await
    }

    pub async fn retire_config(&self, config: &str) -> Result<Vec<(String, String)>> {
        let config = config.to_owned();
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute("INSERT INTO deployments (config, retiring) VALUES (?1, 1) ON CONFLICT(config) DO UPDATE SET retiring = 1", [&config])?;
            transaction.execute("DELETE FROM hosts WHERE config = ?1 AND status = 'failed'", [&config])?;
            let hosts = transaction.prepare("UPDATE hosts SET status = 'retiring' WHERE config = ?1 RETURNING token, host_id")?.query_map([&config], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            transaction.commit()?;
            Ok(hosts)
        }).await
    }

    pub async fn resume_config(&self, config: &str) -> Result<()> {
        let config = config.to_owned();
        self.run(move |connection| {
            connection.execute(
                "UPDATE deployments SET retiring = 0 WHERE config = ?1",
                [config],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.run(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute_batch("UPDATE runtime SET stopping = 1; DELETE FROM hosts WHERE status = 'failed'; UPDATE hosts SET status = 'retiring';")?;
            transaction.commit()?;
            Ok(())
        }).await
    }

    async fn lookup(&self, column: &'static str, value: &str) -> Result<Option<HostRecord>> {
        let value = value.to_owned();
        self.run(move |connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT actor, region, status, lease, epoch, error FROM hosts WHERE {column} = ?1"
            ))?;
            let mut rows = statement.query([value])?;
            rows.next()?
                .map(|row| {
                    Ok(HostRecord {
                        actor: serde_json::from_str(&row.get::<_, String>(0)?)?,
                        region: row.get(1)?,
                        status: row.get(2)?,
                        lease: row
                            .get::<_, Option<String>>(3)?
                            .map(|lease| serde_json::from_str(&lease))
                            .transpose()?,
                        epoch: row.get::<_, String>(4)?.parse()?,
                        error: row.get(5)?,
                    })
                })
                .transpose()
        })
        .await
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.0.clone();
        tokio::task::spawn_blocking(move || operation(&mut connection.lock().unwrap())).await?
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/local_store.rs"]
mod tests;
