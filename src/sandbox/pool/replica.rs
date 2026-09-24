use super::*;

enum Reservation {
    Ready(SpareHandle),
    Create(String),
    Pending,
}

impl SparePool {
    pub(crate) async fn acquire_replica(
        &self,
        image: &str,
        region: &str,
        host: &str,
    ) -> Result<SpareHandle> {
        ensure!(
            self.config.kind == SpareKind::Replica,
            "replica pool required"
        );
        let key = self.key(image, region);
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let reservation = self.store.reserve_replica(&key, host).await?;
                self.wake.notify_one();
                match reservation {
                    Reservation::Ready(handle) => return Ok(handle),
                    Reservation::Create(name) => {
                        return self.build_claimed(image, region, host, name).await;
                    }
                    Reservation::Pending => tokio::time::sleep(Duration::from_millis(25)).await,
                }
            }
        })
        .await
        .context("replica spare acquisition timed out")?
    }

    pub(crate) async fn activate_replica(&self, host: &str) -> Result<()> {
        let updated = self.store.0.execute("UPDATE durable_actors_spares SET status = 'active', expires_at = created_at + interval '24 hours' WHERE host_id = $1 AND kind = 'replica' AND status IN ('claimed', 'active') AND handle IS NOT NULL AND expires_at > clock_timestamp()", &[&host]).await?;
        ensure!(updated == 1, "replica spare claim expired or was retired");
        Ok(())
    }

    pub(crate) async fn retire_replica(&self, host: &str) -> Result<()> {
        let row = self.store.0.query_opt("UPDATE durable_actors_spares SET status = 'retiring' WHERE host_id = $1 AND kind = 'replica' RETURNING name, handle", &[&host]).await?;
        if let Some(row) = row {
            let handle = decode_handle(&row)?;
            self.provider.retire_spare(&handle).await?;
            self.store
                .0
                .execute(
                    "DELETE FROM durable_actors_spares WHERE name = $1 AND status = 'retiring'",
                    &[&handle.name],
                )
                .await?;
        }
        Ok(())
    }

    async fn build_claimed(
        &self,
        image: &str,
        region: &str,
        host: &str,
        name: String,
    ) -> Result<SpareHandle> {
        let handle = match self.create(image, region, &name).await {
            Ok(handle) => handle,
            Err(error) => {
                self.failed(host).await?;
                return Err(error);
            }
        };
        let saved = self.store.0.execute("UPDATE durable_actors_spares SET handle = $3 WHERE name = $1 AND host_id = $2 AND status = 'claimed' AND expires_at > clock_timestamp()", &[&name, &host, &serde_json::to_string(&handle)?]).await;
        if !matches!(saved, Ok(1)) {
            self.provider.retire_spare(&handle).await?;
            saved?;
            anyhow::bail!("replica spare claim was retired during startup");
        }
        Ok(handle)
    }
}

impl PoolStore {
    async fn reserve_replica(&self, key: &str, host: &str) -> Result<Reservation> {
        let mut client = self.0.connection().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 1))",
            &[&host],
        )
        .await?;
        if let Some(row) = tx.query_opt("SELECT name, handle, status, expires_at > clock_timestamp() AS live FROM durable_actors_spares WHERE host_id = $1 AND kind = 'replica'", &[&host]).await? {
            let status: &str = row.get("status");
            ensure!(status == "active" || (status == "claimed" && row.get::<_, bool>("live")), "replica spare claim expired or was retired");
            let result = if row.get::<_, Option<&str>>("handle").is_some() { Reservation::Ready(decode_handle(&row)?) } else { Reservation::Pending };
            tx.commit().await?;
            return Ok(result);
        }
        let available = tx.query_opt("UPDATE durable_actors_spares SET status = 'claimed', host_id = $2, expires_at = clock_timestamp() + interval '120 seconds' WHERE name = (SELECT name FROM durable_actors_spares WHERE pool_key = $1 AND kind = 'replica' AND status = 'ready' AND expires_at > clock_timestamp() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING name, handle", &[&key, &host]).await?;
        let result = match available {
            Some(row) => Reservation::Ready(decode_handle(&row)?),
            None => {
                let name = format!("do-spare-{}", uuid::Uuid::new_v4().simple());
                tx.execute("INSERT INTO durable_actors_spares (name, pool_key, kind, status, host_id, expires_at) VALUES ($1, $2, 'replica', 'claimed', $3, clock_timestamp() + interval '120 seconds')", &[&name, &key, &host]).await?;
                Reservation::Create(name)
            }
        };
        tx.commit().await?;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/replica_pool.rs"]
mod tests;
