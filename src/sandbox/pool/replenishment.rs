use super::{
    sizing::{History, Policy, Target},
    *,
};
use tokio_postgres::Transaction;

impl PoolStore {
    pub(super) async fn replenish(
        &self,
        key: &str,
        config: &PoolConfig,
        batch: u32,
    ) -> Result<Vec<String>> {
        let mut client = self.0.connection().await?;
        let tx = client.transaction().await?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext('durable-actors'), hashtext('spare-capacity'))",
            &[],
        )
        .await?;
        tx.execute("INSERT INTO durable_actors_pool_targets (pool_key, kind, target) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING", &[&key, &self.1.as_str(), &(config.idle as i32)]).await?;
        let row = tx.query_one("SELECT target, shrink_at_ms, retry_after <= clock_timestamp() AS can_build, (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms FROM durable_actors_pool_targets WHERE pool_key = $1 FOR UPDATE", &[&key]).await?;
        let history = history(&tx, key).await?;
        let plan = Policy {
            minimum: config.idle,
            maximum: config.maximum,
            shrink_after_seconds: config.shrink_after_seconds,
        }
        .plan(
            &history,
            Target {
                size: row.get::<_, i32>("target") as u32,
                shrink_at_ms: row.get("shrink_at_ms"),
            },
            row.get("now_ms"),
        );
        tx.execute("UPDATE durable_actors_pool_targets SET target = $2, shrink_at_ms = $3 WHERE pool_key = $1", &[&key, &(plan.target.size as i32), &plan.target.shrink_at_ms]).await?;
        trim(&tx, key, plan.target.size).await?;
        let count = if row.get::<_, bool>("can_build") {
            capacity(&tx, key, config, plan.target.size, plan.horizon_seconds)
                .await?
                .min(batch)
        } else {
            0
        };
        let names = reserve(&tx, key, self.1, count).await?;
        tx.commit().await?;
        if row.get::<_, i32>("target") as u32 != plan.target.size {
            tracing::info!(
                pool_key = key,
                previous = row.get::<_, i32>("target"),
                target = plan.target.size,
                refill_seconds = plan.horizon_seconds,
                "spare pool target changed"
            );
        }
        Ok(names)
    }

    pub(super) async fn build_failed(&self, key: &str, name: &str) -> Result<()> {
        self.0.execute("WITH backoff AS (UPDATE durable_actors_pool_targets SET failures = least(failures + 1, 6), retry_after = clock_timestamp() + make_interval(secs => power(2, least(failures, 5))) WHERE pool_key = $1 RETURNING pool_key) UPDATE durable_actors_spares SET status = 'retiring' WHERE name = $2 AND status = 'starting' AND pool_key IN (SELECT pool_key FROM backoff)", &[&key, &name]).await?;
        Ok(())
    }
}

async fn history(tx: &Transaction<'_>, key: &str) -> Result<History> {
    let demand = tx.query("SELECT floor(extract(epoch FROM (clock_timestamp() - created_at)))::bigint AS age, count(*)::bigint FROM durable_actors_pool_events WHERE pool_key = $1 AND event_kind = 'acquire' AND created_at > clock_timestamp() - interval '60 seconds' GROUP BY age", &[&key]).await?
        .into_iter().map(|row| (row.get::<_, i64>(0).max(0) as u32, row.get::<_, i64>(1).min(i64::from(u32::MAX)) as u32)).collect();
    let startup_ms = tx.query("SELECT startup_ms FROM durable_actors_pool_events WHERE pool_key = $1 AND event_kind = 'ready' AND created_at > clock_timestamp() - interval '10 minutes' ORDER BY created_at DESC LIMIT 100", &[&key]).await?
        .into_iter().map(|row| row.get::<_, i64>(0).max(1) as u64).collect();
    Ok(History { demand, startup_ms })
}

async fn trim(tx: &Transaction<'_>, key: &str, target: u32) -> Result<()> {
    // Keep replacements in flight and retire the oldest ready spare only after the new one is ready.
    tx.execute("UPDATE durable_actors_spares SET status = 'retiring' WHERE status = 'ready' AND name IN (SELECT name FROM (SELECT name, row_number() OVER (ORDER BY expires_at DESC, name) AS rank FROM durable_actors_spares WHERE pool_key = $1 AND status = 'ready') spares WHERE rank > $2)", &[&key, &i64::from(target)]).await?;
    Ok(())
}

async fn capacity(
    tx: &Transaction<'_>,
    key: &str,
    config: &PoolConfig,
    target: u32,
    horizon: u32,
) -> Result<u32> {
    let row = tx.query_one("SELECT count(*) FILTER (WHERE pool_key = $1 AND expires_at > clock_timestamp() AND (status = 'starting' OR (status = 'ready' AND expires_at > clock_timestamp() + make_interval(secs => $2)))) AS available, count(*) FILTER (WHERE pool_key = $1) AS occupied, count(*) AS fleet, count(*) FILTER (WHERE status = 'starting') AS starting FROM durable_actors_spares WHERE status IN ('ready', 'starting') OR (status = 'retiring' AND host_id IS NULL AND handle IS NOT NULL)", &[&key, &f64::from(horizon)]).await?;
    let remaining = |limit: u32, field| {
        limit.saturating_sub(row.get::<_, i64>(field).min(i64::from(u32::MAX)) as u32)
    };
    Ok(remaining(target, "available")
        .min(remaining(config.maximum, "occupied"))
        .min(remaining(config.fleet_maximum, "fleet"))
        .min(remaining(config.max_starting, "starting")))
}

async fn reserve(
    tx: &Transaction<'_>,
    key: &str,
    kind: SpareKind,
    count: u32,
) -> Result<Vec<String>> {
    let names: Vec<_> = (0..count)
        .map(|_| format!("do-spare-{}", uuid::Uuid::new_v4().simple()))
        .collect();
    if !names.is_empty() {
        tx.execute("INSERT INTO durable_actors_spares (name, pool_key, kind, status, expires_at) SELECT unnest($1::text[]), $2, $3, 'starting', clock_timestamp() + interval '120 seconds'", &[&names, &key, &kind.as_str()]).await?;
    }
    Ok(names)
}
