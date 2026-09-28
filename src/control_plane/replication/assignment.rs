use crate::{
    replication::ReplicaAssignment,
    sandbox::{SpareHandle, pool::SparePool},
};
use anyhow::{Result, ensure};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub(super) fn assignment_client(lifetime: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(lifetime)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?)
}

pub(super) async fn assign_replica(
    client: &reqwest::Client,
    spare: &SpareHandle,
    assignment: &ReplicaAssignment,
) -> Result<()> {
    let response = client
        .post(format!(
            "{}/assign",
            spare.control_route.trim_end_matches('/')
        ))
        .bearer_auth(&spare.control_token)
        .json(assignment)
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response.status() == reqwest::StatusCode::NO_CONTENT,
        "replica spare did not confirm assignment"
    );
    Ok(())
}

pub(super) async fn keep_warm(
    pool: Arc<SparePool>,
    client: reqwest::Client,
    stop: CancellationToken,
) {
    let mut warmed = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { () = stop.cancelled() => return, _ = interval.tick() => {} }
        let result = tokio::select! {
            () = stop.cancelled() => return,
            result = warm_idle(&pool, &client, &mut warmed) => result,
        };
        if let Err(error) = result {
            tracing::warn!(%error, "replica assignment warmup failed");
        }
    }
}

async fn warm_idle(
    pool: &SparePool,
    client: &reqwest::Client,
    warmed: &mut HashMap<String, Instant>,
) -> Result<()> {
    let routes = pool.idle_replica_routes().await?;
    warmed.retain(|route, _| routes.contains(route));
    let mut pending: FuturesUnordered<_> = routes
        .into_iter()
        .filter(|route| {
            warmed
                .get(route)
                .is_none_or(|last| last.elapsed() >= Duration::from_secs(30))
        })
        .map(|route| async move {
            let result = warm_assignment(client, &route).await;
            (route, result)
        })
        .collect();
    while let Some((route, result)) = pending.next().await {
        if result.is_ok() {
            warmed.insert(route, Instant::now());
        }
    }
    Ok(())
}

pub(super) async fn warm_assignment(client: &reqwest::Client, route: &str) -> Result<()> {
    let response = client
        .get(format!("{}/assign", route.trim_end_matches('/')))
        .timeout(Duration::from_secs(3))
        .send()
        .await?;
    // A GET intentionally receives 405; consuming it returns the connection to this client's pool.
    response.bytes().await?;
    Ok(())
}
