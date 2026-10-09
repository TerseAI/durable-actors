use super::*;
use axum::{Router, extract::State, http::StatusCode, routing::get};
use kube_quantity::ParsedQuantity;
use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{family::Family, gauge::Gauge},
    registry::Registry,
};
use std::{net::SocketAddr, sync::atomic::AtomicU64};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PoolLabels {
    pool_namespace: String,
    pool: String,
}

impl SubstrateProvider {
    pub async fn start_metrics(
        self: &Arc<Self>,
        bind: SocketAddr,
        stop: CancellationToken,
    ) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(bind).await?;
        let routes = Router::new()
            .route("/metrics", get(scrape))
            .with_state(self.clone());
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, routes)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
            {
                tracing::error!(%error, "Substrate metrics server failed");
            }
        });
        Ok(())
    }
}

async fn scrape(
    State(provider): State<Arc<SubstrateProvider>>,
) -> Result<([(&'static str, &'static str); 1], String), StatusCode> {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let workers = provider.api.workers().await?;
        encode_capacity(workers.into_iter().filter(|worker| {
            provider
                .config
                .worker_labels
                .iter()
                .all(|(key, value)| worker.labels.get(key) == Some(value))
        }))
    })
    .await;
    match result {
        Ok(Ok(body)) => Ok((
            [(
                "content-type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )),
        error => {
            tracing::warn!(?error, "Substrate capacity scrape failed");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

fn encode_capacity(workers: impl Iterator<Item = proto::Worker>) -> Result<String> {
    let demand = Family::<PoolLabels, Gauge<f64, AtomicU64>>::default();
    let mut count = 0;
    for worker in workers {
        let value = worker_demand(&worker)?;
        let labels = PoolLabels {
            pool_namespace: worker.worker_namespace,
            pool: worker.worker_pool,
        };
        demand.get_or_create(&labels).inc_by(value);
        count += 1;
    }
    ensure!(count > 0, "no matching Substrate workers reported capacity");
    let mut registry = Registry::default();
    registry.register(
        "terse_substrate_worker_demand",
        "Sum of each worker's largest reserved CPU, memory or actor-slot fraction",
        demand,
    );
    let mut output = String::new();
    encode(&mut output, &registry)?;
    Ok(output)
}

fn worker_demand(worker: &proto::Worker) -> Result<f64> {
    let status = worker.status.as_ref().context("worker status missing")?;
    ensure!(
        status.state() != proto::WorkerState::Unspecified,
        "worker state missing"
    );
    let capacity = status
        .capacity
        .as_ref()
        .context("worker capacity missing")?;
    ensure!(capacity.actors > 0, "worker actor capacity missing");
    let allocated = status.allocated.clone().unwrap_or_default();
    ensure!(allocated.actors >= 0, "negative actor allocation");
    let mut fraction = f64::from(allocated.actors) / f64::from(capacity.actors);
    for name in ["cpu", "memory"] {
        let total = quantity(capacity, name)?.context("worker resource capacity missing")?;
        ensure!(total > 0.0, "worker resource capacity must be positive");
        let used = quantity(&allocated, name)?.unwrap_or(0.0);
        fraction = fraction.max(used / total);
    }
    Ok(fraction)
}

fn quantity(resources: &proto::WorkerResources, name: &str) -> Result<Option<f64>> {
    resources
        .resources
        .as_ref()
        .and_then(|resources| resources.limits.iter().find(|limit| limit.name == name))
        .map(|limit| {
            let value = ParsedQuantity::try_from(limit.quantity.as_str())?
                .to_bytes_f64()
                .context("resource quantity is out of range")?;
            ensure!(
                value.is_finite() && value >= 0.0,
                "invalid resource quantity"
            );
            Ok(value)
        })
        .transpose()
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/substrate_metrics.rs"]
mod tests;
