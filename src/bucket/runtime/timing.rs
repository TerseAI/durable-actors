use std::{future::Future, time::Instant};

use anyhow::Result;
use tracing::Span;

pub(super) async fn phase<T>(
    name: &'static str,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    let span = Span::current();
    if span
        .metadata()
        .is_none_or(|metadata| metadata.name() != "actor_activation")
    {
        return work.await;
    }
    let mut timing = Phase {
        span,
        name,
        started: Instant::now(),
        outcome: "cancelled",
    };
    let result = work.await;
    timing.outcome = if result.is_ok() {
        "completed"
    } else {
        "failed"
    };
    result
}

struct Phase {
    span: Span,
    name: &'static str,
    started: Instant,
    outcome: &'static str,
}

impl Drop for Phase {
    fn drop(&mut self) {
        self.span.with_subscriber(|(_, dispatch)| {
            tracing::dispatcher::with_default(dispatch, || {
                self.span.in_scope(|| {
                    tracing::info!(
                        event = "actor_activation_phase",
                        phase = self.name,
                        duration_ms = self.started.elapsed().as_secs_f64() * 1_000.0,
                        outcome = self.outcome,
                    )
                });
            });
        });
    }
}
