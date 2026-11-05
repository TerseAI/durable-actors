use std::time::Duration;

use async_trait::async_trait;
use tokio_stream::{Stream, StreamExt};

const FLUSH_INTERVAL: Duration = Duration::from_millis(200);

#[async_trait]
pub(crate) trait BatchSink<T: Send>: Send + Sync {
    async fn report(&self, batch: Vec<T>);
}

/// Full batches are reported back-to-back; a partial batch waits at most `FLUSH_INTERVAL`.
/// The last batch is reported when the stream ends.
pub(crate) async fn export<T: Send>(
    items: impl Stream<Item = T>,
    batch_size: usize,
    sink: impl BatchSink<T>,
) {
    let batches = items.chunks_timeout(batch_size, FLUSH_INTERVAL);
    tokio::pin!(batches);
    while let Some(batch) = batches.next().await {
        sink.report(batch).await;
    }
}
