use std::sync::Arc;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    clock::Clock,
    host::HostId,
    host_leases::{
        HostLease, HostLeaseRegistry, HostLeaseRequest, HostLeaseStatus, HostLeaseStore,
    },
};

use super::{Bucket, component, replace};

pub struct BucketHostLeases {
    bucket: Arc<dyn Bucket>,
    clock: Arc<dyn Clock>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    lease: HostLease,
    retired: Vec<String>,
    mutation: String,
}

impl BucketHostLeases {
    pub fn new(bucket: Arc<dyn Bucket>, clock: Arc<dyn Clock>) -> Self {
        Self { bucket, clock }
    }
}

#[async_trait]
impl HostLeaseRegistry for BucketHostLeases {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        request.validate_duration()?;
        ensure!(
            !request.id.as_str().is_empty() && !request.session_id.is_empty(),
            "lease identity is empty"
        );
        let key = key(&request.id);
        let current = self.bucket.get(&key).await?;
        let now = self.clock.now_ms()?;
        let mut retired = Vec::new();
        if let Some(object) = &current {
            let record: Record = serde_json::from_slice(&object.bytes)?;
            ensure!(
                !record.retired.contains(&request.session_id),
                "host session is retired"
            );
            if record.lease.session_id == request.session_id {
                ensure!(
                    record.lease.expires_at_ms > now,
                    "expired host session cannot renew"
                );
            } else {
                ensure!(
                    record.lease.expires_at_ms <= now,
                    "host lease is held by another session"
                );
            }
            retired = record.retired;
            if record.lease.session_id != request.session_id {
                retired.push(record.lease.session_id);
            }
        }
        let lease = HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: now
                .checked_add(request.duration_ms)
                .ok_or_else(|| anyhow::anyhow!("lease expiration overflow"))?,
        };
        let record = Record {
            lease: lease.clone(),
            retired,
            mutation: uuid::Uuid::new_v4().to_string(),
        };
        ensure!(
            replace(
                self.bucket.as_ref(),
                &key,
                current.map(|o| o.generation),
                serde_json::to_vec(&record)?
            )
            .await?,
            "host lease changed concurrently"
        );
        Ok(lease)
    }

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()> {
        let key = key(id);
        let Some(current) = self.bucket.get(&key).await? else {
            return Ok(());
        };
        let mut record: Record = serde_json::from_slice(&current.bytes)?;
        if record.lease.session_id != session_id {
            return Ok(());
        }
        record.lease.expires_at_ms = 0;
        record.mutation = uuid::Uuid::new_v4().to_string();
        ensure!(
            replace(
                self.bucket.as_ref(),
                &key,
                Some(current.generation),
                serde_json::to_vec(&record)?
            )
            .await?,
            "host lease changed while releasing"
        );
        Ok(())
    }
}

#[async_trait]
impl HostLeaseStore for BucketHostLeases {
    async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus> {
        let lease = self
            .bucket
            .get(&key(id))
            .await?
            .map(|object| {
                serde_json::from_slice::<Record>(&object.bytes).map(|record| record.lease)
            })
            .transpose()?;
        Ok(HostLeaseStatus {
            lease,
            store_now_ms: self.clock.now_ms()?,
        })
    }
}

fn key(id: &HostId) -> String {
    format!("runtime/hosts/{}.json", component(id.as_str()))
}
