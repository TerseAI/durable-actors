use super::*;
use crate::host_leases::HostLeaseRequest;

impl RuntimeStorage {
    pub async fn register_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        new_actor: bool,
    ) -> Result<LoadedActor> {
        actor.validate()?;
        crate::placement::validate_region(region)?;
        request.validate_duration()?;
        let current = if new_actor {
            None
        } else {
            self.load(&actor.storage_key()).await?
        };
        let recovered = match &current {
            Some((_, record)) => {
                ensure!(
                    record.actor == *actor && record.region == region,
                    "ownership scope cannot change"
                );
                ensure!(
                    record.lease.id != request.id || record.lease.session_id != request.session_id,
                    "activation session cannot be reused"
                );
                ensure!(
                    record.lease.expires_at_ms <= self.clock.now_ms()?,
                    "previous owner lease is still active"
                );
                let recovered = self.recover_session(record).await?;
                self.latest(record, recovered).await?
            }
            None => None,
        };
        let mut record = Ownership {
            actor: actor.clone(),
            epoch: current
                .as_ref()
                .map_or(Some(1), |(_, record)| record.epoch.checked_add(1))
                .context("owner epoch overflow")?,
            region: region.into(),
            base: recovered
                .as_ref()
                .map(|snapshot| snapshot.reference.clone()),
            lease: self.new_lease(request)?,
            mutation: String::new(),
        };
        self.save_activation(&mut record, current.map(|(generation, _)| generation))
            .await?;
        Ok(self.remember(record, recovered))
    }

    pub async fn renew_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
    ) -> Result<HostLease> {
        let (generation, mut record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor ownership missing")?;
        ensure!(
            record.actor == *actor
                && record.lease.id == request.id
                && record.lease.session_id == request.session_id,
            "actor ownership changed"
        );
        let current = &record.lease;
        ensure!(
            current.expires_at_ms > self.clock.now_ms()?,
            "expired activation cannot renew"
        );
        ensure!(
            current.route == request.route,
            "activation route cannot change"
        );
        let lease = self.new_lease(request)?;
        record.lease = lease.clone();
        self.save_activation(&mut record, Some(generation)).await?;
        Ok(lease)
    }

    pub async fn release_activation(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) -> Result<()> {
        let Some((generation, mut record)) = self.load(&actor.storage_key()).await? else {
            return Ok(());
        };
        if record.lease.id != *host || record.lease.session_id != session {
            return Ok(());
        }
        record.lease.expires_at_ms = 0;
        self.save_activation(&mut record, Some(generation)).await
    }

    fn new_lease(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        request.validate_duration()?;
        ensure!(
            !request.id.as_str().is_empty() && !request.session_id.is_empty(),
            "activation identity missing"
        );
        Ok(HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: self
                .clock
                .now_ms()?
                .checked_add(request.duration_ms)
                .context("lease expiration overflow")?,
        })
    }

    async fn save_activation(&self, record: &mut Ownership, generation: Option<i64>) -> Result<()> {
        record.mutation = uuid::Uuid::new_v4().to_string();
        ensure!(
            replace(
                self.authority.as_ref(),
                &ownership_key(&record.actor.storage_key())?,
                generation,
                serde_json::to_vec(record)?
            )
            .await?,
            "actor activation changed concurrently"
        );
        Ok(())
    }
}
