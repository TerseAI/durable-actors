use super::*;

impl SubstrateProvider {
    pub(super) async fn launch(&self, mut request: EnsureHostRequest) -> Result<ActorHostHandle> {
        let started_at_ms = SystemClock.now_ms()?;
        let started = std::time::Instant::now();
        let route = self.route(&actor_name(&request.host_id)?);
        let mut timings = ProvisioningTimings::default();
        let (credentials, ownership, sandbox) = tokio::join!(
            timed(self.bootstrap.credentials(&request)),
            timed(self.bootstrap.claim(&request, &route)),
            self.start_actor(&request, &mut timings),
        );
        timings.credentials_ms = credentials.1;
        timings.ownership_ms = ownership.1;
        timings.ready_to_assign_ms = millis(started);
        let allocated = SystemClock.now_ms()?;
        let mut assignment_attempted = false;
        let result = async {
            request.runtime_config = Some(credentials.0?);
            let handoff = ownership.0?;
            let (actor, secrets) = sandbox
                .as_ref()
                .map_err(|error| anyhow::anyhow!("{error:#}"))?;
            ensure!(
                handoff.expires_at_ms() > SystemClock.now_ms()?.saturating_add(5000),
                "activation handoff expired before assignment"
            );
            assignment_attempted = true;
            self.assign(&request, actor, secrets.clone(), &handoff, &mut timings)
                .await
        }
        .await;
        match result {
            Ok(mut handle) => {
                let (actor, _) = sandbox?;
                let completed = SystemClock.now_ms()?;
                tracing::info!(event = "substrate_provisioning", host_id = %request.host_id,
                    total_ms = completed - started_at_ms,
                    phases = %serde_json::to_string(&timings)?, "Substrate provisioning complete");
                let meta = actor.metadata.context("actor identity missing")?;
                handle.provisioning = Some(ActorHostProvisioning {
                    provider: "substrate".into(),
                    resource_id: format!("{}/{}/{}", meta.atespace, meta.name, meta.uid),
                    reused: true,
                    started_at_ms,
                    completed_at_ms: completed,
                    sandbox_scheduled_at_ms: Some(allocated),
                    host_ready_observed_at_ms: Some(completed),
                    input_parsed_at_ms: None,
                    sdk_loaded_at_ms: None,
                    resources_resolved_at_ms: None,
                    route_read_at_ms: None,
                    command_spawned_at_ms: None,
                    request_written_at_ms: None,
                    process_completed_at_ms: None,
                    response_decoded_at_ms: None,
                });
                Ok(handle)
            }
            Err(error) => {
                if let Ok((actor, _)) = sandbox {
                    self.discard(actor).await;
                }
                // An ambiguous assignment may have started a writer; let its lease expire.
                if !assignment_attempted
                    && let Err(cleanup) = self.bootstrap.release(&request).await
                {
                    tracing::warn!(%cleanup, "failed to release unassigned activation");
                }
                Err(error)
            }
        }
    }

    async fn start_actor(
        &self,
        request: &EnsureHostRequest,
        timings: &mut ProvisioningTimings,
    ) -> Result<(proto::Actor, HashMap<String, String>)> {
        let actor = self.allocate(request, timings).await?;
        match self.restore(request, &actor, timings).await {
            Ok(secrets) => Ok((actor, secrets)),
            Err(error) => {
                self.discard(actor).await;
                Err(error)
            }
        }
    }

    async fn discard(&self, actor: proto::Actor) {
        if let Err(cleanup) = self.api.delete(actor, None).await {
            tracing::warn!(%cleanup, "failed to delete unassigned Substrate actor");
        }
    }
}

async fn timed<T>(future: impl std::future::Future<Output = T>) -> (T, f64) {
    let started = std::time::Instant::now();
    let value = future.await;
    (value, millis(started))
}
