use super::*;

fn actor() -> ActorKey {
    ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    }
}

fn sample(at_ms: u64, memory_bytes: u64) -> ResourceSample {
    ResourceSample {
        actor: actor(),
        sampled_at_ms: at_ms,
        cpu_millicores: 100,
        memory_bytes,
        memory_limit_bytes: Some(1000),
    }
}

fn kill(reason: KillReason) -> KillPoint {
    KillPoint {
        at_ms: 10,
        reason,
        message: None,
        memory_bytes: None,
        memory_limit_bytes: Some(1000),
    }
}

fn point(memory_bytes: u64) -> SamplePoint {
    SamplePoint {
        at_ms: 5,
        cpu_millicores: Some(100),
        memory_bytes,
        memory_limit_bytes: Some(1000),
    }
}

#[test]
fn a_sigkill_near_the_memory_limit_counts_as_out_of_memory() {
    let near = attribute(kill(KillReason::Killed), Some(&point(950)));
    assert_eq!(near.reason, KillReason::OutOfMemory);
    assert_eq!(near.memory_bytes, Some(950));
    let far = attribute(kill(KillReason::Killed), Some(&point(100)));
    assert_eq!(far.reason, KillReason::Killed);
    let unknown = attribute(kill(KillReason::Killed), None);
    assert_eq!(
        (unknown.reason, unknown.memory_bytes),
        (KillReason::Killed, None)
    );
}

#[tokio::test]
async fn kills_name_the_actor_assigned_to_the_sandbox_and_its_last_sample() -> Result<()> {
    let persistence = Arc::new(MemoryResourcePersistence::default());
    persistence
        .record_assignment(SandboxAssignment {
            actor: actor(),
            sandbox_id: "sandboxes/pod/uid".into(),
            assigned_at_ms: 1,
            memory_limit_bytes: Some(1000),
        })
        .await?;
    persistence
        .record_samples(vec![sample(5, 980), sample(20, 1)])
        .await?;
    for sandbox_id in ["sandboxes/pod/uid", "sandboxes/spare/uid"] {
        persistence
            .record_kill(SandboxKill {
                sandbox_id: sandbox_id.into(),
                killed_at_ms: 10,
                reason: KillReason::Killed,
                message: None,
            })
            .await?;
    }
    let reader = ResourceReader::new(persistence, LifecycleStore::default());
    let resources = reader.actor(&actor(), 0, u64::MAX).await?;
    assert_eq!(resources.samples.len(), 2);
    assert_eq!(resources.kills.len(), 1);
    assert_eq!(resources.kills[0].reason, KillReason::OutOfMemory);
    assert_eq!(resources.kills[0].memory_bytes, Some(980));
    let other = ActorKey {
        actor_id: "two".into(),
        ..actor()
    };
    assert_eq!(
        reader.actor(&other, 0, u64::MAX).await?,
        ActorResources::default()
    );
    Ok(())
}

struct Sampler;

#[async_trait]
impl ResourceSampler for Sampler {
    async fn sample(&self) -> Result<Vec<ResourceSample>> {
        Ok(vec![sample(7, 42)])
    }
}

#[tokio::test]
async fn the_monitor_records_the_samplers_samples() -> Result<()> {
    let persistence = Arc::new(MemoryResourcePersistence::default());
    let stop = CancellationToken::new();
    let monitor = tokio::spawn(
        ResourceMonitor::new(Arc::new(Sampler), persistence.clone()).run(stop.clone()),
    );
    let key = actor();
    while persistence
        .history(&key, 0, u64::MAX)
        .await?
        .samples
        .is_empty()
    {
        tokio::task::yield_now().await;
    }
    stop.cancel();
    monitor.await?;
    let history = persistence.history(&key, 5, 10).await?;
    assert_eq!(history.samples[0].memory_bytes, 42);
    assert!(persistence.history(&key, 8, 10).await?.samples.is_empty());
    Ok(())
}
