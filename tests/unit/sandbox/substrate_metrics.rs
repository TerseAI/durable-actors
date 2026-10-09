use super::*;

#[test]
fn reserved_demand_uses_the_largest_resource_fraction_per_worker() -> Result<()> {
    for (cpu, memory, actors, expected) in [
        ("2000m", "1Gi", 1, 0.5),
        ("1", "6Gi", 1, 0.75),
        ("1", "1Gi", 90, 0.9),
    ] {
        assert_eq!(worker_demand(&worker(cpu, memory, actors))?, expected);
    }
    Ok(())
}

#[test]
fn capacity_metric_sums_worker_allocations_by_pool() -> Result<()> {
    let body = encode_capacity(vec![worker("2", "1Gi", 1), worker("1", "6Gi", 1)].into_iter())?;
    assert!(
        body.contains(
            "terse_substrate_worker_demand{pool_namespace=\"workers\",pool=\"shared\"} 1.25"
        ),
        "{body}"
    );
    Ok(())
}

#[test]
fn missing_capacity_is_an_error_instead_of_a_scale_down_signal() {
    assert!(encode_capacity(std::iter::empty()).is_err());
    let mut invalid = worker("1", "1Gi", 1);
    invalid.status.as_mut().unwrap().capacity = None;
    assert!(worker_demand(&invalid).is_err());
}

fn worker(cpu: &str, memory: &str, actors: i32) -> proto::Worker {
    let resources = |cpu: &str, memory: &str, actors| proto::WorkerResources {
        actors,
        resources: Some(proto::Resources {
            limits: vec![
                proto::Limits {
                    name: "cpu".into(),
                    quantity: cpu.into(),
                },
                proto::Limits {
                    name: "memory".into(),
                    quantity: memory.into(),
                },
            ],
        }),
    };
    proto::Worker {
        worker_namespace: "workers".into(),
        worker_pool: "shared".into(),
        status: Some(proto::WorkerStatus {
            state: proto::WorkerState::Active.into(),
            capacity: Some(resources("4", "8Gi", 100)),
            allocated: Some(resources(cpu, memory, actors)),
        }),
        ..Default::default()
    }
}
