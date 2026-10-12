use super::{proto::*, *};

pub(super) fn build(
    config: &SubstrateConfig,
    request: &RuntimeTemplateRequest,
    resources: &ResourceLimits,
) -> Result<ActorTemplate> {
    let identity = serde_json::to_string(&(
        &request.canonical_region,
        &request.image_ref,
        resources,
        &request.jwt_public_keys,
        &request.jwt_issuer,
        &config.worker_labels,
        &config.snapshot_location,
        &config.sandbox_config,
    ))?;
    let mut labels = config.worker_labels.clone();
    labels.insert("terse.ai/region".into(), request.canonical_region.clone());
    Ok(ActorTemplate {
        metadata: Some(ResourceMetadata {
            atespace: config.atespace.clone(),
            name: format!("runtime-{}", digest(&identity)),
            ..Default::default()
        }),
        worker_selector: Some(Selector {
            match_labels: labels.into_iter().collect(),
        }),
        containers: vec![Container {
            name: "runtime".into(),
            image: request.image_ref.clone(),
            command: vec!["/usr/local/bin/durable-actors".into()],
            env: [
                ("DURABLE_ACTORS_PROCESS_ROLE", "warm"),
                ("DURABLE_ACTORS_HOST_BIND", "0.0.0.0:80"),
                (
                    "DURABLE_ACTORS_ASSIGNMENT_PUBLIC_KEYS",
                    &request.jwt_public_keys,
                ),
                ("DURABLE_ACTORS_JWT_ISSUER", &request.jwt_issuer),
                (
                    "DURABLE_ACTORS_SANDBOX_IDENTITY_FILE",
                    "/run/substrate/actor-uid",
                ),
            ]
            .into_iter()
            .map(|(name, value)| EnvVar {
                name: name.into(),
                value: value.into(),
            })
            .collect(),
            wakeup_probe: Some(ContainerWakeupProbe {
                http_get: Some(HttpGetAction {
                    path: "/warmz".into(),
                    port: 80,
                }),
                timeout_seconds: 60,
            }),
            volume_mounts: vec![VolumeMount {
                name: "identity".into(),
                mount_path: "/run/substrate".into(),
            }],
            ..Default::default()
        }],
        volumes: vec![Volume {
            name: "identity".into(),
            system_info: Some(SystemInfoVolumeSource {
                data_sources: vec![SystemInfoDataSource {
                    actor_metadata: Some(ActorMetadataDataSource {
                        items: vec![ActorMetadataItem {
                            field: ActorMetadataField::Uid.into(),
                            path: "actor-uid".into(),
                        }],
                    }),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        }],
        resources: Some(Resources {
            limits: vec![
                Limits {
                    name: "cpu".into(),
                    quantity: format!("{}m", resources.cpu_millis),
                },
                Limits {
                    name: "memory".into(),
                    quantity: format!("{}Mi", resources.memory_mib),
                },
            ],
        }),
        snapshot_config: Some(SnapshotConfig {
            on_pause: SnapshotContentScope::Full.into(),
            on_commit: SnapshotContentScope::Full.into(),
            storage_location: config.snapshot_location.clone(),
            ..Default::default()
        }),
        sandbox_config: Some(SandboxConfig {
            sandbox_class: SandboxClass::Gvisor.into(),
            config_name: config.sandbox_config.clone(),
        }),
        ..Default::default()
    })
}
