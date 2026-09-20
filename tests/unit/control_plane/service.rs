use std::{sync::Mutex, time::Duration};

use super::super::{ActorTokenPurpose, admin::LocalAdminRegistry};
use super::*;
use crate::{actor_state::ActorStorageKey, placement::testing::LocalObjectPlacementStore};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};

struct FakeSocketEventSink {
    delivered: tokio::sync::mpsc::UnboundedSender<serde_json::Value>,
}

#[async_trait]
impl super::super::event_sink::SocketMessageEventSink for FakeSocketEventSink {
    async fn deliver(&self, event: super::super::event_sink::SocketMessageEvent) -> Result<()> {
        self.delivered.send(serde_json::to_value(event)?)?;
        Ok(())
    }
}

fn test_lease(host: &HostId) -> HostLease {
    HostLease {
        id: host.clone(),
        session_id: "00000000-0000-4000-8000-000000000001".into(),
        route: "https://host.example.com".into(),
        expires_at_ms: u64::MAX,
    }
}

struct UnavailableProvisioner;

#[async_trait]
impl HostProvisioner for UnavailableProvisioner {
    async fn ensure_actor_host(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        anyhow::bail!("host creation is outside this test")
    }

    async fn terminate_hosts(
        &self,
        _spec: &HostLaunchSpec,
        _regions: &[String],
    ) -> Result<HostTermination> {
        anyhow::bail!("host termination is outside this test")
    }
}

struct FakeRetiringProvisioner {
    retired: tokio::sync::mpsc::UnboundedSender<(HostLaunchSpec, Vec<String>)>,
    fail: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl HostProvisioner for FakeRetiringProvisioner {
    async fn ensure_actor_host(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        anyhow::bail!("host creation is outside this test")
    }

    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination> {
        self.retired.send((spec.clone(), regions.to_vec()))?;
        ensure!(
            !self.fail.load(std::sync::atomic::Ordering::Relaxed),
            "termination failed"
        );
        Ok(HostTermination {
            provider: "test".into(),
            resource_ids: vec!["sandbox-1".into()],
        })
    }
}

#[tokio::test]
async fn gcs_routes_use_the_hosts_epoch_without_claiming_or_preparing_in_the_control_plane()
-> Result<()> {
    use crate::grpc::proto::{
        self,
        actor_host_service_server::{ActorHostService, ActorHostServiceServer},
    };
    use tokio_stream::wrappers::TcpListenerStream;

    struct Host {
        auth: ActorJwtVerifier,
        peers: Arc<Mutex<std::collections::HashSet<std::net::SocketAddr>>>,
    }
    #[tonic::async_trait]
    impl ActorHostService for Host {
        async fn publish_socket_effects(
            &self,
            _: tonic::Request<crate::grpc::proto::PublishSocketEffectsRequest>,
        ) -> Result<tonic::Response<crate::grpc::proto::Empty>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "fixture does not publish socket effects",
            ))
        }

        async fn activate(
            &self,
            request: Request<proto::ActivateActorRequest>,
        ) -> Result<Response<proto::ActivateActorReply>, Status> {
            self.peers
                .lock()
                .unwrap()
                .insert(request.remote_addr().unwrap());
            let principal = self.auth.authenticate(&request).await?;
            assert!(principal.invocation.is_none());
            assert_eq!(request.get_ref().actor.as_ref().unwrap().actor_id, "one");
            Ok(Response::new(proto::ActivateActorReply { owner_epoch: 42 }))
        }
        async fn invoke(
            &self,
            _: Request<proto::HostInvokeActorRequest>,
        ) -> Result<Response<proto::InvokeActorReply>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn handle_socket(
            &self,
            _: Request<proto::HostSocketEventRequest>,
        ) -> Result<Response<proto::InvokeActorReply>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }
    struct Provisioner(HostLease);
    #[async_trait]
    impl HostProvisioner for Provisioner {
        async fn ensure_actor_host(
            &self,
            _: &HostLaunchSpec,
            _: &str,
            _actor: &ActorKey,
            _new_actor: bool,
        ) -> Result<(HostLease, u64)> {
            Ok((self.0.clone(), 42))
        }
        async fn terminate_hosts(
            &self,
            _: &HostLaunchSpec,
            _: &[String],
        ) -> Result<HostTermination> {
            anyhow::bail!("unused")
        }
    }
    let issuer = test_issuer()?;
    let invocation_auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let route = format!("http://{}", listener.local_addr()?);
    let peers = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let host_peers = peers.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ActorHostServiceServer::new(Host {
                auth: invocation_auth,
                peers: host_peers,
            }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            code_snapshot: None,
            code_revision: "revision".into(),
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let service = ControlPlaneService::new(
        placements.clone(),
        ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?,
        registry,
        issuer.clone(),
        Arc::new(Provisioner(HostLease {
            id: HostId::new("host.v3.revision.host"),
            session_id: uuid::Uuid::new_v4().to_string(),
            route: route.clone(),
            expires_at_ms: u64::MAX,
        })),
    );
    let actor = ActorKey {
        actor_type: "Counter".into(),
        actor_id: "one".into(),
    };
    let target = service.resolve_actor_route(&actor, None, None).await?;
    assert_eq!(target.route, route);
    assert_eq!(target.owner_epoch, 42);
    assert!(placements.get(&actor.storage_key()).await?.is_none());
    service.resolve_actor_route(&actor, None, None).await?;
    assert_eq!(
        peers.lock().unwrap().len(),
        0,
        "readiness must eliminate the extra activation RPC"
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn replacing_a_deployment_terminates_the_previous_revision_hosts() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = super::super::admin::AdminService::new(
        "admin-token".into(),
        registry.clone(),
        issuer.clone(),
    )?;
    let (retired_tx, mut retired_rx) = tokio::sync::mpsc::unbounded_channel();
    let provisioner = Arc::new(FakeRetiringProvisioner {
        retired: retired_tx,
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        provisioner.clone(),
    );
    let first = HostLaunchSpec {
        code_snapshot: None,
        code_revision: "revision-1".into(),
        image_ref: "image-1".into(),
        working_directory: "/workspace".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
    };
    let mut replacement = first.clone();
    replacement.code_revision = "revision-2".into();
    replacement.image_ref = "image-2".into();

    assert!(service.register_deployment(&admin, &first, None).await?);
    assert!(retired_rx.try_recv().is_err());
    assert!(
        service
            .register_deployment(&admin, &replacement, None)
            .await
            .is_err()
    );
    assert_eq!(admin.current_deployment().await?, Some(first.clone()));
    assert_eq!(
        retired_rx.recv().await,
        Some((
            first.clone(),
            super::super::regions::ALL
                .iter()
                .map(|r| (*r).into())
                .collect()
        ))
    );
    provisioner
        .fail
        .store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(
        service
            .register_deployment(&admin, &replacement, None)
            .await?
    );
    assert_eq!(
        retired_rx.recv().await,
        Some((
            first,
            super::super::regions::ALL
                .iter()
                .map(|r| (*r).into())
                .collect()
        ))
    );
    assert_eq!(admin.current_deployment().await?, Some(replacement.clone()));
    assert!(
        !service
            .register_deployment(&admin, &replacement, None)
            .await?
    );
    assert!(retired_rx.try_recv().is_err());
    let mut secret_update = replacement.clone();
    secret_update.secret_refs = vec!["project-secrets-updated".into()];
    assert!(
        service
            .register_deployment(&admin, &secret_update, None)
            .await?
    );
    assert_eq!(
        retired_rx.recv().await,
        Some((
            replacement,
            super::super::regions::ALL
                .iter()
                .map(|r| (*r).into())
                .collect()
        ))
    );
    provisioner
        .fail
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(service.delete_deployment(&admin).await.is_err());
    assert_eq!(admin.current_deployment().await?, Some(secret_update));
    provisioner
        .fail
        .store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(service.delete_deployment(&admin).await?);
    assert_eq!(admin.current_deployment().await?, None);
    assert!(!service.delete_deployment(&admin).await?);
    Ok(())
}

#[tokio::test]
async fn accepted_socket_messages_are_delivered_to_the_configured_event_sink() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let (delivered_tx, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        Arc::new(LocalAdminRegistry::default()),
        issuer,
        Arc::new(UnavailableProvisioner),
    )
    .with_socket_event_sink(Some(Arc::new(FakeSocketEventSink {
        delivered: delivered_tx,
    })));
    let actor = ActorKey {
        actor_type: "ChatRoom".into(),
        actor_id: "room-1".into(),
    };

    service.deliver_socket_message_event(
        &actor,
        Some("trigger-1".into()),
        &ActorSocketEvent::Message {
            connection_id: "socket-1".into(),
            message: crate::actor::ActorSocketMessage::Text {
                data: "hello".into(),
            },
        },
    );

    let delivered = tokio::time::timeout(Duration::from_secs(1), delivered_rx.recv())
        .await?
        .context("socket event delivery task stopped")?;
    assert_eq!(delivered["actorType"], "ChatRoom");
    assert_eq!(delivered["actorId"], "room-1");
    assert_eq!(delivered["triggerId"], "trigger-1");
    assert_eq!(delivered["connectionId"], "socket-1");
    assert_eq!(delivered["message"]["type"], "text");
    assert_eq!(delivered["message"]["data"], "hello");
    Ok(())
}

#[test]
fn existing_actors_stay_pinned_to_the_assigned_region() -> Result<()> {
    let actor = ActorStorageKey::new("object.v1.project.Counter.one");
    let current = ObjectPlacement {
        lease: test_lease(&HostId::new("host.v3.revision.host")),
        object: actor,
        owner: HostId::new("host.v3.revision.host"),
        owner_epoch: 1,
        home_region: "north-america-east".into(),
        state_version: 0,
        state_object: None,
        last_request_id: None,
    };

    assert_eq!(
        select_target_region(None, "north-america-central")?,
        "north-america-central"
    );
    assert_eq!(
        select_target_region(Some(&current), "north-america-west")?,
        "north-america-east"
    );
    for (reported, expected) in [
        ("us-east-1", "north-america-east"),
        ("us-west-2", "north-america-west"),
        ("us-central1", "north-america-central"),
        ("us-central1-a", "north-america-central"),
        ("us-ashburn-1", "north-america-east"),
        ("westus3", "north-america-west"),
    ] {
        assert_eq!(select_target_region(None, reported)?, expected);
        assert_eq!(
            select_target_region(Some(&current), reported)?,
            "north-america-east"
        );
    }
    for unsupported in ["", "unknown", "us-east-999", "us-central1-unknown"] {
        assert_eq!(
            select_target_region(None, unsupported)?,
            "north-america-central",
            "{unsupported}"
        );
        assert_eq!(
            select_target_region(Some(&current), unsupported)?,
            "north-america-east"
        );
    }
    Ok(())
}

#[test]
fn execution_regions_do_not_require_separate_buckets() -> Result<()> {
    assert_eq!(
        select_target_region(None, "southcentralus")?,
        "north-america-south"
    );
    assert_eq!(select_target_region(None, "europe-west")?, "europe-west");
    Ok(())
}

struct LosingActivation {
    placements: Arc<LocalObjectPlacementStore>,
    actor: ActorKey,
    waited: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl HostProvisioner for LosingActivation {
    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        let host = HostId::new(format!("host.v3.{}.winner", spec.host_revision()));
        let mut lease = test_lease(&host);
        lease.route = "https://winner.example.com".into();
        self.placements
            .set_owner(&self.actor.storage_key(), lease, region)?;
        anyhow::bail!("another activation won ownership")
    }
    async fn wait_ready(&self, _: &HostId) -> Result<()> {
        self.waited.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn terminate_hosts(&self, _: &HostLaunchSpec, _: &[String]) -> Result<HostTermination> {
        unreachable!()
    }
}

#[tokio::test]
async fn a_losing_activation_routes_to_the_ready_winner() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            code_revision: "revision".into(),
            image_ref: "im-runtime".into(),
            code_snapshot: Some("im-code".into()),
            working_directory: "/customer".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let actor = ActorKey {
        actor_type: "Counter".into(),
        actor_id: "one".into(),
    };
    let provisioner = Arc::new(LosingActivation {
        placements: placements.clone(),
        actor: actor.clone(),
        waited: false.into(),
    });
    let service = ControlPlaneService::new(placements, auth, registry, issuer, provisioner.clone());
    let target = service
        .route_actor(&actor, "north-america-east", None, None)
        .await?;
    assert_eq!(target.lease.route, "https://winner.example.com");
    assert!(provisioner.waited.load(std::sync::atomic::Ordering::SeqCst));
    Ok(())
}

struct FakeRoutingProvisioner {
    failed_regions: Vec<&'static str>,
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl HostProvisioner for FakeRoutingProvisioner {
    async fn socket_credentials(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        lease: &HostLease,
    ) -> Result<crate::sandbox::SocketCredentials> {
        Ok(crate::sandbox::SocketCredentials {
            url: lease.route.clone(),
            token: String::new(),
        })
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        self.calls.lock().unwrap().push(region.to_owned());
        ensure!(
            !self.failed_regions.contains(&region),
            "host unavailable in {region}"
        );
        Ok((
            HostLease {
                id: HostId::new(format!("host.v3.{}.{region}", spec.host_revision())),
                session_id: uuid::Uuid::new_v4().to_string(),
                route: "https://host.example.com".into(),
                expires_at_ms: u64::MAX,
            },
            1,
        ))
    }

    async fn terminate_hosts(
        &self,
        _spec: &HostLaunchSpec,
        _regions: &[String],
    ) -> Result<HostTermination> {
        unreachable!()
    }
}

#[tokio::test]
async fn provisioning_never_changes_the_assigned_region() -> Result<()> {
    let central = "north-america-central";
    let south = "north-america-south";
    for (failed_regions, existing, reported, expected_region, expected_calls) in [
        (vec![], false, "southcentralus", Some(south), vec![south]),
        (
            vec![],
            false,
            "eu-west-1",
            Some("europe-west"),
            vec!["europe-west"],
        ),
        (
            vec![],
            false,
            "unmapped-region",
            Some(central),
            vec![central],
        ),
        (vec![south], false, "southcentralus", None, vec![south]),
        (vec![south], true, "eastus", None, vec![south]),
        (
            vec![south, central],
            false,
            "southcentralus",
            None,
            vec![south],
        ),
        (vec![central], false, "unmapped-region", None, vec![central]),
    ] {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        registry
            .register_test_deployment(&HostLaunchSpec {
                code_snapshot: None,
                code_revision: "revision".into(),
                image_ref: "image".into(),
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
            })
            .await?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let actor = ActorKey {
            actor_type: "Counter".into(),
            actor_id: "one".into(),
        };
        if existing {
            placements.set_owner(
                &actor.storage_key(),
                test_lease(&HostId::new("old-host")),
                south,
            )?;
        }
        let before = placements.get(&actor.storage_key()).await?;
        let provisioner = Arc::new(FakeRoutingProvisioner {
            failed_regions,
            calls: Mutex::new(vec![]),
        });
        let service = ControlPlaneService::new(
            placements.clone(),
            auth,
            registry.clone(),
            issuer,
            provisioner.clone(),
        );
        let spec = registry.launch_spec().await?.unwrap();
        let result = service
            .ensure_actor_host(
                &ActorKey {
                    actor_type: "Counter".into(),
                    actor_id: "one".into(),
                },
                &spec,
                before.as_ref(),
                &select_target_region(before.as_ref(), reported)?,
            )
            .await;
        assert_eq!(*provisioner.calls.lock().unwrap(), expected_calls);
        if let Some(region) = expected_region {
            let (selected, _, _) = result?;
            assert_eq!(selected, region);
            assert_eq!(placements.get(&actor.storage_key()).await?, before);
        } else {
            assert!(result.is_err());
            if expected_calls.len() == 2 {
                let error = format!("{:#}", result.err().unwrap());
                assert!(error.contains(south) && error.contains(central), "{error}");
            }
            assert_eq!(placements.get(&actor.storage_key()).await?, before);
        }
    }
    Ok(())
}

#[tokio::test]
async fn application_credentials_work_without_postgres() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            code_snapshot: None,
            code_revision: "v1".into(),
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let database = crate::postgres::PostgresDatabase::lazy(
        "postgresql://localhost:1/unavailable?sslmode=disable&connect_timeout=1",
    )?;
    let admin = AdminService::new(
        "api-key".into(),
        Arc::new(super::super::admin::PostgresAdminRegistry::from_database(
            database,
        )),
        issuer.clone(),
    )?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let host_id = HostId::new("host.v3.v1.fixture");
    let actor = ActorKey {
        actor_type: "Room".into(),
        actor_id: "lobby".into(),
    };
    placements.set_owner(
        &actor.storage_key(),
        test_lease(&host_id),
        "north-america-east",
    )?;
    let service = ControlPlaneService::new(
        placements,
        auth,
        registry,
        issuer.clone(),
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let requests = [(
        "actors/Room/lobby/connect",
        serde_json::json!({"transport":"websocket", "metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000}),
    )];
    for (path, body) in requests {
        let response = client
            .post(format!("{origin}/v1/{path}"))
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let issued: serde_json::Value = response.json().await?;
        assert!(status.is_success(), "{path}: {status} {issued}");
        let mut url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
        let key = url
            .query_pairs()
            .find(|(name, _)| name == "key")
            .unwrap()
            .1
            .into_owned();
        url.set_query(None);
        assert_eq!(url.as_str(), "wss://host.example.com/v1/socket");
        let ticket = issuer.verify_socket(&key)?;
        assert_eq!(ticket.actor.actor_id, "lobby");
        assert_eq!(ticket.metadata, body["metadata"]);
        assert_eq!(issued["transport"], "websocket");
        assert_eq!(issued["homeRegion"], "north-america-east");
        assert!(issued.get("key").is_none());
    }
    assert_eq!(
        client
            .get(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn socket_ticket_issuance_requires_api_key_and_cannot_delegate_backend_access() -> Result<()>
{
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let host_id = HostId::new(format!(
        "host.v3.{}.fixture",
        HostLaunchSpec {
            code_revision: "v1".into(),
            image_ref: "im-runtime".into(),
            code_snapshot: Some("im-code".into()),
            working_directory: "/customer".into(),
            actor_entrypoint: None,
            secret_refs: vec![]
        }
        .host_revision()
    ));
    let actor = ActorKey {
        actor_type: "Room".into(),
        actor_id: "lobby".into(),
    };
    placements.set_owner(
        &actor.storage_key(),
        test_lease(&host_id),
        "north-america-east",
    )?;
    let service = ControlPlaneService::new(
        placements,
        auth,
        registry,
        issuer.clone(),
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let url = format!("{origin}/v1/actors/Room/lobby/connect");
    let body = serde_json::json!({"transport":"websocket", "metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000,"homeRegion":"north-america-east"});
    let host_token = issuer
        .issue_host(
            &HostId::new("host.v3.v1.fixture"),
            &uuid::Uuid::new_v4().to_string(),
            "v1",
            "north-america-east",
            &actor,
        )?
        .token;
    for credential in ["", "wrong", &host_token] {
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(credential)
                .json(&body)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
    client.put(format!("{origin}/v1/deployment")).bearer_auth("api-key")
        .json(&serde_json::json!({"codeRevision":"v1","imageRef":"im-runtime","codeSnapshot":"im-code","workingDirectory":"/customer"}))
        .send().await?.error_for_status()?;
    for operation in ["websocket", "grpc"] {
        let response = client
                .post(&url)
                .bearer_auth("api-key")
                .json(&if operation == "websocket" {
                    serde_json::json!({"transport":"websocket", "metadata":{},"homeRegion":"north-america-west"})
                } else {
                    serde_json::json!({"transport":"grpc", "homeRegion":"north-america-west"})
                })
                .send()
                .await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::CONFLICT,
            "{operation}"
        );
    }
    let issued = client
        .post(&url)
        .bearer_auth("api-key")
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(issued.headers().get("cache-control").unwrap(), "no-store");
    let issued: serde_json::Value = issued.json().await?;
    let socket_url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
    let key = socket_url
        .query_pairs()
        .find(|(name, _)| name == "key")
        .unwrap()
        .1
        .into_owned();
    assert_eq!(socket_url.scheme(), "wss");
    assert_eq!(socket_url.host_str(), Some("host.example.com"));
    assert_eq!(socket_url.path(), "/v1/socket");
    assert_eq!(
        socket_url.query_pairs().collect::<Vec<_>>(),
        vec![("key".into(), key.as_str().into())]
    );
    assert_ne!(key, "api-key");
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&key)
            .json(&body)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(format!("{origin}/v1/actors/Room/lobby/connect"))
            .bearer_auth(&key)
            .json(&serde_json::json!({}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn api_key_access_connects_directly_without_an_http_socket_relay() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    {
        let host = HostId::new(format!(
            "host.v3.{}.fixture",
            HostLaunchSpec {
                code_revision: "revision".into(),
                image_ref: "im-runtime".into(),
                code_snapshot: Some("im-code".into()),
                working_directory: "/customer".into(),
                actor_entrypoint: None,
                secret_refs: vec![]
            }
            .host_revision()
        ));
        let actor = ActorKey {
            actor_type: "Counter".into(),
            actor_id: "one".into(),
        };
        placements.set_owner(
            &actor.storage_key(),
            test_lease(&host),
            "north-america-east",
        )?;
    }
    let service = ControlPlaneService::new(
        placements,
        auth,
        registry,
        issuer.clone(),
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let deployment = serde_json::json!({ "codeRevision": "revision", "imageRef": "im-runtime", "codeSnapshot": "im-code", "workingDirectory": "/customer" });
    let registered = client
        .put(format!("{origin}/v1/deployment"))
        .bearer_auth("api-key")
        .json(&deployment)
        .send()
        .await?;
    assert_eq!(registered.status(), reqwest::StatusCode::OK);
    for suffix in ["connect"] {
        let url = format!("{origin}/v1/actors/Counter/one/{suffix}");
        for key in ["", "wrong-key"] {
            assert_eq!(
                client
                    .post(&url)
                    .bearer_auth(key)
                    .json(&serde_json::json!({"effects":[]}))
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }
    }
    let target: serde_json::Value = client
        .post(format!("{origin}/v1/actors/Counter/one/connect"))
        .bearer_auth("api-key")
        .json(&serde_json::json!({"transport":"grpc"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(target["transport"], "grpc");
    assert_eq!(target["homeRegion"], "north-america-east");
    assert_eq!(target["route"], "https://host.example.com");
    for body in [
        serde_json::json!({}),
        serde_json::json!({"transport":"http"}),
        serde_json::json!({"transport":"grpc","metadata":{}}),
        serde_json::json!({"transport":"websocket"}),
        serde_json::json!({"transport":"websocket","metadata":{},"unknown":true}),
    ] {
        let reply = client
            .post(format!("{origin}/v1/actors/Counter/one/connect"))
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?;
        assert_eq!(reply.status(), reqwest::StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            reply.json::<serde_json::Value>().await?["error"]["code"],
            "invalid_request"
        );
    }
    assert_ne!(target["token"], "api-key");
    assert_eq!(
        client
            .post(format!("{origin}/v1/actors/Counter/one/socket-effects"))
            .bearer_auth("api-key")
            .json(&serde_json::json!({"effects":[]}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn deployment_reads_and_deletion_require_the_api_key() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
    admin
        .register_test_deployment(&HostLaunchSpec {
            code_snapshot: None,
            code_revision: "revision-1".into(),
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let (retired, _retired_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        Arc::new(FakeRetiringProvisioner {
            retired,
            fail: std::sync::atomic::AtomicBool::new(false),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let deployment_url = format!("{origin}/v1/deployment");
    for method in [reqwest::Method::GET, reqwest::Method::DELETE] {
        assert_eq!(
            client
                .request(method, &deployment_url)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
    let deployment: serde_json::Value = client
        .get(&deployment_url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(deployment["codeRevision"], "revision-1");
    assert_eq!(deployment["secretRefs"], serde_json::json!([]));
    for changed in [true, false] {
        let reply: serde_json::Value = client
            .delete(&deployment_url)
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply["changed"], changed);
    }
    assert_eq!(
        client
            .get(&deployment_url)
            .bearer_auth("api-key")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn contract_api_publishes_with_deployments_and_reads_only_the_active_revision() -> Result<()>
{
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
    let host_token = issuer
        .issue_host(
            &HostId::new("host.v3.r1.one"),
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "us-east",
            &ActorKey {
                actor_type: "Counter".into(),
                actor_id: "one".into(),
            },
        )?
        .token;
    let (retired, _retired_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        Arc::new(FakeRetiringProvisioner {
            retired,
            fail: std::sync::atomic::AtomicBool::new(false),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    for path in ["/v1/deployment/contract"] {
        for credential in ["", "wrong", &host_token] {
            assert_eq!(
                client
                    .get(format!("{origin}{path}"))
                    .bearer_auth(credential)
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }
        let response = client
            .get(format!("{origin}{path}"))
            .bearer_auth("api-key")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(
            response.json::<serde_json::Value>().await?["error"]["code"],
            "not_found"
        );
    }
    let document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    let mut deployment = serde_json::json!({"codeRevision":"r1", "imageRef":"im-runtime", "codeSnapshot":"im-code", "workingDirectory":"/customer", "contract":document});
    for scope in ["/v1"] {
        for changed in [true, false] {
            let reply: serde_json::Value = client
                .put(format!("{origin}{scope}/deployment"))
                .bearer_auth("api-key")
                .json(&deployment)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(reply["changed"], changed);
        }
        let response = client
            .get(format!("{origin}{scope}/deployment/contract"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(response.headers()["cache-control"], "no-store");
        let reply: serde_json::Value = response.json().await?;
        assert_eq!(reply["contract"], document);
        assert_eq!(reply["codeRevision"], "r1");
        assert!(
            reply["contractHash"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
    }
    deployment["contract"] = serde_json::json!({"version":1,"actors":[]});
    let response = client
        .put(format!("{origin}/v1/deployment"))
        .bearer_auth("api-key")
        .json(&deployment)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    deployment["codeRevision"] = "r2".into();
    client
        .put(format!("{origin}/v1/deployment"))
        .bearer_auth("api-key")
        .json(&deployment)
        .send()
        .await?
        .error_for_status()?;
    let active: serde_json::Value = client
        .get(format!("{origin}/v1/deployment/contract"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(active["codeRevision"], "r2");
    assert_eq!(active["contract"]["actors"], serde_json::json!([]));
    assert_eq!(
        client
            .get(format!("{origin}/v1/deployment/contract?revision=r1"))
            .bearer_auth("api-key")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let pinned: serde_json::Value = client
        .get(format!("{origin}/v1/deployment/contract?revision=r2"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(pinned, active);
    for suffix in ["?revision=bad%2Frevision", "?unknown=1"] {
        assert_eq!(
            client
                .get(format!("{origin}/v1/deployment/contract{suffix}"))
                .bearer_auth("api-key")
                .send()
                .await?
                .status(),
            reqwest::StatusCode::BAD_REQUEST
        );
    }
    deployment["codeRevision"] = "r3".into();
    deployment["contract"] = serde_json::json!({"version":2,"actors":[]});
    assert_eq!(
        client
            .put(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let unchanged: serde_json::Value = client
        .get(format!("{origin}/v1/deployment"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(unchanged["codeRevision"], "r2");
    server.abort();
    Ok(())
}

pub(super) fn test_issuer() -> Result<ActorJwtIssuer> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(pkcs8.as_ref()),
        "test-key",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(60),
    )
}
