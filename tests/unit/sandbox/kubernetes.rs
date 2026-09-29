use super::*;

#[test]
fn customer_pod_uses_managed_gvisor_without_ambient_credentials() -> Result<()> {
    let request = CreateSpareRequest {
        control_plane_url: Some("http://control:7100".into()),
        kind: SpareKind::Actor,
        name: "warm-one".into(),
        image_ref: "runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits {
            cpu_millis: 2000,
            memory_mib: 4096,
        },
    };
    let pod = spare_pod(&request, "us-west4-a", "test-token")?;
    let spec = pod.spec.unwrap();
    assert_eq!(spec.runtime_class_name.as_deref(), Some("gvisor"));
    assert_eq!(spec.active_deadline_seconds, None);
    assert_eq!(spec.automount_service_account_token, Some(false));
    assert_eq!(
        spec.node_selector.unwrap()["topology.kubernetes.io/zone"],
        "us-west4-a"
    );
    assert_eq!(
        spec.containers[0]
            .resources
            .as_ref()
            .unwrap()
            .limits
            .as_ref()
            .unwrap()["memory"]
            .0,
        "4096Mi"
    );
    assert_eq!(
        spec.containers[0]
            .security_context
            .as_ref()
            .unwrap()
            .allow_privilege_escalation,
        Some(false)
    );
    Ok(())
}

#[test]
fn resource_identity_requires_namespace_name_and_uid() {
    assert_eq!(
        resource_identity("sandboxes/warm/uid").unwrap(),
        ("sandboxes", "warm", "uid")
    );
    for bad in [
        "",
        "warm",
        "sandboxes/warm",
        "sandboxes/warm/",
        "sandboxes/warm/uid/extra",
    ] {
        assert!(resource_identity(bad).is_err());
    }
}

#[tokio::test]
async fn retirement_recovers_a_lost_create_reply_and_uses_a_uid_precondition() -> Result<()> {
    use axum::{Json, Router, routing::get};
    let deleted = Arc::new(std::sync::Mutex::new(None));
    let recorded = deleted.clone();
    let pod = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"reserved-pod","namespace":"sandboxes","uid":"created-uid","labels":{"app.kubernetes.io/managed-by":"terse"}}});
    let routes = Router::new().route(
        "/api/v1/namespaces/sandboxes/pods/reserved-pod",
        get(move || async move { Json(pod) }).delete(
            move |Json(body): Json<serde_json::Value>| async move {
                *recorded.lock().unwrap() = Some(body);
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let cluster = Kubernetes::new(
        Client::try_from(config)?,
        GkeConfig {
            namespace: "sandboxes".into(),
            zones: BTreeMap::new(),
            public_origin: "https://actors.example.com".into(),
            artifact_bucket: "code".into(),
        },
    );
    let result = cluster
        .retire_spare(&SpareHandle {
            name: "reserved-pod".into(),
            resource_id: String::new(),
            route: String::new(),
            canonical_region: String::new(),
            control_route: String::new(),
            control_token: String::new(),
        })
        .await;
    server.abort();
    result?;
    assert_eq!(
        deleted.lock().unwrap().as_ref().unwrap()["preconditions"]["uid"],
        "created-uid"
    );
    Ok(())
}
